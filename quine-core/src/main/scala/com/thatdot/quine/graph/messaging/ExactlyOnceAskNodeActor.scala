package com.thatdot.quine.graph.messaging

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Promise}
import scala.util.Random

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Timers}

import com.thatdot.quine.graph.Expires
import com.thatdot.quine.model.QuineIdProvider

/** Temporary actor facilitating asks to nodes with exactly-once delivery across the Quine graph
  *
  * @note when the destination actor is local, we do the sending of the message directly in
  * `relayAsk`. This is important because it enforces the message-ordering. For remote nodes, we
  * do the retrying here.
  *
  * TODO: consider making these actors children of the shards
  *
  * TODO: consider making this actor stay alive for a short while after receiving a response back
  *       (so that it can continue sending `Ack`'s in case the first one was dropped)
  *
  * TODO: add a mechanism to handle sending a `Source` or `Future` (symmetric to receiving them)
  *
  * TODO: Reconsider the mechanism for sending a `Source` - it does not account for messages dropped
  *       by the network
  *
  * @param unattributedMessage message to send
  * @param recipient node receiving the message
  * @param remoteShardTarget if in a different JVM, shard actor responsible for the node
  * @param idProvider for debuggind purposes - used to pretty-print the node ID
  * @param originalSender for debuggging purposes - what Akka's `sender()` will report
  * @param promisedResult promise that is fulfilled with the reponse
  * @param timeout time to wait until the promise fails with a timeout
  */
final private[quine] class ExactlyOnceAskNodeActor[Resp](
  unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[Resp],
  recipient: QuineIdAtTime,
  remoteShardTarget: Option[ActorRef],
  idProvider: QuineIdProvider,
  originalSender: ActorRef,
  promisedResult: Promise[Resp],
  timeout: FiniteDuration,
  resultHandler: ResultHandler[Resp]
) extends Actor
    with ActorLogging
    with Timers {

  implicit private val ec: ExecutionContextExecutor = context.dispatcher
  private lazy val msg = unattributedMessage(WrappedActorRef(self))

  private val retryTimeout: Cancellable = remoteShardTarget match {
    case None => Cancellable.alreadyCancelled // node is local, message already sent
    case Some(shardTarget) =>
      // The node is remote, so send its shard the wrapped message until it acks
      val dedupId = Random.nextLong()

      def updateExpiry: QuineMessage => QuineMessage = {
        case expires: Expires => expires.preparedForRemoteTell()
        case other => other
      }

      def toSendFunc() = BaseMessage.DeliveryRelay(
        BaseMessage.LocalMessageDelivery(updateExpiry(msg), recipient, self),
        dedupId,
        needsAck = true
      )

      val retryInterval: FiniteDuration = 2.seconds // TODO: exponential backoff?
      context.system.scheduler.scheduleAtFixedRate(
        initialDelay = Duration.Zero,
        interval = retryInterval
      )(() => shardTarget.!(toSendFunc())(self))
  }

  // Schedule a timeout
  timers.startSingleTimer(
    key = GiveUpWaiting,
    msg = GiveUpWaiting,
    timeout
  )

  private def receiveResponse(response: QuineResponse): Unit = {
    resultHandler.receiveResponse(response, promisedResult)(context.system)
    retryTimeout.cancel() // It is possible to get a reply back before the Ack
    context.system.stop(self)
  }

  def receive: Receive = {
    case BaseMessage.Ack =>
      retryTimeout.cancel()
      ()

    case BaseMessage.DeliveryRelay(BaseMessage.Response(r), _, needsAck) =>
      if (needsAck) sender() ! BaseMessage.Ack
      // deliberately ignore deduplication step.
      receiveResponse(r)

    case BaseMessage.Response(r) =>
      // Should only need to Ack if it was relayed with DeliveryRelay
      receiveResponse(r)

    case GiveUpWaiting =>
      val name = recipient.toInternalString
      val typedName = recipient.debug(idProvider)
      val neverGotAcked = retryTimeout.cancel()
      val throughShard = remoteShardTarget.fold("")(" through remote shard " + _)

      val waitingFor = if (neverGotAcked && remoteShardTarget.nonEmpty) "`Ack`/reply" else "reply"
      val wasSentBy =
        if (originalSender == ActorRef.noSender) "" else s" sent by [$originalSender]"

      val timeoutException = new ExactlyOnceTimeoutException(
        s"Ask relayed by Graph timed out on [$typedName] (at $name) after " +
        s"[${timeout.toMillis} ms] waiting for $waitingFor to `$msg`$wasSentBy$throughShard"
      )
      promisedResult.tryFailure(timeoutException)
      context.system.stop(self)

    case x =>
      val name = recipient.toInternalString
      log.error(s"ExactlyOnceNodeAskActor asking: $name received unknown message: $x")
  }
}

package com.thatdot.quine.graph.messaging

import scala.concurrent.Promise
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.Random

import org.apache.pekko.actor.{Actor, ActorRef, Cancellable, Timers}

import com.codahale.metrics.Timer

import com.thatdot.common.logging.Log.{ActorSafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.Expires
import com.thatdot.quine.graph.metrics.HostQuineMetrics.RelayAskMetric
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log.implicits.LogActorRef

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
  * @param originalSender for debuggging purposes - what Pekko's `sender()` will report
  * @param promisedResult promise that is fulfilled with the response
  * @param timeout time to wait until the promise fails with a timeout
  */
final private[quine] class ExactlyOnceAskNodeActor[Resp](
  unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[Resp],
  recipient: SpaceTimeQuineId,
  remoteShardTarget: Option[ActorRef],
  idProvider: QuineIdProvider,
  originalSender: ActorRef,
  promisedResult: Promise[Resp],
  timeout: FiniteDuration,
  resultHandler: ResultHandler[Resp],
  metrics: RelayAskMetric,
)(implicit logConf: LogConfig)
    extends Actor
    with ActorSafeLogging
    with Timers {
  private lazy val msg = unattributedMessage(WrappedActorRef(self))

  private val timerContext: Timer.Context = metrics.timeMessageSend()

  private val retryTimeout: Cancellable = remoteShardTarget match {
    case None =>
      timerContext.stop()
      Cancellable.alreadyCancelled // node is local, message already sent
    case Some(shardTarget) =>
      // The node is remote, so send its shard the wrapped message until it acks
      val dedupId = Random.nextLong()

      def updateExpiry: QuineMessage => QuineMessage = {
        case expires: Expires => expires.preparedForRemoteTell()
        case other => other
      }

      // This is a function instead of `val` so that `updateExpiry` is regenerated each time message sending is retried
      def toSendFunc() =
        BaseMessage.DeliveryRelay(
          BaseMessage.LocalMessageDelivery(updateExpiry(msg), recipient, self),
          dedupId,
          needsAck = true,
        )

      val retryInterval: FiniteDuration = 2.seconds // TODO: exponential backoff?
      context.system.scheduler.scheduleAtFixedRate(
        initialDelay = Duration.Zero,
        interval = retryInterval,
      )(() => shardTarget.!(toSendFunc())(self))(context.dispatcher)
  }

  // Schedule a timeout
  timers.startSingleTimer(
    key = GiveUpWaiting,
    msg = GiveUpWaiting,
    timeout,
  )

  private def receiveResponse(response: QuineResponse): Unit = {
    resultHandler.receiveResponse(response, promisedResult)(context.system)
    if (!retryTimeout.isCancelled) { // It is possible to get a reply back before the Ack
      timerContext.stop()
      retryTimeout.cancel()
    }
    context.stop(self)
  }

  def receive: Receive = {
    case BaseMessage.Ack =>
      timerContext.stop()
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
      timerContext.stop()
      val neverGotAcked = retryTimeout.cancel()

      val recipientStr =
        recipient.pretty(idProvider) + remoteShardTarget.fold("")(shard => s" (through remote shard $shard)")

      val waitingForStr = if (neverGotAcked && remoteShardTarget.nonEmpty) "`Ack`/reply" else "reply"

      val timeoutException = new ExactlyOnceTimeoutException(
        s"""Ask relayed by graph timed out after $timeout waiting for $waitingForStr to message of type:
           |${msg.getClass.getSimpleName} from originalSender: "$originalSender"
           |to: $recipientStr. Message: $msg""".stripMargin.replace('\n', ' ').trim,
      )
      log.warn(
        log"""Ask relayed by graph timed out after ${Safe(timeout.toString)} waiting for ${Safe(waitingForStr)} to
             |message of type: ${Safe(msg.getClass.getSimpleName)} from originalSender: "${Safe(originalSender)}" to:
             |${Safe(recipientStr)}. If this occurred as part of a Cypher query, the query will be retried by default.
             |Message: ${msg.toString}""".cleanLines,
      )
      promisedResult.tryFailure(timeoutException)
      context.stop(self)

    case x =>
      val name = recipient.toInternalString
      log.error(log"ExactlyOnceNodeAskActor asking: ${Safe(name)} received unknown message: ${x.toString}")
  }
}

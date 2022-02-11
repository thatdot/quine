package com.thatdot.quine.graph.messaging

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Promise}
import scala.util.Random

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Timers}

/** Temporary actor facilitating asks with exactly-once delivery across the Quine graph
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
  * @param actorRef address of the destination actor
  * @param refIsRemote is the destination actor in a different JVM?
  * @param originalSender for debuggging purposes - what Akka's `sender()` will report
  * @param promisedResult promise that is fulfilled with the reponse
  * @param timeout time to wait until the promise fails with a timeout
  */
final private[quine] class ExactlyOnceAskActor[Resp](
  unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[Resp],
  actorRef: ActorRef,
  refIsRemote: Boolean,
  originalSender: ActorRef,
  promisedResult: Promise[Resp],
  timeout: FiniteDuration,
  resultHandler: ResultHandler[Resp]
) extends Actor
    with ActorLogging
    with Timers {
  implicit private val ec: ExecutionContextExecutor = context.dispatcher
  private lazy val msg = unattributedMessage(WrappedActorRef(self))

  // Remote messages get retried
  private val retryTimeout: Cancellable = if (refIsRemote) {
    val dedupId = Random.nextLong()

    val toSend = BaseMessage.DeliveryRelay(msg, dedupId, needsAck = true)

    val retryInterval: FiniteDuration = 2.seconds // TODO: exponential backoff?
    context.system.scheduler.scheduleAtFixedRate(
      initialDelay = Duration.Zero,
      interval = retryInterval,
      receiver = actorRef,
      message = toSend
    )
  } else {
    Cancellable.alreadyCancelled
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

  /* Because the local relaying of an ask message might result in a later node telling a remote
   * node with instructions to reply here, we need the temporary actor used in the ask pattern to
   * have capabilities for dealing with all the remote message send mechanisms we use (e.g.
   * unwrapping [[FutureResult]]) even if the message it relays out is local
   */
  def receive: Receive = {
    case BaseMessage.Ack =>
      retryTimeout.cancel()
      ()

    case BaseMessage.Response(r) => receiveResponse(r)

    case BaseMessage.DeliveryRelay(
          BaseMessage.Response(r),
          _,
          needsAck
        ) => // Message is not a `T` if `FutureResult` is used.
      if (needsAck) sender() ! BaseMessage.Ack
      // deliberately ignore deduplication step - this actor is only ever waiting for one message.
      receiveResponse(r)

    case GiveUpWaiting =>
      val neverGotAcked = retryTimeout.cancel()
      val waitingFor = if (neverGotAcked && refIsRemote) "`Ack`/reply" else "reply"
      val timeoutException = new ExactlyOnceTimeoutException(
        s"$self timed out after $timeout waiting for $waitingFor to `$msg` from originalSender: $originalSender to: $actorRef"
      )
      promisedResult.tryFailure(timeoutException)
      context.system.stop(self)

    case x =>
      log.error(s"ExactlyOnceAskActor asking: $actorRef received unknown message: $x")
  }
}

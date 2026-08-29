package com.thatdot.quine.graph.behavior

import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

import org.apache.pekko.actor.Actor
import org.apache.pekko.dispatch.Envelope

import com.thatdot.common.logging.Log.{ActorSafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.QuineError

/** Functionality for pausing the processing of messages while a future completes.
  *
  * Use this by calling [[pauseMessageProcessingUntil]] with the future. New messages will be stashed until the actor
  * is ready to handle them again, at which point they will be re-enqueued.
  *
  * Callbacks are accumulated in order and their effects will be applied sequentially only once there is no pending
  * callback ordered prior to it. If an earlier callback fails, later callbacks that have succeeded _will_ be executed.
  * So effects are ordered, but they are not dependent.
  *
  * @note actors extending this trait should have a priority mailbox with the priority function wrapped in
  *       [[StashedMessage.priority]] - that way, the order of messages that get unstashed is correct.
  */
trait PriorityStashingBehavior extends Actor with ActorSafeLogging {

  def qid: QuineId
  implicit def idProvider: QuineIdProvider
  implicit protected def logConfig: LogConfig

  sealed trait PausedMessageCallback[A] {
    def id: Int
    def ready(result: Try[A]): Ready[A]
  }
  case class Pending[A](id: Int, callback: (Try[A] => Unit)) extends PausedMessageCallback[A] {
    val promise: Promise[Unit] = Promise()
    def ready(result: Try[A]): Ready[A] =
      Ready(id, promise, result, callback) // Note: Not completing the promise here. Wait until the effects are applied
  }
  case class Ready[A](id: Int, promise: Promise[Unit], result: Try[A], callback: (Try[A] => Unit))
      extends PausedMessageCallback[A] {
    def ready(result: Try[A]): Ready[A] = Ready(id, promise, result, callback)
    def runCallback(): Unit = {
      callback(result)
      val _ = promise.tryComplete(result.map(_ => ()))
    }
  }

  val pendingCallbacks: mutable.ArrayBuffer[PausedMessageCallback[_]] = mutable.ArrayBuffer.empty
  private var idCounter = 0 // Used only to uniquely identify futures in progress. OK if it rolls over.

  def enqueueCallback(callback: Pending[_]): Unit =
    pendingCallbacks.append(callback)

  /** Anything still paused when this actor stops will otherwise never be answered.
    *
    * [[pauseMessageProcessingUntil]] hands its caller a future that completes only when the
    * self-sent [[StashedResultDelivery]] drains the queue. Stopping the actor first (a purge, or a
    * sleep decided while a write is still in flight) sends that message to dead letters, and
    * the promise is then neither completed nor failed. The caller waits forever, and nothing
    * notices: there is no ask in front of it to time out, and retry machinery only fires on
    * failure, so silence is invisible to both.
    *
    * Failing them is what makes the loss observable. The callers that matter run under
    * at-least-once retry, which re-runs the work against whatever node replaces this one.
    */
  override def postStop(): Unit = {
    if (pendingCallbacks.nonEmpty || messageBuffer.nonEmpty) {
      val stopped = NodeStoppedWhilePausedException(qid.toString, pendingCallbacks.size, messageBuffer.size)
      log.info(
        safe"Node stopped with paused work: failing ${Safe(pendingCallbacks.size.toString)} callback(s) and " +
        safe"dropping ${Safe(messageBuffer.size.toString)} stashed message(s); callers retry against its replacement",
      )
      pendingCallbacks.foreach {
        case p: Pending[_] => p.promise.tryFailure(stopped): Unit
        case r: Ready[_] => r.promise.tryFailure(stopped): Unit
      }
      pendingCallbacks.clear()
      // The stashed envelopes die with the actor too. Dead-lettering them at least puts the loss
      // somewhere a reader can find it, rather than dropping them silently.
      messageBuffer.foreach(e => context.system.deadLetters.tell(e.message, e.sender))
      messageBuffer.clear()
    }
    super.postStop()
  }

  def addResultToCallback[A](findId: Int, result: Try[A], isResultLogSafe: Boolean): Unit =
    pendingCallbacks.indexWhere(_.id == findId) match {
      case -1 =>
        log.warn(
          log"Received a result on node: ${Safe(qid.pretty)} for unknown callback ID: ${Safe(findId)}. Result was ${if (isResultLogSafe) Safe(result.toString)
          else result.toString}",
        )
      case i =>
        pendingCallbacks(i) match {
          case cb: PausedMessageCallback[A @unchecked] =>
            pendingCallbacks(i) = cb.ready(result)
        }
    }

  @tailrec
  private def processReadyCallbacks()(implicit logConfig: LogConfig): Unit = {
    log.trace(
      log"""pendingCallbacks on node: $qid size: ${Safe(pendingCallbacks.size)} first is:
           |${pendingCallbacks.headOption.toString} Stashed size: ${Safe(messageBuffer.size)}""".cleanLines,
    )
    pendingCallbacks.headOption match {
      case Some(_: Pending[_]) =>
        () // wait for this result to complete before more processing to maintain effect order
        log.trace(safe"Pending item is next on node $qid. Size is: ${Safe(pendingCallbacks.size)}")
      case Some(r: Ready[_]) =>
        log.trace(
          safe"Ready item: ${Safe(r.id)} is next on node: $qid. Remaining after removal: ${Safe(pendingCallbacks.size - 1)}",
        )
        val _ = pendingCallbacks.remove(0)
        r.runCallback()
        processReadyCallbacks()
      case None =>
        /* Go back to the regular behaviour and enqueue stashed messages back into the actor mailbox. The
         * `StashedMessage` wrapper ensures that re-enqueued messages get processed as if they had arrived first. */
        log.trace(
          safe"Unbecoming on: $qid Remaining size: ${Safe(pendingCallbacks.size)} stashed size: ${Safe(messageBuffer.size)}",
        )
        if (isStashing) {
          isStashing = false
          context.unbecome()
        }
        messageBuffer.foreach { e =>
          log.trace(
            log"Unstashing message: ${e.message.toString} on node: $qid stashed size: ${Safe(messageBuffer.size)}",
          )
          self.tell(StashedMessage(e.message), e.sender)
        }
        messageBuffer.clear()
    }
  }

  val messageBuffer: mutable.ArrayBuffer[Envelope] = mutable.ArrayBuffer.empty

  /** Whether the stashing behaviour is currently pushed onto the actor.
    *
    * Not the same question as whether any callback is outstanding, which is what it might look like. A callback is
    * removed from [[pendingCallbacks]] before it is run, so a callback that pauses again (which is ordinary, since
    * applying one result is exactly when a node discovers the next thing it must wait for) would see an empty queue
    * and push the behaviour a second time, against the one `unbecome` that ends the episode. The node would then stash
    * every message it was ever sent again. So what is tracked is the push itself.
    */
  private var isStashing: Boolean = false

  private val isCalled = new AtomicBoolean()

  /** Pause message processing until a future is completed
    *
    * This method is not thread safe. Only call it sequentially; never in a Future. It can be called multiple times, and
    * the effects will be applied in order. If the pending Futures complete out of order, application of effects will
    * be deferred until the earlier queued futures complete and have their effects applied first.
    *
    * @param until computation which must finish before the actor resumes processing messages
    * @param onComplete action to run on the actor thread right after the computation finishes
    *
    * @return The `Future` returned from this function will be completed after the effects in `onComplete` have been
    *         applied. The Success/Failure of the returned future will correspond to that of the `until` Future. Note
    *         that this means that if the provided `onComplete` callback successfully applies effects when the `until`
    *         Future fails, then the returned Future will also have a `Failure` status after successfully applying the
    *         `onComplete` callback.
    */
  final protected def pauseMessageProcessingUntil[A](
    until: Future[A],
    onComplete: Try[A] => Unit,
    isResultLogSafe: Boolean,
  ): Future[Unit] = if (until.isCompleted && pendingCallbacks.isEmpty) {
    // If the future is already completed and no other callbacks are enqueued ahead of it, apply effects immediately
    Future.successful(onComplete(until.value.get))
  } else {
    log.whenDebugEnabled {
      if (!isCalled.compareAndSet(false, true))
        throw new Exception(s"pauseMessageProcessingUntil was called concurrently on node ${qid.pretty}!")
    }

    val thisFutureId = idCounter
    idCounter += 1
    val pending = Pending(thisFutureId, onComplete)
    enqueueCallback(pending)

    // Temporarily change the actor behavior to only buffer messages, once per episode of stashing
    if (!isStashing) {
      isStashing = true
      log.trace(
        log"Becoming PriorityStashingBehavior on: $qid stashed size: ${Safe(messageBuffer.size)}",
      )
      context.become(
        {
          case StashedResultDelivery(id, result) =>
            log.trace(
              log"Result delivery for: ${Safe(id)} with payload: ${result.toString} on node: $qid",
            )
            addResultToCallback(id, result, isResultLogSafe)
            // Every time a result is delivered, iterate through zero or more results to apply callback effects.
            processReadyCallbacks()

          /* We are are receiving a message that was un-stashed before. Re-stash it. */
          case StashedMessage(msg) =>
            messageBuffer += Envelope(msg, sender())
            log.trace(
              log"Restashed message: ${msg.toString} on node: $qid size: ${Safe(messageBuffer.size)}",
            )

          case msg =>
            messageBuffer += Envelope(msg, sender())
            log.trace(
              log"Stashed message: ${msg.toString} on node: $qid size: ${Safe(messageBuffer.size)}",
            )
        },
        discardOld = false,
      )
    }

    // Schedule the message which will restore the previous actor behavior after the future completes.
    until.onComplete { (done: Try[_]) =>
      done.toEither.left.foreach(err =>
        log.debug(
          safe"pauseMessageProcessingUntil: future for: ${Safe(thisFutureId)} failed on node $qid",
        ),
      )
      self ! StashedResultDelivery(thisFutureId, done)
    }(context.dispatcher)

    log.whenDebugEnabled {
      if (!isCalled.compareAndSet(true, false))
        throw new Exception(s"pauseMessageProcessingUntil was called concurrently on node ${qid.pretty}!")
    }
    pending.promise.future
  }
}

/** Wrapper to represent a message that was re-enqued from a stash and consequently should be prioritized over other
  * messages of otherwise equal priority that are already in the mailbox.
  */
final case class StashedMessage(msg: Any)

/** This message is sent from an actor to itself to conclude (or decrement) the `pauseMessageProcessingUntil`
  * functionality. It will only be sent among the same JVM (from a node to itself), so it is easy to pass through a
  * callback function.
  *
  * @param id an arbitrary identifier for the original call to `pauseMessageProcessingUntil`
  * @param result The value returned from the completed future.
  */
final case class StashedResultDelivery[A](id: Int, result: Try[A])

/** A node actor stopped while it still had paused work.
  *
  * Retriable, but at-least-once rather than never-attempted. The paused future is already running
  * when it reaches `pauseMessageProcessingUntil`, and a `Ready` callback is one whose result has
  * already arrived, so the work may well have finished. What did not run is the callback that
  * applies its effects. A retry against the replacement node can therefore repeat a durable write.
  *
  * A [[QuineError]] (and registered in the message pickler beside the others), so that a node on
  * ANOTHER member says the same thing. `AnyError.fromThrowable` renders anything
  * `QuineError.fromThrowable` does not name as `AnyError.GenericError`, whose type nothing can
  * match on, and a caller that cannot recognize this failure treats it as terminal -- which would
  * make the retry above a property of where the node happened to live.
  *
  * `node` is the rendered id rather than a `QuineId` so the wire form needs only primitive
  * picklers; nothing reads these fields except the message.
  */
final case class NodeStoppedWhilePausedException(node: String, pending: Int, stashed: Int)
    extends Exception(
      s"Node $node stopped while message processing was paused: " +
      s"$pending pending callback(s) and $stashed stashed message(s) abandoned",
    )
    with QuineError

object StashedMessage {

  /** Combinator to produce a new priority function where a [[StashedMessage]] has slightly higher priority than the
    * underlying message it wraps, but otherwise the priorities of the underlying messages take precedence.
    */
  def priority(priorityFunction: Any => Int): Any => Int = {
    case StashedMessage(msg) => priorityFunction(msg) * 2
    case msg => priorityFunction(msg) * 2 + 1
  }
}

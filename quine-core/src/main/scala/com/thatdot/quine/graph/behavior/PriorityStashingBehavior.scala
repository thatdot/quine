package com.thatdot.quine.graph.behavior

import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

import org.apache.pekko.actor.Actor
import org.apache.pekko.dispatch.Envelope

import com.thatdot.quine.model.{QuineId, QuineIdProvider}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

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
  *       [[StashedMessage.priority]] — that way, the order of messages that get unstashed is correct.
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

  def addResultToCallback[A](findId: Int, result: Try[A], isResultSafe: Boolean): Unit =
    pendingCallbacks.indexWhere(_.id == findId) match {
      case -1 =>
        log.warn(
          log"Received a result on node: ${Safe(qid.debug)} for unknown callback ID: ${Safe(findId)}. Result was ${if (isResultSafe) Safe(result.toString)
          else result.toString}"
        )
      case i =>
        pendingCallbacks(i) match {
          case cb: PausedMessageCallback[A @unchecked] =>
            pendingCallbacks(i) = cb.ready(result)
        }
    }

  @tailrec
  private def processReadyCallbacks()(implicit logConfig: LogConfig): Unit = {
    log.debug(
      log"""pendingCallbacks on node: ${Safe(qid.pretty)} size: ${Safe(pendingCallbacks.size)} first is:
           |${pendingCallbacks.headOption.toString} Stashed size: ${Safe(messageBuffer.size)}""".cleanLines
    )
    pendingCallbacks.headOption match {
      case Some(_: Pending[_]) =>
        () // wait for this result to complete before more processing to maintain effect order
        log.debug(safe"Pending item is next on node ${Safe(qid.pretty)}. Size is: ${Safe(pendingCallbacks.size)}")
      case Some(r: Ready[_]) =>
        log.debug(
          safe"Ready item: ${Safe(r.id)} is next on node: ${Safe(qid.pretty)}. Remaining after removal: ${Safe(pendingCallbacks.size - 1)}"
        )
        val _ = pendingCallbacks.remove(0)
        r.runCallback()
        processReadyCallbacks()
      case None =>
        /* Go back to the regular behaviour and enqueue stashed messages back into the actor mailbox. The
         * `StashedMessage` wrapper ensures that re-enqueued messages get processed as if they had arrived first. */
        log.debug(
          safe"Unbecoming on: ${Safe(qid.pretty)} Remaining size: ${Safe(pendingCallbacks.size)} stashed size: ${Safe(messageBuffer.size)}"
        )
        context.unbecome()
        messageBuffer.foreach { e =>
          log.debug(
            log"Unstashing message: ${e.message.toString} on node: ${Safe(qid.pretty)} stashed size: ${Safe(messageBuffer.size)}"
          )
          self.tell(StashedMessage(e.message), e.sender)
        }
        messageBuffer.clear()
    }
  }

  val messageBuffer: mutable.ArrayBuffer[Envelope] = mutable.ArrayBuffer.empty

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
    isResultSafe: Boolean
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

    // Temporarily change the actor behavior to only buffer messages
    if (pendingCallbacks.size == 1) {
      log.debug(
        log"Becoming PriorityStashingBehavior on: ${Safe(qid.pretty)} stashed size: ${Safe(messageBuffer.size)}"
      )
      context.become(
        {
          case StashedResultDelivery(id, result) =>
            log.debug(
              log"Result delivery for: ${Safe(id)} with payload: ${result.toString} on node: ${Safe(qid.pretty)}"
            )
            addResultToCallback(id, result, isResultSafe)
            // Every time a result is delivered, iterate through zero or more results to apply callback effects.
            processReadyCallbacks()

          /* We are are receiving a message that was un-stashed before. Re-stash it. */
          case StashedMessage(msg) =>
            messageBuffer += Envelope(msg, sender())
            log.debug(
              log"Restashed message: ${msg.toString} on node: ${Safe(qid.pretty)} size: ${Safe(messageBuffer.size)}"
            )

          case msg =>
            messageBuffer += Envelope(msg, sender())
            log.debug(
              log"Stashed message: ${msg.toString} on node: ${Safe(qid.pretty)} size: ${Safe(messageBuffer.size)}"
            )
        },
        discardOld = false
      )
    }

    // Schedule the message which will restore the previous actor behavior after the future completes.
    until.onComplete { (done: Try[_]) =>
      done.toEither.left.foreach(err =>
        log.debug(
          safe"pauseMessageProcessingUntil: future for: ${Safe(thisFutureId)} failed on node ${Safe(qid.debug)}"
        )
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

object StashedMessage {

  /** Combinator to produce a new priority function where a [[StashedMessage]] has slightly higher priority than the
    * underlying message it wraps, but otherwise the priorities of the underlying messages take precedence.
    */
  def priority(priorityFunction: Any => Int): Any => Int = {
    case StashedMessage(msg) => priorityFunction(msg) * 2
    case msg => priorityFunction(msg) * 2 + 1
  }
}

package com.thatdot.quine.graph.behavior

import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

import akka.actor.{Actor, ActorLogging}
import akka.dispatch.Envelope

import com.thatdot.quine.model.{QuineId, QuineIdProvider}

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
trait PriorityStashingBehavior extends Actor with ActorLogging {

  def qid: QuineId
  implicit def idProvider: QuineIdProvider

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
  var idCounter = 0 // Used only to uniquely identify futures in progress. OK if it rolls over.

  // Convenience method which doesn't eagerly evaluate strings.
  private def debug(msg: => String): Unit = if (log.isDebugEnabled) log.debug(msg)

  def enqueueCallback(callback: Pending[_]): Unit =
    pendingCallbacks.append(callback)

  def addResultToCallback[A](findId: Int, result: Try[A]): Unit =
    pendingCallbacks.indexWhere(_.id == findId) match {
      case -1 =>
        log.warning(
          "Received a result on node: {} (logged at INFO level) for a callback with unknown ID: {} in pendingCallbacks",
          qid.debug,
          findId
        )
        log.info("Received a result on node: {} for unknown callback ID: {}. Result was {}", qid.debug, findId, result)
      case i =>
        pendingCallbacks(i) match {
          case cb: PausedMessageCallback[A @unchecked] =>
            pendingCallbacks(i) = cb.ready(result)
        }
    }

  @tailrec
  private def processReadyCallbacks(): Unit = {
    debug(
      s"pendingCallbacks on node: ${qid.pretty} size: ${pendingCallbacks.size} first is: " +
      s"${pendingCallbacks.headOption} Stashed size: ${messageBuffer.size}"
    )
    pendingCallbacks.headOption match {
      case Some(_: Pending[_]) =>
        () // wait for this result to complete before more processing to maintain effect order
        debug(s"Pending item is next on node ${qid.pretty}. Size is: ${pendingCallbacks.size}")
      case Some(r: Ready[_]) =>
        debug(
          s"Ready item: ${r.id} is next on node: ${qid.pretty}. Remaining after removal: ${pendingCallbacks.size - 1}"
        )
        val _ = pendingCallbacks.remove(0)
        r.runCallback()
        processReadyCallbacks()
      case None =>
        /* Go back to the regular behaviour and enqueue stashed messages back into the actor mailbox. The
         * `StashedMessage` wrapper ensures that re-enqueued messages get processed as if they had arrived first. */
        debug(
          s"Unbecoming on: ${qid.pretty} Remaining size: ${pendingCallbacks.size} stashed size: ${messageBuffer.size}"
        )
        context.unbecome()
        messageBuffer.foreach { e =>
          debug(s"Unstashing message: ${e.message} on node: ${qid.pretty} stashed size: ${messageBuffer.size}")
          self.tell(StashedMessage(e.message), e.sender)
        }
        messageBuffer.clear()
    }
  }

  val messageBuffer: mutable.ArrayBuffer[Envelope] = mutable.ArrayBuffer.empty

  var isCalled = new AtomicBoolean()

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
    *         applied. The Success/Failure of the returned future will correspond to that of teh `until` Future. Note
    *         that this means that if the provided `onComplete` callback successfully applies effects when the `until`
    *         Future fails, then the returned Future will also have a `Failure` status after successfully applying the
    *         `onComplete` callback.
    */
  final protected def pauseMessageProcessingUntil[A](
    until: Future[A],
    onComplete: Try[A] => Unit = (_: Try[A]) => ()
  ): Future[Unit] = if (until.isCompleted && pendingCallbacks.isEmpty) {
    // If the future is already completed and no other callbacks are enqueued ahead of it, apply effects immediately
    Future.successful(onComplete(until.value.get))
  } else {
    if (log.isDebugEnabled && !isCalled.compareAndSet(false, true))
      throw new Exception(s"pauseMessageProcessingUntil was called concurrently on node ${qid.pretty}!")

    val thisFutureId = idCounter
    idCounter += 1
    val pending = Pending(thisFutureId, onComplete)
    enqueueCallback(pending)

    // Temporarily change the actor behavior to only buffer messages
    if (pendingCallbacks.size == 1) {
      debug(s"Becoming on: ${qid.pretty} stashed size: ${messageBuffer.size}")
      context.become(
        {
          case StashedResultDelivery(id, result) =>
            debug(s"Result delivery for: $id with payload: $result on node: ${qid.pretty}")
            addResultToCallback(id, result)
            // Every time a result is delivered, iterate through zero or more results to apply callback effects.
            processReadyCallbacks()

          /* We are are receiving a message that was un-stashed before. Re-stash it. */
          case StashedMessage(msg) =>
            messageBuffer += Envelope(msg, sender())
            debug(s"Restashed message: $msg on node: ${qid.pretty} size: ${messageBuffer.size}")

          case msg =>
            messageBuffer += Envelope(msg, sender())
            debug(s"Stashed message: $msg on node: ${qid.pretty} size: ${messageBuffer.size}")
        },
        discardOld = false
      )
    }

    // Schedule the message which will restore the previous actor behavior after the future completes.
    until.onComplete { (done: Try[A]) =>
      done.recover { case err =>
        log.error(err, s"pauseMessageProcessingUntil: future failed on node ${qid.debug(idProvider)}")
      }
      self ! StashedResultDelivery(thisFutureId, done)
    }(context.dispatcher)

    if (log.isDebugEnabled && !isCalled.compareAndSet(true, false))
      throw new Exception(s"pauseMessageProcessingUntil was called concurrently on node ${qid.pretty}!")

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

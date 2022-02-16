package com.thatdot.quine.graph.behavior

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

import akka.actor.{Actor, ActorLogging}
import akka.dispatch.Envelope

import com.thatdot.quine.model.{QuineId, QuineIdProvider}

/** Functionality for pausing processing of messages while a future to complete
  *
  * Use this by calling [[pauseMessageProcessingUntil]] with the future. New
  * messages will be stashed until the actor is ready to handle them again, at
  * which point they will be re-enqueued.
  *
  * @note actors extending this trait should have a priority mailbox with the
  * priority function wrapped in [[StashedMessage.priority]] - that way, the
  * order of messages that get unstashed is correct.
  */
trait PriorityStashingBehavior extends Actor with ActorLogging {

  def qid: QuineId
  implicit def idProvider: QuineIdProvider

  var pendingCallbacks: Int = 0

  /** Pause message processing until a future is completed
    *
    * @param until computation which must finish before the actor resumes processing messages
    * @param onComplete action to run on the actor thread right after the computation finishes
    */
  final protected def pauseMessageProcessingUntil[A](
    until: Future[A],
    onComplete: Try[A] => Unit = (_: Try[A]) => ()
  ): Unit = {
    val messageBuffer = mutable.ArrayBuffer.empty[Envelope]

    pendingCallbacks += 1
    if (pendingCallbacks > 1) {
      log.error(
        s"Actor at ${qid.debug(idProvider)} is now awaiting multiple callbacks in pauseMessageProcessingUntil. The callbacks will probably complete in the wrong order"
      )
    }
    // Temporarily change the actor behaviour to just buffer messages
    context.become(
      {
        /* Go back to the regular behaviour and enqueue stashed messages back
         * into the actor mailbox. The `StashedMessage` wrapper ensures that
         * re-enqueued messages get processed as though they had arrived first.
         *
         * NB: as long as there is only 1 callback, this is always a self tell
         * (below), so the type will match
         */
        case StopStashing(result: Try[A @unchecked]) =>
          pendingCallbacks -= 1
          context.unbecome()
          messageBuffer.foreach(e => self.tell(StashedMessage(e.message), e.sender))
          onComplete(result)

        /* We are are receiving a message that was un-stashed before. Re-stash it. */
        case StashedMessage(msg) =>
          messageBuffer += Envelope(msg, sender())

        case msg =>
          messageBuffer += Envelope(msg, sender())
      },
      discardOld = false
    )

    // Schedule the message which will restore the previous actor behaviour
    until.onComplete { (done: Try[A]) =>
      done.recover { case err =>
        log.error(
          err,
          s"pauseMessageProcessingUntil: future failed on node ${qid.debug(idProvider)}"
        )
      }

      self ! StopStashing(done)
    }(context.dispatcher)
  }
}

/** Wrapper to represent a message that was re-enqued from a stash and
  * consequently should be prioritized over other messages of otherwise equal
  * priority that are already in the mailbox
  */
final case class StashedMessage(msg: Any)
final case class StopStashing[A](result: Try[A])

object StashedMessage {

  /** Combinator to produce a new priority function where a [[StashedMessage]]
    * has slightly higher priority than the underlying message it wraps, but
    * otherwise the priorities of the underlying messages take precedence.
    */
  def priority(priorityFunction: Any => Int): Any => Int = {
    case StashedMessage(msg) => priorityFunction(msg) * 2
    case msg => priorityFunction(msg) * 2 + 1
  }
}

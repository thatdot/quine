package com.thatdot.quine.graph.behavior

import scala.concurrent.Promise
import scala.concurrent.duration.DurationLong

import org.apache.pekko.actor.ActorLogging

import com.thatdot.quine.graph.{BaseNodeActorView, EventTime}
import com.thatdot.quine.model.Milliseconds

/** Mix this in last to build in a monotonic [[EventTime]] clock to the actor.
  *
  * The clocks logical time is advanced every time a new message is processed.
  * While processing of a message, [[tickEventSequence]] can be used to generate a fresh event time.
  */
trait ActorClock extends ActorLogging with PriorityStashingBehavior {

  this: BaseNodeActorView =>

  private var currentTime: EventTime = EventTime.fromMillis(Milliseconds.currentTime())
  private var eventOccurred: Boolean = false

  /** @returns fresh event time (still at the actor's current logical time) */
  final protected def tickEventSequence(): EventTime = {
    // don't tick the event sequence on the first event for a message,
    // since we want the event sequence to be zero-based, not one-based
    if (eventOccurred) currentTime = currentTime.tickEventSequence(Some(log))
    eventOccurred = true
    currentTime
  }

  /** @returns event time produced by the next call to [[tickEventSequence]]
    * @note do not use this for creating times which must be ordered!
    */
  final protected def peekEventSequence(): EventTime = currentTime.tickEventSequence(Some(log))

  /** @returns the millisecond of the most recent received message [[actorClockBehavior]] */
  final protected def previousMessageMillis(): Long = currentTime.millis

  protected def actorClockBehavior(inner: Receive): Receive = { case message: Any =>
    val previousMillis = currentTime.millis
    val systemMillis = System.currentTimeMillis()
    val atSysDiff = atTime.map(systemMillis - _.millis)

    // Time has gone backwards! Pause message processing until it is caught up
    if (systemMillis < previousMillis) {
      // Some systems will frequently report a clock going back several milliseconds, and Quine can handle this without
      // intervention, so log only at INFO level. If this message was due to an overflow, a warning will have already
      // been logged by EventTime
      log.info(
        "No more operations are available on node: {} during the millisecond: {}  This can occur because of high traffic to a single node (which will slow the stream slightly), or because the system clock has moved backwards. Previous time record was: {}",
        idProvider.customIdFromQid(qid).getOrElse(qid),
        systemMillis,
        previousMillis
      )

      // Re-enqueue this message. We'll process it when time has caught up
      self.tell(StashedMessage(message), sender())

      // Create a future that will complete in whatever the current backwards delay is
      val timeHasProbablyCaughtUp = Promise[Unit]()
      context.system.scheduler
        .scheduleOnce(
          delay = (previousMillis - systemMillis + 1).millis,
          runnable = (() => timeHasProbablyCaughtUp.success(())): Runnable
        )(context.system.dispatcher)

      // Pause message processing until system time has likely caught up to local actor millis
      val _ = pauseMessageProcessingUntil[Unit](timeHasProbablyCaughtUp.future, _ => ())
    } else {
      atSysDiff match {
        // Clock skew: if at-time is too far in the future, drop the message
        case Some(diff) if -diff > graph.maxCatchUpSleepMillis =>
          log.error("Dropping message because node at-time is {} ms in future", -diff)
        // Clock skew: if at-time is in the near future, resend the message when the
        // time difference has elapsed
        case Some(diff) if diff < 0 =>
          log.warning("Resending message with delay because node at-time is {} ms in future", -diff)
          context.system.scheduler
            .scheduleOnce(
              delay = (diff + 1).millis,
              runnable = (() => self.tell(StashedMessage(message), sender())): Runnable
            )(context.system.dispatcher)
          ()
        case _ =>
          currentTime = currentTime.tick(
            mustAdvanceLogicalTime = eventOccurred,
            newMillis = systemMillis
          )
          eventOccurred = false
          inner(message)
      }
    }
  }
}

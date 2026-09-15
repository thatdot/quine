package com.thatdot.quine.graph.behavior

import scala.concurrent.Promise
import scala.concurrent.duration.DurationLong

import com.thatdot.common.logging.Log.{ActorSafeLogging, Safe, SafeLoggableInterpolator}
import com.thatdot.common.logging.Pretty._
import com.thatdot.quine.graph.EventTime
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.util.Log.implicits._

/** Mix this in last to build in a monotonic [[EventTime]] clock to the actor.
  *
  * The clocks logical time is advanced every time a new message is processed.
  * While processing of a message, [[tickEventSequence]] can be used to generate a fresh event time.
  */
trait ActorClock extends ActorSafeLogging with PriorityStashingBehavior {

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

    // Time has gone backwards! Pause message processing until it is caught up
    if (systemMillis < previousMillis) {
      // Some systems will frequently report a clock going back several milliseconds, and Quine can handle this without
      // intervention, so log only at INFO level. If this message was due to an overflow, a warning will have already
      // been logged by EventTime
      log.info(
        safe"""No more operations are available on node: ${Safe(qid.pretty)} during the millisecond:
              |${Safe(systemMillis)}  This can occur because of high traffic to a single node (which
              |will slow the stream slightly), or because the system clock has moved backwards. Previous
              |time record was: ${Safe(previousMillis)}""".cleanLines,
      )

      // Re-enqueue this message. We'll process it when time has caught up
      self.tell(StashedMessage(message), sender())

      // Create a future that will complete in whatever the current backwards delay is
      val timeHasProbablyCaughtUp = Promise[Unit]()
      context.system.scheduler
        .scheduleOnce(
          delay = (previousMillis - systemMillis + 1).millis,
          runnable = (() => timeHasProbablyCaughtUp.success(())): Runnable,
        )(context.system.dispatcher)

      // Pause message processing until system time has likely caught up to local actor millis
      val _ = pauseMessageProcessingUntil[Unit](timeHasProbablyCaughtUp.future, _ => (), true)
    } else {
      currentTime = currentTime.tick(
        mustAdvanceLogicalTime = eventOccurred,
        newMillis = systemMillis,
      )
      eventOccurred = false
      inner(message)
    }
  }
}

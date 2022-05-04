package com.thatdot.quine.graph.behavior

import scala.concurrent.Promise
import scala.concurrent.duration.DurationLong

import akka.actor.ActorLogging

import com.thatdot.quine.graph.EventTime
import com.thatdot.quine.model.Milliseconds

/** Mix this in last to build in a monotonic [[EventTime]] clock to the actor.
  *
  * The clocks logical time is advanced every time a new message is processed.
  * While processing of a message, [[nextEventTime]] can be used to generate a fresh event time.
  */
trait ActorClock extends ActorLogging with PriorityStashingBehavior {

  private var currentTime: EventTime = EventTime.fromMillis(Milliseconds.currentTime())
  private var previousMillis: Long = 0
  private var eventOccurred: Boolean = false

  /** @returns fresh event time (still at the actor's current logical time) */
  final protected def nextEventTime(): EventTime = {
    eventOccurred = true
    val current = currentTime
    currentTime = currentTime.nextEventTime(Some(log))
    current
  }

  /** @returns event time produced by the most recent call to [[nextEventTime]]
    * @note do not use this for creating times which must be ordered!
    */
  final protected def latestEventTime(): EventTime = currentTime

  /** @returns millisecond of the last time the actor clock ticked (so not the
    * _current_ time, but the one before)
    */
  final protected def previousMillisTime(): Long = previousMillis

  protected def actorClockBehavior(inner: Receive): Receive = { case message: Any =>
    previousMillis = currentTime.millis
    val systemMillis = System.currentTimeMillis()

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
      val _ = pauseMessageProcessingUntil(timeHasProbablyCaughtUp.future)
    } else {
      currentTime = currentTime.tick(
        mustAdvanceLogicalTime = eventOccurred,
        newMillis = systemMillis
      )
      eventOccurred = false
      inner(message)
    }
  }
}

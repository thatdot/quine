package com.thatdot.quine.util

import scala.concurrent.duration.FiniteDuration

import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}

/** Emits a count of elements received, throttled to the given interval.
  * Guarantees the final count is emitted on upstream completion.
  *
  * Unlike `conflateWithSeed(...).throttle(...)`, this stage ensures that
  * the final accumulated count is always emitted before completing, even
  * if the upstream completes during the throttle delay period.
  *
  * @param interval minimum time between emitted count updates
  */
case class ProgressCounter(interval: FiniteDuration) extends GraphStage[FlowShape[Any, Long]] {
  val in: Inlet[Any] = Inlet("ProgressCounter.in")
  val out: Outlet[Long] = Outlet("ProgressCounter.out")
  override val shape: FlowShape[Any, Long] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private var count: Long = 0L
      private var lastEmittedCount: Long = 0L
      private var upstreamFinished: Boolean = false

      override def preStart(): Unit = {
        scheduleWithFixedDelay(ProgressCounter.TimerKey, interval, interval)
        pull(in)
      }

      override def onPush(): Unit = {
        count += 1
        pull(in)
      }

      override def onPull(): Unit =
        // If the upstream finished but we were backpressured, we need to check if there is a final value to emit that
        // hasn't already flowed downstream.
        if (upstreamFinished && haveUnseenFinalValueToEmit) {
          push(out, count)
          lastEmittedCount = count
          completeStage()
        } // Otherwise, demand is noted and we'll push on next timer tick or completion

      override def onUpstreamFinish(): Unit = {
        upstreamFinished = true
        if (haveUnseenFinalValueToEmit) {
          if (isAvailable(out)) {
            push(out, count)
            lastEmittedCount = count
            completeStage()
          }
          // else: will emit in onPull when downstream requests
        } else {
          // No new count to emit, complete immediately
          completeStage()
        }
      }

      override protected def onTimer(timerKey: Any): Unit =
        if (count > lastEmittedCount && isAvailable(out)) {
          push(out, count)
          lastEmittedCount = count
        }

      private def haveUnseenFinalValueToEmit: Boolean = count > lastEmittedCount || count == 0

      setHandlers(in, out, this)
    }
}

object ProgressCounter {
  private case object TimerKey
}

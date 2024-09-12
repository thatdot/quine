package com.thatdot.quine.graph.metrics

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import com.codahale.metrics.Timer

object implicits {
  implicit final class TimeFuture(private val timer: Timer) extends AnyVal {

    /** Time how long a future takes to complete (success or failure is not differentiated)
      *
      * @param future what to time
      * @param timer how to do the timing
      * @return the future value
      */
    def time[T](future: => Future[T]): Future[T] = {
      val ctx = timer.time()
      val theFuture =
        try future
        catch {
          case NonFatal(err) =>
            ctx.stop()
            throw err
        }
      theFuture.onComplete(_ => ctx.stop())(ExecutionContext.parasitic)
      theFuture
    }
  }
}

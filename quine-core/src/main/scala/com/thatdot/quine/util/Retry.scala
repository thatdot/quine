package com.thatdot.quine.util

import scala.compat.ExecutionContexts
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import akka.actor.Scheduler
import akka.pattern.after

object Retry {
  def until[T](
    attempt: => Future[T],
    condition: T => Boolean,
    attempts: Int,
    delay: FiniteDuration,
    scheduler: Scheduler
  )(ec: ExecutionContext): Future[T] = attempt.flatMap(result =>
    if (condition(result))
      Future.successful(result)
    else if (attempts > 0)
      after(delay, scheduler)(until(attempt, condition, attempts - 1, delay, scheduler)(ec))(ec)
    else
      Future.failed(new NoSuchElementException("Ran out of attempts trying to retry; last value was: " + result))
  )(ec)

  def untilDefined[T](
    attempt: => Future[Option[T]],
    attempts: Int,
    delay: FiniteDuration,
    scheduler: Scheduler
  )(ec: ExecutionContext): Future[T] =
    until[Option[T]](attempt, _.isDefined, attempts, delay, scheduler)(ec).map(_.get)(ExecutionContexts.parasitic)

}

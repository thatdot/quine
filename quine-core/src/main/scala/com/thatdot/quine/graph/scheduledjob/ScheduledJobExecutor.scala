package com.thatdot.quine.graph.scheduledjob

import scala.concurrent.Future

import com.thatdot.quine.model.Milliseconds

/** App-provided executor a scheduler driver calls to run a scheduled job's work. The scheduler is
  * generic: it mints `executionId` at dispatch and hands over the opaque `jobType`/`payload`, which
  * the app routes by type. The returned Future completes when the execution reaches a terminal
  * state, letting the driver advance run-state and re-arm.
  */
trait ScheduledJobExecutor {
  def execute(jobName: String, executionId: String, jobType: String, payload: String): Future[Unit]
}

/** App-provided sweeper a scheduler driver calls on each wake. The app may scope it to records no
  * live host is responsible for (each host cleaning up its own on its own schedule) — the driver
  * just provides the periodic trigger on the active/manager host.
  */
trait ScheduledJobSweeper {
  def sweepExpired(now: Milliseconds): Future[Unit]
}

/** Outcome of a create request against the name-keyed job registry. Jobs are identified by name;
  * `AlreadyExists` is returned when a job of that name is already registered and the request did not
  * ask to update it in place.
  */
sealed trait ScheduledJobCreateOutcome
object ScheduledJobCreateOutcome {

  /** A new job was registered under this name. */
  case object Created extends ScheduledJobCreateOutcome

  /** An existing job of this name was replaced in place (requested via `updateIfExists`). */
  case object Updated extends ScheduledJobCreateOutcome

  /** A job of this name already existed and the request did not ask to update it. */
  case object AlreadyExists extends ScheduledJobCreateOutcome
}

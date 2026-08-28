package com.thatdot.quine.app.model.jobs

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.graph.scheduledjob.{
  ScheduledJobCreateOutcome,
  ScheduledJobDriver,
  ScheduledJobExecutor,
  ScheduledJobSweeper,
}

/** Single-host (OSS) [[JobService]]: a thin adapter mapping the app's [[Job]]/[[JobStatus]] types
  * onto the shared [[ScheduledJobDriver]], which owns all run-state, persistence, timing, and
  * dispatch. Always active on the one host.
  */
class JobScheduler(
  graph: BaseGraph,
  executor: ScheduledJobExecutor,
  sweeper: ScheduledJobSweeper,
) extends JobService {

  implicit private val ec: ExecutionContext = graph.system.dispatcher

  private val driver =
    new ScheduledJobDriver(graph, JobScheduler.Key, executor = () => Some(executor), sweeper = () => Some(sweeper))

  /** Rehydrate on startup: re-fires interrupted runs (at-least-once), arms the timer, and runs an
    * activation sweep. Completes when the load has resolved.
    */
  def start(): Future[Unit] = driver.load()

  /** Cancel the timer (e.g. on shutdown). */
  def stop(): Unit = driver.stop()

  override def createJob(job: Job, updateIfExists: Boolean): Future[ScheduledJobCreateOutcome] =
    // Single-host: the API awaits the driver's Future directly, so there is no internal ask/retry to
    // dedup — a fresh id per call is fine (the driver's idempotency path is simply never taken).
    driver.createJob(
      job.name,
      job.work.jobType,
      job.work.payloadJson,
      job.schedule,
      updateIfExists,
      requestId = java.util.UUID.randomUUID().toString,
    )

  override def getJobs: Future[Map[String, JobStatus]] =
    driver.getJobs.map(_.view.mapValues(JobStatus.fromState).toMap)

  override def deleteJob(name: String): Future[Option[JobStatus]] =
    driver.deleteJob(name).map(_.map(JobStatus.fromState))
}

object JobScheduler {

  /** Global metadata key for the single-host scheduled-job registry. */
  val Key: String = "job_schedule_state"
}

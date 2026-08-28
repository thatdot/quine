package com.thatdot.quine.app.model.jobs

import java.util.UUID

import scala.concurrent.Future
import scala.util.Try

import cats.data.NonEmptyList
import io.circe.syntax._
import io.circe.{Json, parser}

import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.graph.scheduledjob.{
  ScheduleSpec,
  ScheduledJobCreateOutcome,
  ScheduledJobExecutor,
  ScheduledJobState,
}

/** The work a [[Job]] performs on each fire. The scheduler layer only sees the opaque
  * (`jobType`, `payloadJson`) pair; this ADT is where the app assigns those tags meaning. Future
  * job types are new variants here plus a case in [[JobWork.decode]] — no scheduler changes.
  */
sealed trait JobWork {
  def jobType: String
  def payloadJson: String
}
object JobWork {
  val BackgroundQueryType: String = "background-query"

  /** Dispatch one execution of the given background query per fire. */
  final case class RunBackgroundQuery(query: BackgroundQuery) extends JobWork {
    override def jobType: String = BackgroundQueryType
    // Persist with the secret-preserving encoder so destination credentials survive to later fires
    // (the default encoder redacts them).
    override def payloadJson: String = {
      import com.thatdot.common.security.Secret.Unsafe._
      query.asJson(BackgroundQuery.preservingEncoder).noSpaces
    }
  }

  /** Back-compat default for payloads persisted before `destinations` existed: stream to `Drop` so
    * such jobs keep firing (as no-ops) rather than failing to decode.
    */
  private val legacyDestinations: Json =
    NonEmptyList.one[QuineDestinationSteps](QuineDestinationSteps.Drop).asJson

  /** Reconstruct the typed work from the scheduler's opaque (jobType, payload) pair. */
  def decode(jobType: String, payloadJson: String): Either[String, JobWork] = jobType match {
    case BackgroundQueryType =>
      parser
        .parse(payloadJson)
        .map { json =>
          // Legacy payloads predate `destinations` (default them to Drop so old jobs still decode)
          // and named the status-record retention `resultExpiryMillis` (map to the current key).
          json.mapObject { obj =>
            val withDestinations =
              if (obj.contains("destinations")) obj else obj.add("destinations", legacyDestinations)
            withDestinations("resultExpiryMillis") match {
              case Some(expiry) if !withDestinations.contains("statusExpiryMillis") =>
                withDestinations.add("statusExpiryMillis", expiry).remove("resultExpiryMillis")
              case _ => withDestinations
            }
          }
        }
        .flatMap(_.as[BackgroundQuery])
        .map(RunBackgroundQuery.apply)
        .left
        .map(_.toString)
    case other => Left(s"Unknown job type: $other")
  }

  /** The app-side dispatch a scheduler driver calls: decode the opaque work by type and run it,
    * linking the execution record to the dispatching job (by name). Shared by the OSS and enterprise
    * apps.
    */
  def executorFor(runner: BackgroundQueryRunner): ScheduledJobExecutor = new ScheduledJobExecutor {
    override def execute(jobName: String, executionId: String, jobType: String, payload: String): Future[Unit] =
      decode(jobType, payload) match {
        case Right(RunBackgroundQuery(query)) =>
          Future
            .fromTry(Try(UUID.fromString(executionId)))
            .flatMap(execId => runner.run(execId, Some(jobName), query).done)(
              scala.concurrent.ExecutionContext.parasitic,
            )
        case Left(err) => Future.failed(new IllegalArgumentException(err))
      }
  }
}

/** A scheduled job: fires on `schedule` and dispatches its `work` (minting a fresh execution id per
  * fire). Identified by its `name` (unique across the registry). Driven by the elected cluster
  * manager in enterprise, the single host in OSS.
  */
final case class Job(
  name: String,
  schedule: ScheduleSpec,
  work: JobWork,
)

/** Point-in-time view of a scheduled job's state, served by [[JobService.getJobs]] keyed by job
  * name; the job's executions are discoverable through their status records (which carry the job's
  * name). Mirrored field-for-field by `ClusterCommand.ScheduledJobStatusWire` (which cannot depend
  * on this app-module type): a field added here must be added there and threaded through
  * `DistributedJobService.fromWire`.
  */
final case class JobStatus(
  jobType: String,
  schedule: ScheduleSpec,
  nextFireAtMillis: Option[Long],
  lastFireAtMillis: Option[Long],
  running: Boolean,
)
object JobStatus {
  def fromState(state: ScheduledJobState): JobStatus = JobStatus(
    jobType = state.jobType,
    schedule = state.schedule,
    nextFireAtMillis = state.nextFireAt.map(_.millis),
    lastFireAtMillis = state.lastFireAt.map(_.millis),
    running = state.inProgressSince.isDefined,
  )
}

/** How the app creates and inspects scheduled jobs. OSS: the local [[JobScheduler]] itself.
  * Enterprise: delegates to the elected cluster manager. Jobs are keyed by name.
  */
trait JobService {

  /** Register a scheduled job, keyed by its name. For an existing name, `updateIfExists = true`
    * replaces the definition in place ([[ScheduledJobCreateOutcome.Updated]]); `false` leaves it
    * untouched ([[ScheduledJobCreateOutcome.AlreadyExists]]).
    */
  def createJob(job: Job, updateIfExists: Boolean): Future[ScheduledJobCreateOutcome]
  def getJobs: Future[Map[String, JobStatus]]

  /** Remove a job (by name) and erase its persisted state, returning its status or `None` if no
    * such job. Its executions' status records are untouched and expire on their own.
    */
  def deleteJob(name: String): Future[Option[JobStatus]]
}

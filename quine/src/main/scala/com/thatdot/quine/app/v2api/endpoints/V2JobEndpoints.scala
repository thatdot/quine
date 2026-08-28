package com.thatdot.quine.app.v2api.endpoints

import java.time.Instant

import scala.concurrent.Future

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.description
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, Schema, path, statusCode}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.quine.app.model.jobs
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions._

object V2JobEndpointEntities {

  import StringOps.syntax._

  /** The timezone/DST contract for schedules, rendered into the create-job endpoint description.
    * Wall-clock schedules are evaluated against the local clock of their `timezone`, so a daylight
    * saving transition can drop a fire (a local time that does not exist) or offer one twice (a local
    * time that occurs twice); this spells out which happens for each schedule shape.
    */
  val TimezoneAndDstDocs: String =
    "**Time zones and daylight saving time**\n\n" +
    """The `hourly`, `daily`, `weekly` and `monthly` schedules are wall-clock schedules: each fire is
      |the next moment at which the local clock in `timezone` reads the requested time. `interval` is
      |not — it counts absolute elapsed time from its anchor and ignores time zones
      |entirely.""".asOneLine + "\n\n" +
    "`timezone` accepts any zone id known to the tz database of the running JVM, matched case-sensitively:\n\n" +
    """ - Region ids — `America/New_York`, `Europe/London`, `Australia/Lord_Howe` — observe that region's daylight saving rules.
      | - Fixed-offset ids — `UTC` (the default), `GMT`, `Z`, `-05:00`, `+05:30`, `Etc/GMT+5` — never shift, and so never skip or repeat a fire.
      | - Legacy aliases such as `US/Eastern` and `EST5EDT` are accepted. Bare abbreviations such as `EST` or `PST` are not, and are rejected with a 400.
      |""".stripMargin + "\n" +
    """Zone rules come from the tz database bundled with the running JVM, so a JVM or OS upgrade that
      |revises a zone's rules also changes the future fire times of jobs already scheduled in
      |it.""".asOneLine + "\n\n" +
    "*Spring forward — a local time that does not exist:*\n\n" +
    """ - `daily`, `weekly` and `monthly` do not shift the fire, they skip it. A `daily` at `02:30` in `America/New_York` fires on 2026-03-07 and then on 2026-03-09; nothing runs on 2026-03-08. A `weekly` at `SUNDAY 02:30` loses the entire week, and a `monthly` whose `dayOfMonth` lands on the transition day loses the entire month.
      | - `hourly` loses exactly one fire, leaving 23 that day.
      | - The skip covers the whole clock hour containing the transition, not only the minutes that literally do not exist. This is visible only in zones whose shift is not a whole hour: `Australia/Lord_Howe` moves 02:00 to 02:30, and a `daily` anywhere in `02:00`–`02:59` is skipped that day — including `02:45`, which does exist locally.
      |""".stripMargin + "\n" +
    "*Fall back — a local time that occurs twice:*\n\n" +
    """ - `daily`, `weekly` and `monthly` fire once, at the first (pre-transition) occurrence. A `daily` at `01:30` in `America/New_York` fires at `2026-11-01T05:30:00Z` and not again at `06:30Z`.
      | - `hourly` fires at both occurrences, giving 25 fires that day.
      | - Firing once is a consequence of advancing past the repeated hour, so a job whose next fire is first computed from a moment inside that hour does land on the second occurrence: creating the `01:30` daily job at `01:31` on the first pass fires it 59 minutes later, at `01:30` on the second pass.
      |""".stripMargin + "\n" +
    "*Consequences worth planning for:*\n\n" +
    """ - The absolute gap between consecutive wall-clock fires is not constant: a `daily` job fires 23 hours after its predecessor on a spring-forward day and 25 hours after it on a fall-back day. An action that assumes it covers exactly 24 hours of data will under- or over-cover on those two days.
      | - `interval` has the mirror-image behavior: it holds its absolute spacing and drifts against the local clock, so a 24h interval firing at 09:00 local fires at 10:00 local after a spring-forward. Use a wall-clock schedule to pin local time, `interval` to pin elapsed time.
      | - Fires missed while the scheduler is down collapse into a single fire on recovery, however many slots elapsed; the schedule then resumes normally. This is independent of DST but compounds with it.
      | - `nextFireAt` and `lastFireAt` in job status are absolute UTC instants, not local times — convert them into the job's `timezone` before comparing against `at`.
      | - A job that must never skip or double up belongs on `UTC` (the default), at the cost of its local firing time moving twice a year.
      |""".stripMargin

  /** Request to create a scheduled job that dispatches an action on a schedule. Jobs are keyed by
    * `name`.
    */
  final case class CreateJobRequest(
    @description("When the job fires.") schedule: Schedule,
    @description("The action dispatched on each fire.") action: Action,
    @description("Unique name identifying the job.") name: String,
    @description(
      "If a job with this name already exists: when true, replace its definition in place; " +
      "when false or unset, the request is rejected. Defaults to false. Replacing a job re-evaluates " +
      "its schedule as if newly created (run history is preserved), so an Interval schedule that still " +
      "omits startAt re-anchors to the replacement time and fires immediately again.",
    )
    updateIfExists: Option[Boolean] = None,
  )
  object CreateJobRequest {
    // Request body: decode strictly (reject unknown/misspelled fields with a 400 rather than
    // silently dropping them) and honor Scala default values, matching every other v2 request
    // entity (e.g. BackgroundQueryDef). Response entities below stay on the plain lenient
    // derivation since the server produces them.
    import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
    import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

    implicit val encoder: Encoder[CreateJobRequest] = deriveConfiguredEncoder
    implicit val decoder: Decoder[CreateJobRequest] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[CreateJobRequest] = Schema.derived
  }

  /** Response to a job creation: the job name. */
  final case class JobCreated(@description("Name of the created job.") name: String)
  object JobCreated {
    implicit val encoder: Encoder[JobCreated] = deriveEncoder
    implicit val decoder: Decoder[JobCreated] = deriveDecoder
    implicit lazy val schema: Schema[JobCreated] = Schema.derived
  }

  /** Point-in-time view of a scheduled job. */
  final case class JobStatus(
    name: String,
    @description("Kind of work this job dispatches (e.g. background-query).") jobType: String,
    @description("When this job fires.") schedule: Schedule,
    @description(
      "Next scheduled fire time as an absolute UTC instant — convert into the schedule's timezone " +
      "before comparing with its time of day. Absent for a schedule that never fires.",
    )
    nextFireAt: Option[Instant],
    @description("Most recent fire time as an absolute UTC instant.") lastFireAt: Option[Instant],
    @description("Whether a dispatched execution is currently in flight.") running: Boolean,
  )
  object JobStatus {
    implicit val encoder: Encoder[JobStatus] = deriveEncoder
    implicit val decoder: Decoder[JobStatus] = deriveDecoder
    implicit lazy val schema: Schema[JobStatus] = Schema.derived

    def fromStatus(name: String, status: jobs.JobStatus): JobStatus =
      JobStatus(
        name = name,
        jobType = status.jobType,
        schedule = Schedule.fromModel(status.schedule),
        nextFireAt = status.nextFireAtMillis.map(Instant.ofEpochMilli),
        lastFireAt = status.lastFireAtMillis.map(Instant.ofEpochMilli),
        running = status.running,
      )
  }
}

trait V2JobEndpoints extends V2QuineEndpointDefinitions with StringOps {

  import V2JobEndpointEntities._

  // Jobs are cluster-wide, not graph-scoped: the target graph lives in the action, not the path.
  private def jobBase = rawEndpoint("system", "jobs").tag("System Administration")

  val createJob: Endpoint[Unit, CreateJobRequest, Either[
    ServerError,
    Either[BadRequest, NotFound],
  ], JobCreated, Any] =
    jobBase
      .name("create-job")
      .summary("Create a scheduled job")
      .description(
        """Create a job (identified by name) that dispatches an action on a schedule, driven by
          |the elected manager (at-least-once). If a job with the name already exists the request is
          |rejected unless updateIfExists is true, in which case its definition is replaced in place.
          |Each dispatch mints an execution id; poll backgroundQueries?jobName=<name> for
          |status.""".asOneLine + "\n\n" +
        TimezoneAndDstDocs,
      )
      .in(jsonBody[CreateJobRequest])
      .post
      .errorOut(badRequestError("Invalid job.", "A job with that name already exists."))
      .errorOutEither(notFoundError("Graph not found."))
      .errorOutEither(serverError())
      .mapErrorOut(err => err.swap)(err => err.swap)
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[JobCreated])

  private val createJobServerEndpoint: ServerEndpoint[Any, Future] =
    createJob.serverLogic[Future] { request =>
      recoverServerErrorEither(appMethods.createJob(request))(JobCreated(_))
    }

  val listJobs: Endpoint[Unit, Unit, ServerError, Seq[JobStatus], Any] =
    jobBase
      .name("list-jobs")
      .summary("List scheduled jobs")
      .description("List all scheduled jobs.")
      .get
      .errorOut(serverError())
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Seq[JobStatus]])

  private val listJobsServerEndpoint: ServerEndpoint[Any, Future] =
    listJobs.serverLogic[Future] { _ =>
      recoverServerError(appMethods.listJobs())(identity)
    }

  val getJob: Endpoint[Unit, String, Either[ServerError, NotFound], JobStatus, Any] =
    jobBase
      .name("get-job")
      .summary("Get a scheduled job's status")
      .description("Retrieve the current status of the scheduled job with the given name.")
      .in(path[String]("name"))
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[JobStatus])
      .errorOut(serverError())
      .errorOutEither(notFoundError("No job with that name."))

  private val getJobServerEndpoint: ServerEndpoint[Any, Future] =
    getJob.serverLogic[Future] { name =>
      recoverServerErrorEitherFlat(appMethods.getJob(name))(identity)
    }

  val deleteJob: Endpoint[Unit, String, Either[ServerError, NotFound], JobStatus, Any] =
    jobBase
      .name("delete-job")
      .summary("Delete a scheduled job")
      .description(
        "Remove the job (by name) from the scheduler, cancel any of its background-query executions " +
        "still running, and erase its persisted state. Status records of past executions are left to " +
        "expire on their own (still queryable by the job's name until then). Returns the job's " +
        "status as it was before deletion.",
      )
      .in(path[String]("name"))
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[JobStatus])
      .errorOut(serverError())
      .errorOutEither(notFoundError("No job with that name."))

  private val deleteJobServerEndpoint: ServerEndpoint[Any, Future] =
    deleteJob.serverLogic[Future] { name =>
      recoverServerErrorEitherFlat(appMethods.deleteJob(name))(identity)
    }

  val jobEndpoints: List[ServerEndpoint[Any, Future]] = List(
    createJobServerEndpoint,
    listJobsServerEndpoint,
    getJobServerEndpoint,
    deleteJobServerEndpoint,
  )
}

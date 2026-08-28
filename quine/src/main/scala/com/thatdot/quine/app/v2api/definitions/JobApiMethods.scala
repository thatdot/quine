package com.thatdot.quine.app.v2api.definitions

import java.util.UUID

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import io.circe.Json
import shapeless.{:+:, CNil, Coproduct}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound}
import com.thatdot.quine.app.model.jobs.{BackgroundQueryRecord, ExecutionAction, Job, JobHost, JobWork}
import com.thatdot.quine.app.v2api.endpoints.Action
import com.thatdot.quine.app.v2api.endpoints.V2BackgroundQueryEndpointEntities.{
  BackgroundQueryDef,
  BackgroundQueryStatus,
}
import com.thatdot.quine.app.v2api.endpoints.V2JobEndpointEntities.{CreateJobRequest, JobStatus}
import com.thatdot.quine.compiler.cypher.{compile => compileCypher}
import com.thatdot.quine.graph.scheduledjob.{ScheduleSpec, ScheduledJobCreateOutcome}
import com.thatdot.quine.graph.{BaseGraph, NamespaceId, defaultNamespaceId}

/** API methods for the background-query and job endpoints, all scoped to the graph/namespace named
  * in the request path. The shared logic (validation, model building, scoping, record conversion)
  * lives here, driving the app through the [[JobHost]] interface both apps implement.
  */
trait JobApiMethods {

  val graph: BaseGraph

  /** The app-side collaborators (registry/runner/scheduler) these endpoints drive. */
  protected def jobHost: JobHost

  /** Error coproduct for the write endpoints: a missing graph is a `NotFound` (matching the rest of
    * V2), a bad payload/interval is a `BadRequest`.
    */
  private type JobErr = BadRequest :+: NotFound :+: CNil
  private def asBadRequest(br: BadRequest): JobErr = Coproduct[JobErr](br)
  private def asNotFound(nf: NotFound): JobErr = Coproduct[JobErr](nf)

  private def namespaceExists(namespace: NamespaceId): Boolean = graph.getNamespaces.contains(namespace)

  /** Whether a record belongs to the graph named in the path. */
  private def recordInNamespace(record: BackgroundQueryRecord, namespace: NamespaceId): Boolean =
    record.namespace == namespace.name

  private def executionNotFound(executionId: UUID, namespace: NamespaceId): NotFound =
    NotFound(s"No background query execution with id $executionId in graph '${namespace.name}'")

  private def jobNotFound(name: String): NotFound = NotFound(s"No job named '$name'")

  // Validate on the truncated millis: a sub-millisecond expiry truncates to 0 and would expire the
  // record immediately.
  private def validateStatusExpiry(statusExpiry: Option[FiniteDuration]): Option[BadRequest] =
    if (statusExpiry.exists(_.toMillis <= 0)) Some(BadRequest("statusExpiry must be positive"))
    else None

  /** Validate and normalize a name: trimmed, non-empty, no control characters. Callers must
    * register the *returned* name — otherwise `" foo"` and `"foo"` would mint near-duplicate keys
    * distinguishable only by invisible characters.
    */
  private def validateName(raw: String, what: String): Either[BadRequest, String] = {
    val name = raw.trim
    if (name.isEmpty) Left(BadRequest(s"$what must not be empty"))
    else if (name.exists(_.isControl)) Left(BadRequest(s"$what must not contain control characters"))
    else Right(name)
  }

  private def validateOptionalName(raw: Option[String], what: String): Either[BadRequest, Option[String]] =
    raw match {
      case None => Right(None)
      case Some(n) => validateName(n, what).map(Some(_))
    }

  /** Compile-check the Cypher at accept time (with the request's parameter names bound), so a query
    * that can never run is a `BadRequest` rather than a failure at every execution. Compilation is
    * cached, so an accepted query's execution reuses this work.
    */
  private def validateQuery(query: String, parameters: Map[String, Json]): Option[BadRequest] =
    Try(compileCypher(query, unfixedParameters = parameters.keys.toSeq)).failed.toOption.map { e =>
      BadRequest(s"Invalid query: ${e.getMessage}")
    }

  private def validateNamespace(namespace: NamespaceId): Option[NotFound] =
    if (namespaceExists(namespace)) None else Some(NotFound(s"Graph '${namespace.name}' not found"))

  /** Parse an optional namespace name from a job action into a `NamespaceId`. An absent name defaults
    * to the root graph; an ill-formed name folds into a `BadRequest` rather than escaping as a 500
    * (`NamespaceId.apply` throws on names outside `[a-z][a-z0-9]{0,15}`).
    */
  private def parseNamespace(name: Option[String]): Either[BadRequest, NamespaceId] =
    name match {
      case None => Right(defaultNamespaceId)
      case Some(n) => Try(NamespaceId(n)).toEither.left.map(_ => BadRequest(s"Invalid namespace: $n"))
    }

  /** Dispatch one background-query execution now, in the given graph, and return its id — or an error
    * (`NotFound` for a missing graph, `BadRequest` for an invalid name, expiry, or query). The id is
    * returned once the execution's status record exists, not once the query has finished.
    */
  def runBackgroundQuery(
    namespace: NamespaceId,
    request: BackgroundQueryDef,
  ): Future[Either[JobErr, UUID]] = {
    val validated: Either[JobErr, BackgroundQueryDef] = for {
      _ <- validateNamespace(namespace).map(asNotFound).toLeft(())
      name <- validateOptionalName(request.name, "Background query name").left.map(asBadRequest)
      _ <- validateStatusExpiry(request.statusExpiry).map(asBadRequest).toLeft(())
      _ <- validateQuery(request.query, request.parameters).map(asBadRequest).toLeft(())
    } yield request.copy(name = name)
    validated match {
      case Left(err) => Future.successful(Left(err))
      case Right(valid) =>
        jobHost.runBackgroundQuery(valid.toModel(namespace)).map(Right(_))(ExecutionContext.parasitic)
    }
  }

  /** Create a scheduled job, keyed by its name (system-scoped: the target graph comes from the
    * action, not the path), and return its name once durably recorded — or an error (`NotFound` for
    * a missing graph, `BadRequest` for an invalid name/schedule/query, or a name collision when
    * `updateIfExists` is not set).
    */
  def createJob(request: CreateJobRequest): Future[Either[JobErr, String]] =
    request.action match {
      case action: Action.BackgroundQuery =>
        parseNamespace(action.namespace) match {
          case Left(badNamespace) => Future.successful(Left(asBadRequest(badNamespace)))
          case Right(namespace) => createBackgroundQueryJob(request, action, namespace)
        }
    }

  private def createBackgroundQueryJob(
    request: CreateJobRequest,
    action: Action.BackgroundQuery,
    namespace: NamespaceId,
  ): Future[Either[JobErr, String]] = {
    // Validate everything up front (a bad schedule or query is a BadRequest, not a job that fails
    // every fire), registering the validated/normalized values rather than the raw ones.
    val validated: Either[JobErr, (String, ScheduleSpec, Option[String])] = for {
      _ <- validateNamespace(namespace).map(asNotFound).toLeft(())
      jobName <- validateName(request.name, "Job name").left.map(asBadRequest)
      queryName <- validateOptionalName(action.name, "Background query name").left.map(asBadRequest)
      schedule <- request.schedule.toModel.left.map(e => asBadRequest(BadRequest(e)))
      _ <- validateStatusExpiry(action.statusExpiry).map(asBadRequest).toLeft(())
      _ <- validateQuery(action.query, action.parameters).map(asBadRequest).toLeft(())
    } yield (jobName, schedule, queryName)
    validated match {
      case Left(err) => Future.successful(Left(err))
      case Right((jobName, schedule, queryName)) =>
        // The query's records carry a name: the query's own, else the job's.
        val query = action.copy(name = queryName).toModel(namespace)
        val named = query.copy(name = query.name.orElse(Some(jobName)))
        val job = Job(name = jobName, schedule = schedule, work = JobWork.RunBackgroundQuery(named))
        jobHost.jobService
          .createJob(job, request.updateIfExists.getOrElse(false))
          .map {
            case ScheduledJobCreateOutcome.AlreadyExists =>
              Left(asBadRequest(BadRequest(s"A job named '$jobName' already exists")))
            case _ => Right(jobName)
          }(ExecutionContext.parasitic)
    }
  }

  /** One job's status by name, or `NotFound`. */
  def getJob(name: String): Future[Either[NotFound, JobStatus]] =
    jobHost.jobService.getJobs.map { jobs =>
      jobs
        .get(name)
        .map(JobStatus.fromStatus(name, _))
        .toRight(jobNotFound(name))
    }(ExecutionContext.parasitic)

  /** All scheduled jobs. */
  def listJobs(): Future[Seq[JobStatus]] =
    jobHost.jobService.getJobs.map {
      _.iterator.map { case (name, status) => JobStatus.fromStatus(name, status) }.toSeq
    }(ExecutionContext.parasitic)

  /** Delete a job (by name): remove it from the scheduler, erase its persisted state, and cancel
    * any of its executions still in flight. Past executions' status records are left to expire on
    * their own (queryable by the job's name until then). Returns the deleted job's status, or
    * `NotFound`.
    */
  def deleteJob(name: String): Future[Either[NotFound, JobStatus]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    jobHost.jobService.deleteJob(name).flatMap {
      case None => Future.successful(Left(jobNotFound(name)))
      case Some(status) =>
        val deleted: Either[NotFound, JobStatus] = Right(JobStatus.fromStatus(name, status))
        // Cancelling in-flight executions is best-effort: the job itself is already deleted, so a
        // cleanup failure must not misreport the delete as failed or abort the remaining cancels.
        jobHost.backgroundQueryRegistry
          .list(Some(name))
          .flatMap { records =>
            val inFlight = records.filter(_.lastAction match {
              case _: ExecutionAction.Started => true
              case _ => false
            })
            Future.traverse(inFlight)(r => jobHost.cancelBackgroundQuery(r.executionId).recover { case _ => () })
          }
          .map(_ => deleted)
          .recover { case _ => deleted }
    }
  }

  /** One execution's status record in the given graph, or `NotFound` (absent, expired, or
    * in a different graph).
    */
  def getBackgroundQuery(
    namespace: NamespaceId,
    executionId: UUID,
  ): Future[Either[NotFound, BackgroundQueryStatus]] =
    jobHost.backgroundQueryRegistry
      .get(executionId)
      .map(
        _.filter(recordInNamespace(_, namespace))
          .map(BackgroundQueryStatus.fromRecord)
          .toRight(executionNotFound(executionId, namespace)),
      )(ExecutionContext.parasitic)

  /** All unexpired execution records in the given graph, optionally filtered by dispatching job name. */
  def listBackgroundQueries(namespace: NamespaceId, jobName: Option[String]): Future[Seq[BackgroundQueryStatus]] =
    jobHost.backgroundQueryRegistry
      .list(jobName)
      .map(
        _.filter(recordInNamespace(_, namespace)).map(BackgroundQueryStatus.fromRecord),
      )(ExecutionContext.parasitic)

  /** Cancel one execution (if still running, cluster-wide); or `NotFound` (absent, expired, or in a
    * different graph). The "cancelled" transition lands asynchronously as the executing host's
    * stream unwinds, so the returned record may still say "started". A no-op on terminal executions.
    */
  def cancelBackgroundQuery(
    namespace: NamespaceId,
    executionId: UUID,
  ): Future[Either[NotFound, BackgroundQueryStatus]] =
    jobHost.backgroundQueryRegistry
      .get(executionId)
      .flatMap {
        case Some(rec) if recordInNamespace(rec, namespace) =>
          jobHost
            .cancelBackgroundQuery(executionId)
            .map(_ => Right(BackgroundQueryStatus.fromRecord(rec)))(ExecutionContext.parasitic)
        case _ =>
          Future.successful(Left(executionNotFound(executionId, namespace)))
      }(ExecutionContext.parasitic)

  /** Delete one execution's status record (in the given graph); or `NotFound` (absent, expired, or
    * in a different graph). A still-running execution is cancelled first, cluster-wide, so it stops
    * streaming before its record is removed; the cancel is best-effort, so a failure there does not
    * abort the delete. Returns the record as it was at request time. The runner's terminal-write gate
    * keeps the cancelled run's late finish from resurrecting the removed record.
    */
  def deleteBackgroundQuery(
    namespace: NamespaceId,
    executionId: UUID,
  ): Future[Either[NotFound, BackgroundQueryStatus]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    jobHost.backgroundQueryRegistry
      .get(executionId)
      .flatMap {
        case Some(rec) if recordInNamespace(rec, namespace) =>
          val status = BackgroundQueryStatus.fromRecord(rec)
          val cancelFirst = rec.lastAction match {
            case ExecutionAction.Started() => jobHost.cancelBackgroundQuery(executionId).recover { case _ => () }
            case _ => Future.unit
          }
          cancelFirst
            .flatMap(_ => jobHost.backgroundQueryRegistry.delete(executionId))
            .map(_ => Right(status))
        case _ =>
          Future.successful(Left(executionNotFound(executionId, namespace)))
      }
  }
}

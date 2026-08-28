package com.thatdot.quine.app.v2api.endpoints

import java.time.Instant
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptyList
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Json}
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.{default, description, encodedExample}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, Schema, path, query, statusCode}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.api.v2.codec.ThirdPartyCodecs.scala.{finiteDurationDecoder, finiteDurationEncoder}
import com.thatdot.api.v2.schema.ThirdPartySchemas.cats._
import com.thatdot.api.v2.schema.ThirdPartySchemas.circe.mapStringJsonSchema
import com.thatdot.api.v2.schema.ThirdPartySchemas.scala.finiteDurationSchema
import com.thatdot.quine.app.model.jobs.{BackgroundQuery, BackgroundQueryRecord, ExecutionAction}
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.graph.NamespaceId

object V2BackgroundQueryEndpointEntities {

  /** Definition of a background query, shared by the run-now request and a job's `query` field.
    * The graph/namespace it runs in comes from the endpoint's `graph/{graphName}` path scope, not
    * the body.
    */
  final case class BackgroundQueryDef(
    @description("Cypher query to run.") query: String,
    @description(
      "Destinations the query's result rows are streamed to (like Standing Query outputs). " +
      "At least one; use `Drop` for a pure side-effect run.",
    )
    destinations: NonEmptyList[QuineDestinationSteps],
    @description("Optional human-readable name, surfaced in execution records.") name: Option[String] = None,
    @description("Query parameters, if any.") @default(Map.empty[String, Json])
    parameters: Map[String, Json] = Map.empty,
    @description(
      "How long execution status records are retained after the execution terminates (e.g. \"1h\", \"30m\"). " +
      "Default one week. Retention is counted from termination, so a still-running execution stays " +
      "visible however long it runs, no matter what this is set to.",
    )
    @encodedExample("168h")
    statusExpiry: Option[FiniteDuration] = None,
  ) {
    def toModel(namespace: NamespaceId): BackgroundQuery =
      BackgroundQuery.fromApi(query, name, namespace, parameters, destinations, statusExpiry)
  }
  object BackgroundQueryDef {
    // Derive via the shared discriminator config (as Action.BackgroundQuery does), so its flattened
    // sibling decodes identically: `.withDefaults` makes fields with a Scala default — notably
    // `parameters` — omittable on the wire, and `.withStrictDecoding` rejects unknown/misspelled
    // fields. Scoped to this companion so the response types below keep plain derivation.
    import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
    import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

    implicit val encoder: Encoder[BackgroundQueryDef] = deriveConfiguredEncoder
    implicit val decoder: Decoder[BackgroundQueryDef] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[BackgroundQueryDef] = Schema.derived
  }

  /** Response to a run request: the execution id to poll for status. */
  final case class BackgroundQueryCreated(@description("Id of the created execution.") id: UUID)
  object BackgroundQueryCreated {
    implicit val encoder: Encoder[BackgroundQueryCreated] = deriveEncoder
    implicit val decoder: Decoder[BackgroundQueryCreated] = deriveDecoder
    implicit lazy val schema: Schema[BackgroundQueryCreated] = Schema.derived
  }

  /** Status of one background-query execution. Result rows are streamed to the query's destinations,
    * not returned here.
    */
  final case class BackgroundQueryStatus(
    id: UUID,
    @description("Name of the scheduled job that dispatched this execution, if any.") jobName: Option[String],
    name: Option[String],
    query: String,
    @description(
      "One of: started, completed, failed, cancelled, interrupted. \"interrupted\" means the " +
      "executing host restarted or left the cluster mid-run, so the execution ended without a " +
      "recorded outcome.",
    ) status: String,
    @description("Host the execution runs (or ran) on.") hostId: String,
    @description("Total rows streamed to the destinations (when completed).") totalRowCount: Option[Long] = None,
    @description("Result column names (when completed).") columns: Option[Seq[String]] = None,
    @description("Error message (when failed).") error: Option[String] = None,
    @description("When this record expires and is swept.") expiresAt: Instant,
  )
  object BackgroundQueryStatus {
    implicit val encoder: Encoder[BackgroundQueryStatus] = deriveEncoder
    implicit val decoder: Decoder[BackgroundQueryStatus] = deriveDecoder
    implicit lazy val schema: Schema[BackgroundQueryStatus] = Schema.derived

    def fromRecord(record: BackgroundQueryRecord): BackgroundQueryStatus = {
      val base = BackgroundQueryStatus(
        id = record.executionId,
        jobName = record.jobName,
        name = record.name,
        query = record.query,
        status = "started",
        hostId = record.hostId,
        expiresAt = Instant.ofEpochMilli(record.expiresAtMillis),
      )
      record.lastAction match {
        case ExecutionAction.Started() => base
        case ExecutionAction.Completed(totalRowCount, columns) =>
          base.copy(
            status = "completed",
            totalRowCount = Some(totalRowCount),
            columns = Some(columns),
          )
        case ExecutionAction.Failed(error) => base.copy(status = "failed", error = Some(error))
        case ExecutionAction.Cancelled() => base.copy(status = "cancelled")
        case ExecutionAction.Interrupted() => base.copy(status = "interrupted")
      }
    }
  }
}

trait V2BackgroundQueryEndpoints extends V2QuineEndpointDefinitions with GraphScopedEndpoints {

  import V2BackgroundQueryEndpointEntities._

  private def backgroundQueryBase(restPaths: String*) =
    graphScopedEndpoint("backgroundQueries" +: restPaths: _*).tag("Cypher Query Language")

  val runBackgroundQuery: Endpoint[Unit, (NamespaceId, BackgroundQueryDef), Either[
    ServerError,
    Either[BadRequest, NotFound],
  ], BackgroundQueryCreated, Any] =
    backgroundQueryBase()
      .name("run-background-query")
      .summary("Run a background query")
      .description(
        "Run a Cypher query out-of-band, in the graph named in the path, on the host that receives " +
        "this request. Result rows are streamed to the configured destinations (like Standing Query " +
        "outputs); they are not stored. Returns an execution id that can be polled for status, " +
        "cancelled, or tapped immediately — the response is sent once the execution's status record " +
        "exists, not once the query finishes. At-most-once: if the host dies mid-run the run is lost. " +
        "The status record is retained until its expiry, then swept.",
      )
      .in(jsonBody[BackgroundQueryDef])
      .post
      .errorOut(badRequestError("Invalid background query."))
      .errorOutEither(notFoundError("Graph not found."))
      .errorOutEither(serverError())
      .mapErrorOut(err => err.swap)(err => err.swap)
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[BackgroundQueryCreated])

  private val runBackgroundQueryServerEndpoint: ServerEndpoint[Any, Future] =
    runBackgroundQuery.serverLogic[Future] { case (namespace, request) =>
      recoverServerErrorEither(appMethods.runBackgroundQuery(namespace, request))(BackgroundQueryCreated(_))
    }

  val listBackgroundQueries: Endpoint[Unit, (NamespaceId, Option[String]), ServerError, Seq[
    BackgroundQueryStatus,
  ], Any] =
    backgroundQueryBase()
      .name("list-background-queries")
      .summary("List background queries")
      .description(
        "List unexpired background-query execution records in the graph named in the path, " +
        "optionally filtered by the name of the dispatching job.",
      )
      .in(query[Option[String]]("jobName").description("Only executions dispatched by the job with this name."))
      .get
      .errorOut(serverError())
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Seq[BackgroundQueryStatus]])

  private val listBackgroundQueriesServerEndpoint: ServerEndpoint[Any, Future] =
    listBackgroundQueries.serverLogic[Future] { case (namespace, jobName) =>
      recoverServerError(appMethods.listBackgroundQueries(namespace, jobName))(identity)
    }

  val getBackgroundQuery
    : Endpoint[Unit, (NamespaceId, UUID), Either[ServerError, NotFound], BackgroundQueryStatus, Any] =
    backgroundQueryBase()
      .name("get-background-query")
      .summary("Get background query status")
      .description(
        "Retrieve the current execution status of the background query with the given id " +
        "in the graph named in the path.",
      )
      .in(path[UUID]("id"))
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[BackgroundQueryStatus])
      .errorOut(serverError())
      .errorOutEither(notFoundError("No background query execution with that id in that graph."))

  private val getBackgroundQueryServerEndpoint: ServerEndpoint[Any, Future] =
    getBackgroundQuery.serverLogic[Future] { case (namespace, id) =>
      recoverServerErrorEitherFlat(appMethods.getBackgroundQuery(namespace, id))(identity)
    }

  val cancelBackgroundQuery
    : Endpoint[Unit, (NamespaceId, UUID), Either[ServerError, NotFound], BackgroundQueryStatus, Any] =
    backgroundQueryBase()
      .name("cancel-background-query")
      .summary("Cancel a background query")
      .description(
        "Cancel the execution if it is still running (anywhere in the cluster). The record " +
        "transitions to the terminal \"cancelled\" status as the executing host unwinds — the " +
        "returned record, read at request time, may still say \"started\"; poll for the transition. " +
        "The record is retained until it expires. Cancelling an already-terminal execution is a no-op.",
      )
      .in(CustomMethod.colonVerbPath[UUID]("id", "cancel"))
      .post
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[BackgroundQueryStatus])
      .errorOut(serverError())
      .errorOutEither(notFoundError("No background query execution with that id in that graph."))

  private val cancelBackgroundQueryServerEndpoint: ServerEndpoint[Any, Future] =
    cancelBackgroundQuery.serverLogic[Future] { case (namespace, id) =>
      recoverServerErrorEitherFlat(appMethods.cancelBackgroundQuery(namespace, id))(identity)
    }

  val deleteBackgroundQuery
    : Endpoint[Unit, (NamespaceId, UUID), Either[ServerError, NotFound], BackgroundQueryStatus, Any] =
    backgroundQueryBase()
      .name("delete-background-query")
      .summary("Delete a background query")
      .description(
        "Delete the execution's status record in the graph named in the path. If the execution is " +
        "still running it is cancelled first (cluster-wide), then its record is removed. Returns the " +
        "record as it was at request time. This drops the status immediately rather than waiting for " +
        "its expiry; a still-running execution's late finish is gated on the record still existing, so " +
        "it does not restore a deleted record. Deleting an already-absent execution is a NotFound.",
      )
      .in(path[UUID]("id"))
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[BackgroundQueryStatus])
      .errorOut(serverError())
      .errorOutEither(notFoundError("No background query execution with that id in that graph."))

  private val deleteBackgroundQueryServerEndpoint: ServerEndpoint[Any, Future] =
    deleteBackgroundQuery.serverLogic[Future] { case (namespace, id) =>
      recoverServerErrorEitherFlat(appMethods.deleteBackgroundQuery(namespace, id))(identity)
    }

  val backgroundQueryEndpoints: List[ServerEndpoint[Any, Future]] = List(
    runBackgroundQueryServerEndpoint,
    listBackgroundQueriesServerEndpoint,
    getBackgroundQueryServerEndpoint,
    cancelBackgroundQueryServerEndpoint,
    deleteBackgroundQueryServerEndpoint,
  )
}

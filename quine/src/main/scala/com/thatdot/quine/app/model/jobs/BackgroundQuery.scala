package com.thatdot.quine.app.model.jobs

import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptyList
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Json}

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.graph.NamespaceId

/** Definition of a background query: a Cypher query run out-of-band, dispatched directly ("run
  * now") or by a scheduled [[Job]]. Each *execution* gets its own UUID and reports a
  * [[BackgroundQueryRecord]] to the status registry; result rows are *streamed* to the configured
  * `destinations` (like Standing Query outputs), never persisted.
  *
  * @param query             the Cypher text to run
  * @param name              optional human-readable name, surfaced in the execution record
  * @param namespace         namespace to run in; `None` means the default namespace
  * @param parameters        Cypher parameters as JSON, converted to cypher values at run time
  * @param destinations      where result rows are streamed (the SQ-output destination catalog); at
  *                          least one, e.g. `Drop` for a pure side-effect run
  * @param statusExpiryMillis retention of the execution *status* record past its last transition; a
  *                          run outliving it finishes without a status, so set it longer than the
  *                          expected runtime
  */
final case class BackgroundQuery(
  query: String,
  destinations: NonEmptyList[QuineDestinationSteps],
  name: Option[String] = None,
  namespace: Option[String] = None,
  parameters: Map[String, Json] = Map.empty,
  statusExpiryMillis: Long = BackgroundQuery.DefaultStatusExpiryMillis,
)
object BackgroundQuery {
  val DefaultStatusExpiryMillis: Long = 7L * 24 * 60 * 60 * 1000 // one week

  /** Build a model query from the API's optional overrides. Shared by the run-now request body and
    * the scheduled-job action so the default/units logic lives in exactly one place.
    */
  def fromApi(
    query: String,
    name: Option[String],
    namespace: NamespaceId,
    parameters: Map[String, Json],
    destinations: NonEmptyList[QuineDestinationSteps],
    statusExpiry: Option[FiniteDuration],
  ): BackgroundQuery = BackgroundQuery(
    query = query,
    destinations = destinations,
    name = name,
    namespace = Some(namespace.name),
    parameters = parameters,
    statusExpiryMillis = statusExpiry.map(_.toMillis).getOrElse(DefaultStatusExpiryMillis),
  )

  implicit val encoder: Encoder[BackgroundQuery] = deriveEncoder
  implicit val decoder: Decoder[BackgroundQuery] = deriveDecoder

  /** Encoder preserving destination credentials, for persistence in a scheduled job's payload (the
    * default [[encoder]] redacts secrets, which would break auth on later fires). Requires witness
    * (`import Secret.Unsafe._`); same pattern as `StandingQueryResultWorkflow.preservingEncoder`.
    */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[BackgroundQuery] = {
    implicit val destinationsEncoder: Encoder[QuineDestinationSteps] = QuineDestinationSteps.preservingEncoder
    deriveEncoder
  }
}

/** The most recent lifecycle action of a background-query execution. Overwritten on each transition
  * (only the latest action is kept).
  */
sealed trait ExecutionAction
object ExecutionAction {

  /** Execution began and, until a terminal action overwrites this, may still be running. */
  final case class Started() extends ExecutionAction

  /** Execution finished successfully. Result rows were streamed to the query's destinations, not
    * stored; `totalRowCount` is the full count emitted and `columns` the result column names.
    */
  final case class Completed(
    totalRowCount: Long,
    columns: Vector[String],
  ) extends ExecutionAction

  /** Execution failed terminally (after any configured retries were exhausted, or a compile error). */
  final case class Failed(error: String) extends ExecutionAction

  /** Execution was cancelled while in flight. Rows streamed before the cancel are not retracted. */
  final case class Cancelled() extends ExecutionAction

  /** Execution was interrupted by infrastructure — its executing host restarted or departed the
    * cluster mid-run, before any terminal outcome could be recorded. Not written by the run itself;
    * reconciled onto a leftover `Started` record whose run no longer exists (see
    * [[BackgroundQueryStatusRegistry]]'s startup reconciliation and manager sweep). Terminal, so it
    * hides and is swept on the normal retention schedule.
    */
  final case class Interrupted() extends ExecutionAction

  implicit val encoder: Encoder[ExecutionAction] = deriveEncoder
  implicit val decoder: Decoder[ExecutionAction] = deriveDecoder
}

/** A cluster-visible record of one background-query execution: written by the executing host,
  * readable by any host, swept once `expiresAtMillis` passes (see `BackgroundQueryStatusRegistry`
  * for the sweep rules).
  *
  * @param jobName   the scheduled job that dispatched this execution, if any
  * @param namespace the resolved graph/namespace the query ran in — scopes API reads
  * @param hostId    the executing host: the record's sole writer, responsible for sweeping it
  */
final case class BackgroundQueryRecord(
  executionId: UUID,
  jobName: Option[String],
  namespace: String,
  hostId: String,
  name: Option[String],
  query: String,
  lastAction: ExecutionAction,
  expiresAtMillis: Long,
)
object BackgroundQueryRecord {
  implicit val encoder: Encoder[BackgroundQueryRecord] = deriveEncoder
  implicit val decoder: Decoder[BackgroundQueryRecord] = deriveDecoder
}

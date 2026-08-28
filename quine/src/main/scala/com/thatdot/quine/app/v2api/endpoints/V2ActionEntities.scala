package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptyList
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{default, description, encodedExample, title}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
import com.thatdot.api.v2.codec.ThirdPartyCodecs.scala.{finiteDurationDecoder, finiteDurationEncoder}
import com.thatdot.api.v2.schema.ThirdPartySchemas.cats._
import com.thatdot.api.v2.schema.ThirdPartySchemas.circe.mapStringJsonSchema
import com.thatdot.api.v2.schema.ThirdPartySchemas.scala.finiteDurationSchema
import com.thatdot.quine.app.model.jobs
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.graph.NamespaceId

/** The unit of work a scheduled job performs on each fire. A discriminated (`"type"`) sealed trait
  * so new action kinds can be added later without changing the job request shape. Because jobs are
  * system-scoped (not graph-scoped), the action — not the URL path — carries the graph/namespace it
  * targets. Currently the only action is a background query.
  */
sealed trait Action
object Action {

  /** Dispatch one execution of a background Cypher query per fire. The query fields mirror the
    * run-now request body (`BackgroundQueryDef`); the extra `namespace` field carries the target
    * graph, which for the graph-scoped run-now endpoint comes from the URL path instead.
    */
  @title("Background Query")
  @description("Dispatch one execution of a background Cypher query, in the named graph, per fire.")
  final case class BackgroundQuery(
    @description("Graph/namespace the query runs in; defaults to the default graph.")
    namespace: Option[String] = None,
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
  ) extends Action {

    /** Build the model query, targeting the (already-parsed) namespace — the action-side twin of
      * `BackgroundQueryDef.toModel`.
      */
    def toModel(resolvedNamespace: NamespaceId): jobs.BackgroundQuery =
      jobs.BackgroundQuery.fromApi(query, name, resolvedNamespace, parameters, destinations, statusExpiry)
  }

  implicit val encoder: Encoder[Action] = deriveConfiguredEncoder
  implicit val decoder: Decoder[Action] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[Action] = Schema.derived
}

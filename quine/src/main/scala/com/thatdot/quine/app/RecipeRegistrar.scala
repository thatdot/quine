package com.thatdot.quine.app

import scala.concurrent.{ExecutionContext, Future}

import shapeless.{:+:, CNil, Inl, Inr}

import com.thatdot.api.v2.{ErrorDetail, HasError, ResourceName}
import com.thatdot.quine.app.v2api.converters.ApiToIngest
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.GraphFeed
import com.thatdot.quine.app.v2api.definitions.QuineApiMethods
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.PropagateTo
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.graph.NamespaceId

/** Registers (and reports status of) a V2 recipe's standing queries, ingest streams, and graph feeds.
  *
  * [[RecipeInterpreterV2]] depends only on this narrow interface, so it never has to hold a
  * [[QuineApiMethods]] — which is a large façade over the entire V2 API surface, almost none of
  * which the recipe interpreter needs. The production implementation ([[ApiRecipeRegistrar]]) routes creation
  * and status reads through the same code path the V2 HTTP API uses, giving recipes the API's pre-flight
  * validation (output-workflow checks, Kafka/DLQ connectivity, enrichment-Cypher compilation,
  * graph-ready and namespace gates) instead of calling the graph's `add*` methods directly.
  *
  * The `register*` methods return `Left(messages)` on failure and `Right(_)` on success; ingest
  * creation additionally surfaces advisory warnings (e.g. Cypher analysis) on the `Right`. The
  * `*Status` reads return `None` when the resource no longer exists.
  */
trait RecipeRegistrar {
  def registerStandingQuery(
    name: ResourceName,
    namespace: NamespaceId,
    standingQuery: RecipeV2.StandingQueryDefinitionV2,
  ): Future[Either[Seq[String], Unit]]

  def registerIngest(
    name: ResourceName,
    namespace: NamespaceId,
    ingest: RecipeV2.IngestStreamV2,
  ): Future[Either[Seq[String], Set[String]]]

  /** Validate (read-only projection queries) and register all graph feeds for a namespace.
    * Replace-all, mirroring the V2 API's PUT semantics. `Left` carries one message per invalid feed.
    */
  def registerGraphFeeds(
    namespace: NamespaceId,
    feeds: Vector[GraphFeed],
  ): Future[Either[Seq[String], Unit]]

  /** Current status of a registered standing query, or `None` if it no longer exists. Read through
    * the same V2 API path as the HTTP GET, so recipe status reporting sees exactly what the API does.
    */
  def standingQueryStatus(
    name: String,
    namespace: NamespaceId,
  ): Future[Option[ApiStanding.StandingQuery.RegisteredStandingQuery]]

  /** Current status of a registered ingest stream, or `None` if it no longer exists. Read through
    * the same V2 API path as the HTTP GET.
    */
  def ingestStreamStatus(
    name: String,
    namespace: NamespaceId,
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]]
}

object RecipeRegistrar {

  /** Fallback names for anonymous recipe components are constructor-controlled and always
    * valid; the throw surfaces a template regression rather than letting a malformed name
    * reach the API.
    */
  def syntheticResourceName(s: String): ResourceName =
    ResourceName(s).getOrElse(
      throw new AssertionError(s"Synthetic recipe fallback name '$s' is not a valid ResourceName"),
    )
}

/** [[RecipeRegistrar]] backed by [[QuineApiMethods]], so recipes get the same validation as the
  * V2 API. Converts recipe-shaped definitions into the API request types, then delegates to
  * `createSQ` / `createIngestStream`.
  */
class ApiRecipeRegistrar(appMethods: QuineApiMethods)(implicit ec: ExecutionContext) extends RecipeRegistrar {

  import ApiToIngest.OssConversions._ // OfApiMethod[…, Oss.QuineIngestConfiguration] for createIngestStream

  override def registerStandingQuery(
    name: ResourceName,
    namespace: NamespaceId,
    standingQuery: RecipeV2.StandingQueryDefinitionV2,
  ): Future[Either[Seq[String], Unit]] = {
    val apiSqDef = ApiStanding.StandingQuery.StandingQueryDefinition(
      name = name,
      pattern = standingQuery.pattern,
      outputs = standingQuery.outputs.zipWithIndex.map { case (workflow, wfIndex) =>
        ApiStanding.StandingQueryResultWorkflow(
          name = workflow.name.getOrElse(RecipeRegistrar.syntheticResourceName(s"output-$wfIndex")),
          filter = workflow.filter,
          preEnrichmentTransformation = workflow.preEnrichmentTransformation,
          resultEnrichment = workflow.resultEnrichment.map(e =>
            QuineDestinationSteps.CypherQuery(query = e.query, parameter = e.parameter),
          ),
          destinations = workflow.destinations,
        )
      },
      includeCancellations = standingQuery.includeCancellations,
      inputBufferSize = standingQuery.inputBufferSize,
    )
    // PropagateTo.None preserves the recipe's historical behaviour of not waking nodes to
    // register the SQ; the pre-flight validation runs regardless of this choice.
    appMethods
      .createSQ(name.value, namespace, sq = apiSqDef, propagateTo = PropagateTo.None)
      .map(_.left.map(err => messagesFrom(err)).map(_ => ()))
  }

  override def registerIngest(
    name: ResourceName,
    namespace: NamespaceId,
    ingest: RecipeV2.IngestStreamV2,
  ): Future[Either[Seq[String], Set[String]]] = {
    val apiConfig = ApiIngest.Oss.QuineIngestConfiguration(
      name = name,
      source = ingest.source,
      query = ingest.query,
      parameter = ingest.parameter,
      transformation = ingest.transformation,
      parallelism = ingest.parallelism,
      maxPerSecond = ingest.maxPerSecond,
      onRecordError = ingest.onRecordError,
      onStreamError = ingest.onStreamError.getOrElse(ApiIngest.LogStreamError),
    )
    // memberIdx = None: recipes run on the member interpreting them.
    appMethods
      .createIngestStream(name.value, namespace, apiConfig, memberIdx = None)
      .map {
        case Left(err) => Left(messagesFrom(err))
        case Right((_, warnings)) => Right(warnings)
      }
  }

  override def registerGraphFeeds(
    namespace: NamespaceId,
    feeds: Vector[GraphFeed],
  ): Future[Either[Seq[String], Unit]] =
    // replaceGraphFeeds already runs the same read-only-projection validation as the V2 API PUT.
    appMethods.replaceGraphFeeds(namespace, feeds)

  override def standingQueryStatus(
    name: String,
    namespace: NamespaceId,
  ): Future[Option[ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    // getSQ's Left(NotFound) collapses to None — the reporter only distinguishes exists / gone.
    appMethods.getSQ(name, namespace).map(_.toOption)

  override def ingestStreamStatus(
    name: String,
    namespace: NamespaceId,
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] =
    // memberIdx = None: recipes report the stream on the member interpreting them.
    appMethods.ingestStreamStatus(name, namespace, memberIdx = None)

  /** Flatten an API error coproduct — whose members all carry a human-readable message — into
    * the list of message strings (primary message plus any `Help` detail lines).
    */
  private def messagesFrom[A <: HasError, B <: HasError](err: A :+: B :+: CNil): Seq[String] = err match {
    case Inl(e) => errorMessages(e)
    case Inr(Inl(e)) => errorMessages(e)
    case Inr(Inr(cnil)) => cnil.impossible
  }

  private def errorMessages(err: HasError): Seq[String] =
    err.message +: err.details.collect { case ErrorDetail.Help(m) => m }
}

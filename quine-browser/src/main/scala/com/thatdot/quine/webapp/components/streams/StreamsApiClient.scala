package com.thatdot.quine.webapp.components.streams

import scala.concurrent.{ExecutionContext, Future}

import io.circe.Json

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec, SchemaNode}

/** Typed API client for the Streams page. Hides endpoint discovery, path param
  * substitution, HTTP methods, and AIP-158 page-envelope unwrapping behind
  * named CRUD methods.
  *
  * Components receive this trait (or Signals/Observers derived from it) and
  * never import HttpClient, ApiOperationRegistry, or StreamOp directly.
  */
trait StreamsApiClient {

  /** The parsed spec — needed by SchemaFormRenderer for $ref resolution. */
  def spec: ParsedSpec

  // Schemas for create forms
  def ingestCreateSchema: Option[SchemaNode]
  def sqCreateSchema: Option[SchemaNode]
  def outputCreateSchema: Option[SchemaNode]

  // Ingest operations (reads live on the DataService; only mutations go through here)
  def createIngest(body: Json, memberIdx: Option[Int])(implicit ec: ExecutionContext): Future[Either[String, Json]]
  def deleteIngest(name: String, memberIdx: Option[Int])(implicit ec: ExecutionContext): Future[Either[String, Json]]
  def pauseIngest(name: String, memberIdx: Option[Int])(implicit ec: ExecutionContext): Future[Either[String, Json]]
  def resumeIngest(name: String, memberIdx: Option[Int])(implicit ec: ExecutionContext): Future[Either[String, Json]]

  // Standing query operations (reads live on the DataService; only mutations go through here)
  def createStandingQuery(body: Json)(implicit ec: ExecutionContext): Future[Either[String, Json]]
  def deleteStandingQuery(name: String)(implicit ec: ExecutionContext): Future[Either[String, Json]]
  def addOutput(sqName: String, body: Json)(implicit ec: ExecutionContext): Future[Either[String, Json]]
  def removeOutput(sqName: String, outputName: String)(implicit ec: ExecutionContext): Future[Either[String, Json]]
}

object StreamsApiClient {

  /** Create a StreamsApiClient from a parsed OpenAPI spec.
    * Internally discovers endpoints via V2 path pattern matching and
    * makes HTTP calls via dom.fetch.
    *
    * @param graphName the graph (namespace) to scope operations to. For OSS this is always "quine";
    *                  for Enterprise it should be the user's selected graph.
    */
  def apply(parsedSpec: ParsedSpec, baseUrl: String, graphName: String = "quine"): StreamsApiClient =
    new Impl(parsedSpec, baseUrl, graphName)

  private class Impl(val spec: ParsedSpec, baseUrl: String, graphName: String) extends StreamsApiClient {
    import com.thatdot.quine.webapp.openapi.{ApiOperationRegistry, HttpClient, StreamOp}

    private val registry = new ApiOperationRegistry(spec, baseUrl)

    /** Default path params: provides the graphName for graph-scoped endpoints. */
    private val defaultPathParams: Map[String, String] = Map("graphName" -> graphName)

    def ingestCreateSchema: Option[SchemaNode] = registry.requestSchema(StreamOp.CreateIngest)
    def sqCreateSchema: Option[SchemaNode] = registry.requestSchema(StreamOp.CreateStandingQuery)
    def outputCreateSchema: Option[SchemaNode] =
      registry
        .findEndpoint(StreamOp.AddSQOutput)
        .flatMap(_.requestBodySchema)
        .map(OpenApiParser.resolveNode(_, spec.schemas))

    def createIngest(body: Json, memberIdx: Option[Int])(implicit ec: ExecutionContext): Future[Either[String, Json]] =
      callWithBody(StreamOp.CreateIngest, body, memberHeader(memberIdx))

    /** A specific position routes the mutation to that one cluster member via the
      * `Quine-Member-Idx` header; `None` sends no header.
      */
    private def memberHeader(memberIdx: Option[Int]): Map[String, String] =
      memberIdx.map(idx => Map("Quine-Member-Idx" -> idx.toString)).getOrElse(Map.empty)

    def deleteIngest(name: String, memberIdx: Option[Int])(implicit
      ec: ExecutionContext,
    ): Future[Either[String, Json]] =
      registry.executeAction(StreamOp.DeleteIngest, name, defaultPathParams, memberHeader(memberIdx))

    def pauseIngest(name: String, memberIdx: Option[Int])(implicit
      ec: ExecutionContext,
    ): Future[Either[String, Json]] =
      registry.executeAction(StreamOp.PauseIngest, name, defaultPathParams, memberHeader(memberIdx))

    def resumeIngest(name: String, memberIdx: Option[Int])(implicit
      ec: ExecutionContext,
    ): Future[Either[String, Json]] =
      registry.executeAction(StreamOp.ResumeIngest, name, defaultPathParams, memberHeader(memberIdx))

    def createStandingQuery(body: Json)(implicit ec: ExecutionContext): Future[Either[String, Json]] =
      callWithBody(StreamOp.CreateStandingQuery, body)

    def deleteStandingQuery(name: String)(implicit ec: ExecutionContext): Future[Either[String, Json]] =
      registry.executeAction(StreamOp.DeleteStandingQuery, name, defaultPathParams, Map.empty)

    def addOutput(sqName: String, body: Json)(implicit ec: ExecutionContext): Future[Either[String, Json]] =
      registry.findEndpoint(StreamOp.AddSQOutput) match {
        case Some(ep) =>
          val params =
            defaultPathParams ++ ep.pathParams.filterNot(defaultPathParams.contains).headOption.map(_ -> sqName)
          HttpClient.call(ep, pathParams = params, body = Some(body), baseUrl = baseUrl)
        case None =>
          Future.successful(Left("Add output endpoint not found in API spec."))
      }

    def removeOutput(sqName: String, outputName: String)(implicit ec: ExecutionContext): Future[Either[String, Json]] =
      registry.executeAction(StreamOp.RemoveSQOutput, Seq(sqName, outputName), defaultPathParams)

    private def callWithBody(
      op: StreamOp,
      body: Json,
      headers: Map[String, String] = Map.empty,
    )(implicit ec: ExecutionContext): Future[Either[String, Json]] =
      registry.findEndpoint(op) match {
        case Some(ep) =>
          HttpClient.call(ep, pathParams = defaultPathParams, body = Some(body), headers = headers, baseUrl = baseUrl)
        case None => Future.successful(Left(s"Endpoint for $op not found in API spec."))
      }
  }
}

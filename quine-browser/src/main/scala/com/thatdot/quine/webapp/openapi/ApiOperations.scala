package com.thatdot.quine.webapp.openapi

import scala.concurrent.{ExecutionContext, Future}

import io.circe.Json

import com.thatdot.quine.openapi.{ApiEndpoint, OpenApiParser, ParsedSpec, SchemaNode}

/** High-level operations that the Streams UI page needs to perform.
  * These are matched to concrete API endpoints discovered from the OpenAPI spec.
  */
sealed trait StreamOp

object StreamOp {
  case object ListIngests extends StreamOp
  case object CreateIngest extends StreamOp
  case object DeleteIngest extends StreamOp
  case object PauseIngest extends StreamOp
  case object ResumeIngest extends StreamOp
  case object GetIngest extends StreamOp

  case object ListStandingQueries extends StreamOp
  case object CreateStandingQuery extends StreamOp
  case object DeleteStandingQuery extends StreamOp
  case object GetStandingQuery extends StreamOp
  case object AddSQOutput extends StreamOp
  case object RemoveSQOutput extends StreamOp
}

/** Discovers and maps logical [[StreamOp]] operations to concrete [[ApiEndpoint]] definitions
  * found in a parsed OpenAPI spec. Works with V2 API paths.
  */
class ApiOperationRegistry(spec: ParsedSpec, baseUrl: String) {

  // The graph segment is `quine` (literal) in OSS and `{graphName}` (path template) in
  // Enterprise — `[^/]+` matches either. The inner `\{[^}]+\}` captures vary by parameter
  // name across products.
  private val graphScope = """/api/v2/graph/[^/]+"""
  private val v2IngestBase = (graphScope + """/ingests/?$""").r
  private val v2IngestNamed = (graphScope + """/ingests/\{[^}]+\}$""").r
  private val v2IngestPause = (graphScope + """/ingests/\{[^}]+\}:pause$""").r
  private val v2IngestResume = (graphScope + """/ingests/\{[^}]+\}:resume$""").r
  private val v2SqBase = (graphScope + """/standingQueries/?$""").r
  private val v2SqNamed = (graphScope + """/standingQueries/\{[^}]+\}$""").r
  private val v2SqOutputBase = (graphScope + """/standingQueries/\{[^}]+\}/outputs/?$""").r
  private val v2SqOutputNamed = (graphScope + """/standingQueries/\{[^}]+\}/outputs/\{[^}]+\}$""").r

  private def matches(path: String, pattern: scala.util.matching.Regex): Boolean =
    pattern.findFirstIn(path).isDefined

  /** Find the [[ApiEndpoint]] for a given [[StreamOp]], or None if not found in the spec. */
  def findEndpoint(op: StreamOp): Option[ApiEndpoint] =
    spec.endpoints.find(e => matchesOp(e, op))

  /** Get the request body schema for an operation, with $refs resolved. */
  def requestSchema(op: StreamOp): Option[SchemaNode] =
    findEndpoint(op).flatMap(_.requestBodySchema).map(OpenApiParser.resolveNode(_, spec.schemas))

  /** Invoke an action endpoint, substituting `pathValues` into the endpoint's path
    * parameters in the order they appear (after any pre-supplied `extraPathParams`).
    * Used by table rows for play/pause/delete style actions where there's no body to send.
    *
    * @param extraPathParams pre-supplied path params (e.g., graphName) that are merged
    *                        before positionally-assigned values
    * Returns `Left` if the endpoint isn't in the spec; otherwise the raw
    * [[HttpClient]] response (still an `Either` for HTTP-level errors).
    */
  def executeAction(
    op: StreamOp,
    pathValues: Seq[String],
    extraPathParams: Map[String, String] = Map.empty,
    headers: Map[String, String] = Map.empty,
  )(implicit ec: ExecutionContext): Future[Either[String, Json]] =
    findEndpoint(op) match {
      case Some(ep) =>
        val remainingParams = ep.pathParams.filterNot(extraPathParams.contains)
        val params = extraPathParams ++ remainingParams.zip(pathValues).toMap
        HttpClient.call(ep, pathParams = params, headers = headers, baseUrl = baseUrl)
      case None =>
        Future.successful(Left(s"Endpoint for $op not found in API spec."))
    }

  /** Single-path-parameter convenience for the most common case. */
  def executeAction(
    op: StreamOp,
    pathValue: String,
    extraPathParams: Map[String, String],
    headers: Map[String, String],
  )(implicit ec: ExecutionContext): Future[Either[String, Json]] =
    executeAction(op, Seq(pathValue), extraPathParams, headers)

  private def matchesOp(e: ApiEndpoint, op: StreamOp): Boolean = {
    val m = e.method.toUpperCase
    val p = e.path
    op match {
      case StreamOp.ListIngests =>
        m == "GET" && matches(p, v2IngestBase)
      case StreamOp.CreateIngest =>
        m == "POST" && matches(p, v2IngestBase)
      case StreamOp.DeleteIngest =>
        m == "DELETE" && matches(p, v2IngestNamed)
      case StreamOp.GetIngest =>
        m == "GET" && matches(p, v2IngestNamed)
      case StreamOp.PauseIngest =>
        m == "POST" && matches(p, v2IngestPause)
      case StreamOp.ResumeIngest =>
        m == "POST" && matches(p, v2IngestResume)

      case StreamOp.ListStandingQueries =>
        m == "GET" && matches(p, v2SqBase)
      case StreamOp.CreateStandingQuery =>
        m == "POST" && matches(p, v2SqBase)
      case StreamOp.DeleteStandingQuery =>
        m == "DELETE" && matches(p, v2SqNamed)
      case StreamOp.GetStandingQuery =>
        m == "GET" && matches(p, v2SqNamed)
      case StreamOp.AddSQOutput =>
        m == "POST" && (matches(p, v2SqOutputBase) || matches(p, v2SqOutputNamed))
      case StreamOp.RemoveSQOutput =>
        m == "DELETE" && matches(p, v2SqOutputNamed)
    }
  }
}

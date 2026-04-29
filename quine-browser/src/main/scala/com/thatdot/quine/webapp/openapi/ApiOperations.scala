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

  // V2 release URL patterns: lowerCamelCase per AIP-122, `:verb` colon-method per AIP-136.
  // Note `\{[^}]+\}` matches an OpenAPI path-template parameter like `{name}` or
  // `{standingQueryName}` — the parameter name varies between OSS and Enterprise definitions.
  private val v2IngestBase = """/api/v2/ingests/?$""".r
  private val v2IngestNamed = """/api/v2/ingests/\{[^}]+\}$""".r
  private val v2IngestPause = """/api/v2/ingests/\{[^}]+\}:pause$""".r
  private val v2IngestResume = """/api/v2/ingests/\{[^}]+\}:resume$""".r
  private val v2SqBase = """/api/v2/standingQueries/?$""".r
  private val v2SqNamed = """/api/v2/standingQueries/\{[^}]+\}$""".r
  private val v2SqOutputBase = """/api/v2/standingQueries/\{[^}]+\}/outputs/?$""".r
  private val v2SqOutputNamed = """/api/v2/standingQueries/\{[^}]+\}/outputs/\{[^}]+\}$""".r

  private def matches(path: String, pattern: scala.util.matching.Regex): Boolean =
    pattern.findFirstIn(path).isDefined

  /** Find the [[ApiEndpoint]] for a given [[StreamOp]], or None if not found in the spec. */
  def findEndpoint(op: StreamOp): Option[ApiEndpoint] =
    spec.endpoints.find(e => matchesOp(e, op))

  /** Get the request body schema for an operation, with $refs resolved. */
  def requestSchema(op: StreamOp): Option[SchemaNode] =
    findEndpoint(op).flatMap(_.requestBodySchema).map(OpenApiParser.resolveNode(_, spec.schemas))

  /** Invoke an action endpoint, substituting `pathValues` into the endpoint's path
    * parameters in the order they appear. Used by table rows for play/pause/delete
    * style actions where there's no body to send.
    *
    * Returns `Left` if the endpoint isn't in the spec; otherwise the raw
    * [[HttpClient]] response (still an `Either` for HTTP-level errors).
    */
  def executeAction(
    op: StreamOp,
    pathValues: Seq[String],
  )(implicit ec: ExecutionContext): Future[Either[String, Json]] =
    findEndpoint(op) match {
      case Some(ep) =>
        val params = ep.pathParams.zip(pathValues).toMap
        HttpClient.call(ep, pathParams = params, baseUrl = baseUrl)
      case None =>
        Future.successful(Left(s"Endpoint for $op not found in API spec."))
    }

  /** Single-path-parameter convenience for the most common case. */
  def executeAction(
    op: StreamOp,
    pathValue: String,
  )(implicit ec: ExecutionContext): Future[Either[String, Json]] =
    executeAction(op, Seq(pathValue))

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

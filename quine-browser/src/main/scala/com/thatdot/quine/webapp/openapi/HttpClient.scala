package com.thatdot.quine.webapp.openapi

import scala.concurrent.{ExecutionContext, Future}
import scala.scalajs.js

import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.openapi.ApiEndpoint

/** Thin wrapper around dom.fetch() for making API calls discovered from the OpenAPI spec.
  * Handles path parameter substitution, JSON request/response bodies, AIP-193 error envelope
  * parsing, and AIP-158 pagination unwrapping for list responses.
  */
object HttpClient {

  /** Execute an HTTP request.
    *
    * @param method     HTTP method (GET, POST, PUT, DELETE)
    * @param path       URL path with {param} placeholders (e.g., "/api/v2/ingests/{name}")
    * @param pathParams Values to substitute for path placeholders
    * @param queryParams Query string parameters
    * @param body       Optional JSON request body
    * @param baseUrl    Base URL prefix (empty string for same-origin)
    * @return Either an error message or the parsed JSON response body
    */
  def execute(
    method: String,
    path: String,
    pathParams: Map[String, String] = Map.empty,
    queryParams: Map[String, String] = Map.empty,
    body: Option[Json] = None,
    baseUrl: String,
  )(implicit ec: ExecutionContext): Future[Either[String, Json]] = {
    val resolvedPath = pathParams.foldLeft(path) { case (p, (k, v)) =>
      p.replace(s"{$k}", js.URIUtils.encodeURIComponent(v))
    }

    val queryString =
      if (queryParams.isEmpty) ""
      else
        "?" + queryParams
          .map { case (k, v) => s"${js.URIUtils.encodeURIComponent(k)}=${js.URIUtils.encodeURIComponent(v)}" }
          .mkString("&")

    val url = s"$baseUrl$resolvedPath$queryString"

    val reqHeaders = new dom.Headers()
    reqHeaders.set("Accept", "application/json")
    body.foreach(_ => reqHeaders.set("Content-Type", "application/json"))

    val httpMethod = method.toUpperCase.asInstanceOf[dom.HttpMethod]
    val init = new dom.RequestInit {
      this.method = httpMethod
      this.headers = reqHeaders
    }
    body.foreach(b => init.body = b.noSpaces)

    (for {
      response <- dom.fetch(url, init).toFuture
      text <- response.text().toFuture
    } yield
      if (response.ok) {
        if (text.isEmpty) Right(Json.Null)
        else
          io.circe.parser.parse(text) match {
            case Right(json) => Right(json)
            case Left(err) => Left(s"Failed to parse response: ${err.getMessage}")
          }
      } else {
        // AIP-193 envelope: {"error": {"code", "status", "message", "details": [...]}}.
        // Pull the primary message and any user-facing Help details; ignore RequestInfo/ErrorInfo
        // (those are operator-facing — see ShowShort on the server side for the audit-log version).
        val fromAip193: Option[String] = io.circe.parser.parse(text).toOption.flatMap { json =>
          val errorCursor = json.hcursor.downField("error")
          errorCursor.get[String]("message").toOption.map { msg =>
            val helpHints: List[String] = errorCursor
              .downField("details")
              .focus
              .flatMap(_.asArray)
              .map(_.toList.flatMap { d =>
                d.hcursor.get[String]("type").toOption.flatMap {
                  case "Help" => d.hcursor.get[String]("message").toOption
                  case _ => None
                }
              })
              .getOrElse(Nil)
            if (helpHints.isEmpty) msg else s"$msg (${helpHints.mkString("; ")})"
          }
        }

        Left(s"HTTP ${response.status}: ${fromAip193.getOrElse(text.take(500))}")
      }).recover { case ex: Throwable =>
      dom.console.error("HTTP request failed:", ex.getMessage)
      Left("Could not connect to the server. Please check that Quine is running and try again.")
    }
  }

  /** Execute an API call using an ApiEndpoint definition. */
  def call(
    endpoint: ApiEndpoint,
    pathParams: Map[String, String] = Map.empty,
    queryParams: Map[String, String] = Map.empty,
    body: Option[Json] = None,
    baseUrl: String,
  )(implicit ec: ExecutionContext): Future[Either[String, Json]] =
    execute(
      method = endpoint.method,
      path = endpoint.path,
      pathParams = pathParams,
      queryParams = queryParams,
      body = body,
      baseUrl = baseUrl,
    )

  /** Unwrap the AIP-158 pagination envelope on V2 list responses:
    * `{"items": [...], "nextPageToken": "..."}` → the items array.
    *
    * Non-paginated responses (single resources, NoContent) have no `items` field, so this
    * falls back to returning the raw JSON unchanged.
    */
  def unwrapPageItems(json: Json): Json =
    json.hcursor.downField("items").focus.getOrElse(json)
}

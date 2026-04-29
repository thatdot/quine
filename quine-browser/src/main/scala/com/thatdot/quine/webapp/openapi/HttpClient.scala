package com.thatdot.quine.webapp.openapi

import scala.concurrent.{ExecutionContext, Future}
import scala.scalajs.js

import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.openapi.ApiEndpoint

/** Thin wrapper around dom.fetch() for making API calls discovered from the OpenAPI spec.
  * Handles path parameter substitution, JSON request/response bodies, and V2 response unwrapping.
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
        val parsed = io.circe.parser.parse(text).toOption

        // V2 errors: {"errors": [{"message": "...", "type": "..."}, ...]}
        val fromErrorsArray: Option[List[String]] = parsed
          .flatMap(_.hcursor.downField("errors").focus)
          .flatMap(_.asArray)
          .map(_.toList.flatMap(_.hcursor.get[String]("message").toOption))
          .filter(_.nonEmpty)

        // Top-level message field
        val fromTopMessage: Option[List[String]] = parsed
          .flatMap(_.hcursor.get[String]("message").toOption)
          .map(List(_))

        // Array of strings
        val fromStringArray: Option[List[String]] = parsed
          .flatMap(_.asArray)
          .map(_.toList.flatMap(_.asString))
          .filter(_.nonEmpty)

        val messages = fromErrorsArray
          .orElse(fromTopMessage)
          .orElse(fromStringArray)
          .getOrElse(List(text.take(500)))

        Left(s"HTTP ${response.status}: ${messages.mkString("; ")}")
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

  /** Unwrap a V2 SuccessEnvelope.Ok/Created response: {"content": ..., "warnings": [...], "message": ...} → the content field.
    * Falls back to the raw JSON for envelopes without a `content` field (e.g. Accepted, NoContent).
    */
  def unwrapV2Envelope(json: Json): Json =
    json.hcursor.downField("content").focus.getOrElse(json)
}

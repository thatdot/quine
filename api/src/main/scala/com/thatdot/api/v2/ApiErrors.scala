package com.thatdot.api.v2

import java.util.UUID

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.{EndpointOutput, Schema, statusCode}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances._
import com.thatdot.api.v2.schema.TapirJsonConfig.jsonBody
import com.thatdot.common.logging.Log._
import com.thatdot.quine.util.BaseError

/** Supplementary detail entries on an error response.
  *
  * Wire format follows AIP-193 / `google.rpc.Status.details[]` — each entry carries a
  * `type` discriminator alongside its content. New variants can be added as the API
  * evolves; existing clients ignore types they don't recognize.
  */
sealed trait ErrorDetail
object ErrorDetail {

  /** Server-assigned correlation ID for support to look up server-side context. Replaces
    * the previous practice of embedding the ID inside the human-readable `message` string.
    */
  final case class RequestInfo(requestId: String) extends ErrorDetail
  object RequestInfo {
    implicit lazy val schema: Schema[RequestInfo] = Schema.derived
  }

  /** Free-form supplementary text — hints, secondary error messages, decode-failure help. */
  final case class Help(message: String) extends ErrorDetail
  object Help {
    implicit lazy val schema: Schema[Help] = Schema.derived
  }

  /** Machine-readable error classification, modelled on `google.rpc.ErrorInfo`.
    *
    *   - `reason` is a stable SCREAMING_SNAKE_CASE code (e.g. `CYPHER_ERROR`); clients
    *     should switch on this rather than parsing `message`.
    *   - `domain` namespaces the reason so codes from different sources don't collide
    *     (we always use `quine.io`).
    *   - `metadata` carries optional structured context (line/column for parse errors, etc.).
    */
  final case class ErrorInfo(
    reason: String,
    domain: String = "quine.io",
    metadata: Map[String, String] = Map.empty,
  ) extends ErrorDetail
  object ErrorInfo {
    implicit lazy val schema: Schema[ErrorInfo] = Schema.derived
  }

  /** Convenience: classify a `BadRequest` as a Cypher syntax/compilation error. */
  val cypherError: ErrorInfo = ErrorInfo(reason = "CYPHER_ERROR")

  implicit lazy val schema: Schema[ErrorDetail] = Schema.derived
  implicit val encoder: Encoder[ErrorDetail] = deriveConfiguredEncoder
  implicit val decoder: Decoder[ErrorDetail] = deriveConfiguredDecoder
}

/** Body of an error response, per AIP-193 / `google.rpc.Status`.
  *
  *   - `code` mirrors the HTTP status code (also sent in the HTTP layer; included here so
  *     the body is self-contained when extracted from the response).
  *   - `status` is the canonical status name (e.g. `INVALID_ARGUMENT`, `NOT_FOUND`).
  *   - `message` is the primary human-readable error.
  *   - `details` carries additional structured context (correlation IDs, hints, etc.).
  */
final case class ErrorBody(
  code: Int,
  status: String,
  message: String,
  details: List[ErrorDetail] = Nil,
)
object ErrorBody {
  implicit lazy val schema: Schema[ErrorBody] = Schema.derived
  implicit val encoder: Encoder[ErrorBody] = deriveConfiguredEncoder
  implicit val decoder: Decoder[ErrorBody] = deriveConfiguredDecoder
}

/** Top-level error envelope. Every error response is `{"error": <ErrorBody>}` per AIP-193. */
final case class ApiError(error: ErrorBody)
object ApiError {
  implicit lazy val schema: Schema[ApiError] = Schema.derived
  implicit val encoder: Encoder[ApiError] = deriveConfiguredEncoder
  implicit val decoder: Decoder[ApiError] = deriveConfiguredDecoder
}

/** Common interface for all error-response types: every variant has a primary
  * `message` and an optional list of structured `details`.
  */
trait HasError {
  def message: String
  def details: List[ErrorDetail]
}

/** Per-HTTP-status error types, used for Tapir `oneOf` output dispatch.
  *
  * Each is a thin wrapper around the underlying [[ErrorBody]]: the case classes carry
  * only `message` + `details`, while the canonical `code` and `status` for the type are
  * fixed in their JSON encoders. Calling code constructs them with bare-message helpers
  * (`BadRequest("…")`) and the wire format comes out as `{"error": {"code": 400, …}}`.
  */
object ErrorResponse {

  private def envelopeEncoder[A](toBody: A => ErrorBody): Encoder[A] =
    Encoder.instance(a => ApiError(toBody(a)).asJson)

  private def envelopeDecoder[A](expectedCode: Int, build: (String, List[ErrorDetail]) => A): Decoder[A] =
    Decoder[ApiError].emap { ae =>
      if (ae.error.code == expectedCode) Right(build(ae.error.message, ae.error.details))
      else Left(s"Expected error.code=$expectedCode, got ${ae.error.code}")
    }

  /** 500 INTERNAL */
  final case class ServerError(message: String, details: List[ErrorDetail] = Nil) extends HasError
  object ServerError {
    val httpCode: Int = 500
    val canonicalStatus: String = "INTERNAL"

    def apply(error: BaseError): ServerError = ServerError(error.getMessage)
    def ofErrors(errors: List[BaseError]): ServerError =
      fromMessages(errors.map(_.getMessage))

    def fromMessages(messages: List[String]): ServerError = messages match {
      case Nil => ServerError("An internal error occurred.")
      case head :: tail => ServerError(head, tail.map(ErrorDetail.Help(_)))
    }

    private def toBody(e: ServerError): ErrorBody = ErrorBody(httpCode, canonicalStatus, e.message, e.details)
    implicit val encoder: Encoder[ServerError] = envelopeEncoder(toBody)
    implicit val decoder: Decoder[ServerError] = envelopeDecoder(httpCode, ServerError(_, _))
    implicit val schema: Schema[ServerError] = ApiError.schema.as[ServerError]
  }

  /** 400 INVALID_ARGUMENT */
  final case class BadRequest(message: String, details: List[ErrorDetail] = Nil) extends HasError
  object BadRequest {
    val httpCode: Int = 400
    val canonicalStatus: String = "INVALID_ARGUMENT"

    def apply(error: BaseError): BadRequest = BadRequest(error.getMessage)
    def ofErrorStrings(errors: List[String]): BadRequest = fromMessages(errors)
    def ofErrors(errors: List[BaseError]): BadRequest = fromMessages(errors.map(_.getMessage))

    def fromMessages(messages: List[String]): BadRequest = messages match {
      case Nil => BadRequest("Bad request.")
      case head :: tail => BadRequest(head, tail.map(ErrorDetail.Help(_)))
    }

    private def toBody(e: BadRequest): ErrorBody = ErrorBody(httpCode, canonicalStatus, e.message, e.details)
    implicit val encoder: Encoder[BadRequest] = envelopeEncoder(toBody)
    implicit val decoder: Decoder[BadRequest] = envelopeDecoder(httpCode, BadRequest(_, _))
    implicit val schema: Schema[BadRequest] = ApiError.schema.as[BadRequest]
  }

  /** 404 NOT_FOUND */
  final case class NotFound(message: String, details: List[ErrorDetail] = Nil) extends HasError
  object NotFound {
    val httpCode: Int = 404
    val canonicalStatus: String = "NOT_FOUND"

    def apply(error: BaseError): NotFound = NotFound(error.getMessage)
    def ofErrors(errors: List[BaseError]): NotFound = fromMessages(errors.map(_.getMessage))

    def fromMessages(messages: List[String]): NotFound = messages match {
      case Nil => NotFound("Not found.")
      case head :: tail => NotFound(head, tail.map(ErrorDetail.Help(_)))
    }

    private def toBody(e: NotFound): ErrorBody = ErrorBody(httpCode, canonicalStatus, e.message, e.details)
    implicit val encoder: Encoder[NotFound] = envelopeEncoder(toBody)
    implicit val decoder: Decoder[NotFound] = envelopeDecoder(httpCode, NotFound(_, _))
    implicit val schema: Schema[NotFound] = ApiError.schema.as[NotFound]
  }

  /** 401 UNAUTHENTICATED */
  final case class Unauthorized(message: String, details: List[ErrorDetail] = Nil) extends HasError
  object Unauthorized {
    val httpCode: Int = 401
    val canonicalStatus: String = "UNAUTHENTICATED"

    private def toBody(e: Unauthorized): ErrorBody = ErrorBody(httpCode, canonicalStatus, e.message, e.details)
    implicit val encoder: Encoder[Unauthorized] = envelopeEncoder(toBody)
    implicit val decoder: Decoder[Unauthorized] = envelopeDecoder(httpCode, Unauthorized(_, _))
    implicit val schema: Schema[Unauthorized] = ApiError.schema.as[Unauthorized]
    implicit val loggable: AlwaysSafeLoggable[Unauthorized] = u => s"Unauthorized: ${u.message}"
  }

  /** 503 UNAVAILABLE */
  final case class ServiceUnavailable(message: String, details: List[ErrorDetail] = Nil) extends HasError
  object ServiceUnavailable {
    val httpCode: Int = 503
    val canonicalStatus: String = "UNAVAILABLE"

    def apply(error: BaseError): ServiceUnavailable = ServiceUnavailable(error.getMessage)
    def ofErrors(errors: List[BaseError]): ServiceUnavailable = fromMessages(errors.map(_.getMessage))

    def fromMessages(messages: List[String]): ServiceUnavailable = messages match {
      case Nil => ServiceUnavailable("Service unavailable.")
      case head :: tail => ServiceUnavailable(head, tail.map(ErrorDetail.Help(_)))
    }

    private def toBody(e: ServiceUnavailable): ErrorBody = ErrorBody(httpCode, canonicalStatus, e.message, e.details)
    implicit val encoder: Encoder[ServiceUnavailable] = envelopeEncoder(toBody)
    implicit val decoder: Decoder[ServiceUnavailable] = envelopeDecoder(httpCode, ServiceUnavailable(_, _))
    implicit val schema: Schema[ServiceUnavailable] = ApiError.schema.as[ServiceUnavailable]
  }
}

object ErrorResponseHelpers extends LazySafeLogging {

  /** Default error catching for server logic. The correlation ID is now a structured
    * `RequestInfo` detail entry (machine-extractable) rather than embedded in the message.
    */
  def toServerError(e: Throwable)(implicit logConfig: LogConfig): ErrorResponse.ServerError = {
    val correlationId = UUID.randomUUID().toString
    logger.error(log"Internal server error [correlationId=${Safe(correlationId)}]" withException e)

    ErrorResponse.ServerError(
      message = "An internal error occurred.",
      details = List(ErrorDetail.RequestInfo(correlationId)),
    )
  }

  /** Convert IllegalArgumentException to BadRequest with the exception's message */
  def toBadRequest(e: IllegalArgumentException): ErrorResponse.BadRequest =
    ErrorResponse.BadRequest(e.getMessage)

  def serverError(possibleReasons: String*)(implicit
    enc: Encoder[ErrorResponse.ServerError],
    dec: Decoder[ErrorResponse.ServerError],
    sch: Schema[ErrorResponse.ServerError],
  ): EndpointOutput[ErrorResponse.ServerError] =
    statusCode(StatusCode.InternalServerError).and {
      jsonBody[ErrorResponse.ServerError]
        .description(ErrorText.serverErrorDescription(possibleReasons: _*))
    }

  def badRequestError(possibleReasons: String*)(implicit
    enc: Encoder[ErrorResponse.BadRequest],
    dec: Decoder[ErrorResponse.BadRequest],
    sch: Schema[ErrorResponse.BadRequest],
  ): EndpointOutput[ErrorResponse.BadRequest] =
    statusCode(StatusCode.BadRequest).and {
      jsonBody[ErrorResponse.BadRequest]
        .description(ErrorText.badRequestDescription(possibleReasons: _*))
    }

  def notFoundError(possibleReasons: String*)(implicit
    enc: Encoder[ErrorResponse.NotFound],
    dec: Decoder[ErrorResponse.NotFound],
    sch: Schema[ErrorResponse.NotFound],
  ): EndpointOutput[ErrorResponse.NotFound] =
    statusCode(StatusCode.NotFound).and {
      jsonBody[ErrorResponse.NotFound]
        .description(ErrorText.notFoundDescription(possibleReasons: _*))
    }

  def unauthorizedError(possibleReasons: String*)(implicit
    enc: Encoder[ErrorResponse.Unauthorized],
    dec: Decoder[ErrorResponse.Unauthorized],
    sch: Schema[ErrorResponse.Unauthorized],
  ): EndpointOutput[ErrorResponse.Unauthorized] =
    statusCode(StatusCode.Unauthorized).and {
      jsonBody[ErrorResponse.Unauthorized]
        .description(ErrorText.unauthorizedErrorDescription(possibleReasons: _*))
    }
}

object ErrorText {

  private def notFoundDoc =
    """Not Found
      |
      |The resource referenced was not found.
      |
      |%s
      |
      |""".stripMargin

  private def badRequestDoc =
    s"""Bad Request
      |
      |  Something in your request is invalid, and could not be processed.
      |  Review your request and attempt to submit it again.
      |
      |  %s
      |
      |  Contact support if you continue to have issues.
      |
      |""".stripMargin

  private val serverErrorDoc =
    s"""Internal Server Error
      |
      |  Encountered an unexpected condition that prevented processing your request.
      |
      |  %s
      |
      |  Contact support if you continue to have issues.
      |
      |""".stripMargin

  private val unauthorizedDoc =
    s"""Unauthorized
       |
       |Permission to access a protected resource not found
       |
       |%s
       |
       |""".stripMargin

  /** Manually generate a markdown bullet list from the list of message strings. */
  private def buildErrorMessage(docs: String, messages: Seq[String]): String =
    if (messages.isEmpty) docs.format("")
    else {
      val bulletSeparator = "\n - "
      val msgString = f"Possible reasons:$bulletSeparator${messages.mkString(bulletSeparator)}"
      docs.format(msgString)
    }

  def badRequestDescription(messages: String*): String =
    buildErrorMessage(badRequestDoc, messages)

  def notFoundDescription(messages: String*): String =
    buildErrorMessage(notFoundDoc, messages)

  def serverErrorDescription(messages: String*): String =
    buildErrorMessage(serverErrorDoc, messages)

  def unauthorizedErrorDescription(messages: String*): String =
    buildErrorMessage(unauthorizedDoc, messages)
}

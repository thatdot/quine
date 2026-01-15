package com.thatdot.api.v2

import java.util.UUID

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.{EndpointOutput, Schema, statusCode}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances._
import com.thatdot.api.v2.schema.TapirJsonConfig.jsonBody
import com.thatdot.common.logging.Log._
import com.thatdot.quine.util.BaseError

/** Errors that api v2 cares to distinguish for reporting */
sealed trait ErrorType {
  val message: String
}

/** The types of errors that the api knows how to distinguish and report
  *
  *  Should be extended for all errors we want to be distinguished in an api response.
  *  See: [[BaseError]] for future extension.
  */
object ErrorType {

  /** General Api error that we don't have any extra information about */
  case class ApiError(message: String) extends ErrorType
  object ApiError {
    implicit lazy val schema: Schema[ApiError] = Schema.derived
    implicit val encoder: Encoder[ApiError] = deriveConfiguredEncoder
    implicit val decoder: Decoder[ApiError] = deriveConfiguredDecoder
  }

  /** Api error type for any sort of Decode Failure
    *
    * Used currently for a custom decode failure handler passed to Pekko Server Options.
    */
  case class DecodeError(message: String, help: Option[String] = None) extends ErrorType
  object DecodeError {
    implicit lazy val schema: Schema[DecodeError] = Schema.derived
    implicit val encoder: Encoder[DecodeError] = deriveConfiguredEncoder
    implicit val decoder: Decoder[DecodeError] = deriveConfiguredDecoder
  }

  /** Api error type for any Cypher Error
    *
    *  This could be further broken down based upon CypherException later.
    */
  case class CypherError(message: String) extends ErrorType
  object CypherError {
    implicit lazy val schema: Schema[CypherError] = Schema.derived
    implicit val encoder: Encoder[CypherError] = deriveConfiguredEncoder
    implicit val decoder: Decoder[CypherError] = deriveConfiguredDecoder
  }

  implicit lazy val schema: Schema[ErrorType] = Schema.derived
  implicit val encoder: Encoder[ErrorType] = deriveConfiguredEncoder
  implicit val decoder: Decoder[ErrorType] = deriveConfiguredDecoder
}

trait HasErrors extends Product with Serializable {
  def errors: List[ErrorType]

}

/** Provides the types of error codes that the api can give back to a user.
  *
  *  Maps directly to http error codes (400s to 500s)
  *  They are combined with Coproduct from shapeless where used. This should be updated to Union in scala 3.
  */
object ErrorResponse {

  case class ServerError(errors: List[ErrorType]) extends HasErrors
  case class BadRequest(errors: List[ErrorType]) extends HasErrors
  case class NotFound(errors: List[ErrorType]) extends HasErrors
  case class Unauthorized(errors: List[ErrorType]) extends HasErrors
  case class ServiceUnavailable(errors: List[ErrorType]) extends HasErrors

  implicit private val errorListSchema: Schema[List[ErrorType]] = ErrorType.schema.asIterable[List]

  object ServerError {
    def apply(error: String): ServerError = ServerError(List(ErrorType.ApiError(error)))
    def apply(error: ErrorType): ServerError = ServerError(List(error))
    def apply(error: BaseError): ServerError = ServerError(
      List(ErrorType.ApiError(error.getMessage)),
    )
    def ofErrors(errors: List[BaseError]): ServerError = ServerError(
      errors.map(err => ErrorType.ApiError(err.getMessage)),
    )
    implicit lazy val schema: Schema[ServerError] = Schema.derived
    implicit val encoder: Encoder[ServerError] = deriveConfiguredEncoder
    implicit val decoder: Decoder[ServerError] = deriveConfiguredDecoder
  }

  // It would be nice to take away the below methods once we have our errors properly coded.
  object BadRequest {
    def apply(error: String): BadRequest = BadRequest(List(ErrorType.ApiError(error)))
    def apply(error: ErrorType): BadRequest = BadRequest(List(error))
    def apply(error: BaseError): BadRequest = BadRequest(List(ErrorType.ApiError(error.getMessage)))
    def ofErrorStrings(errors: List[String]): BadRequest = BadRequest(errors.map(err => ErrorType.ApiError(err)))
    def ofErrors(errors: List[BaseError]): BadRequest = BadRequest(
      errors.map(err => ErrorType.ApiError(err.getMessage)),
    )
    implicit lazy val schema: Schema[BadRequest] = Schema.derived
    implicit val encoder: Encoder[BadRequest] = deriveConfiguredEncoder
    implicit val decoder: Decoder[BadRequest] = deriveConfiguredDecoder
  }

  object NotFound {
    def apply(error: String): NotFound = NotFound(List(ErrorType.ApiError(error)))
    def apply(error: ErrorType): NotFound = NotFound(List(error))
    def apply(error: BaseError): NotFound = NotFound(List(ErrorType.ApiError(error.getMessage)))
    def ofErrors(errors: List[BaseError]): NotFound = NotFound(errors.map(err => ErrorType.ApiError(err.getMessage)))
    implicit lazy val schema: Schema[NotFound] = Schema.derived
    implicit val encoder: Encoder[NotFound] = deriveConfiguredEncoder
    implicit val decoder: Decoder[NotFound] = deriveConfiguredDecoder
  }

  object Unauthorized {
    def apply(reason: String): Unauthorized = Unauthorized(List(ErrorType.ApiError(reason)))
    def apply(reason: ErrorType) = new Unauthorized(List(reason))
    implicit lazy val schema: Schema[Unauthorized] = Schema.derived
    implicit val encoder: Encoder[Unauthorized] = deriveConfiguredEncoder
    implicit val decoder: Decoder[Unauthorized] = deriveConfiguredDecoder
  }

  object ServiceUnavailable {
    def apply(error: String): ServiceUnavailable = ServiceUnavailable(List(ErrorType.ApiError(error)))
    def apply(error: ErrorType): ServiceUnavailable = ServiceUnavailable(List(error))
    def apply(error: BaseError): ServiceUnavailable = ServiceUnavailable(List(ErrorType.ApiError(error.getMessage)))
    def ofErrors(errors: List[BaseError]): ServiceUnavailable = ServiceUnavailable(
      errors.map(err => ErrorType.ApiError(err.getMessage)),
    )
    implicit lazy val schema: Schema[ServiceUnavailable] = Schema.derived
    implicit val encoder: Encoder[ServiceUnavailable] = deriveConfiguredEncoder
    implicit val decoder: Decoder[ServiceUnavailable] = deriveConfiguredDecoder
  }

}

object ErrorResponseHelpers extends LazySafeLogging {

  /** Default error catching for server logic.  Could use a second look once more errors are codified */
  def toServerError(e: Throwable)(implicit logConfig: LogConfig): ErrorResponse.ServerError = {
    val correlationId = UUID.randomUUID().toString
    logger.error(log"Internal server error [correlationId=${Safe(correlationId)}]" withException e)

    ErrorResponse.ServerError(
      s"An internal error occurred. Reference ID: $correlationId",
    )
  }

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

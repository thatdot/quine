package com.thatdot.quine.app.v2api.definitions

import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.{EndpointOutput, Schema, oneOfVariantFromMatchType, statusCode}

import com.thatdot.quine.util.BaseError

sealed trait CustomError
case class ServerError(errors: String) extends CustomError
case class BadRequest(errors: List[String]) extends CustomError
case class NotFound(errors: String) extends CustomError
case class Unauthorized(errors: String) extends CustomError
case class ServiceUnavailable(errors: String) extends CustomError

object BadRequest {
  def apply(error: String): BadRequest = BadRequest(List(error))
  def apply(error: BaseError): BadRequest = BadRequest(List(error.getMessage))
  def ofErrors(errors: List[BaseError]): BadRequest = BadRequest(errors.map(_.getMessage))
}
object CustomError {

  /** Placeholder. Should be (throwable -> CustomError) */
  def toCustomError(e: Throwable): ServerError = ServerError(e.getMessage)

  def badRequestError(productName: String, possibleReasons: String*)(implicit
    enc: Encoder[ErrorEnvelope[BadRequest]],
    dec: Decoder[ErrorEnvelope[BadRequest]],
    sch: Schema[ErrorEnvelope[BadRequest]],
  ): EndpointOutput.OneOfVariant[ErrorEnvelope[BadRequest]] =
    oneOfVariantFromMatchType(
      statusCode(StatusCode.BadRequest).and {
        jsonBody[ErrorEnvelope[BadRequest]]
          .description(ErrorText.badRequestDescription(productName, possibleReasons: _*))
      },
    )

  def notFoundError(possibleReasons: String*)(implicit
    enc: Encoder[ErrorEnvelope[NotFound]],
    dec: Decoder[ErrorEnvelope[NotFound]],
    sch: Schema[ErrorEnvelope[NotFound]],
  ): EndpointOutput.OneOfVariant[ErrorEnvelope[NotFound]] =
    oneOfVariantFromMatchType(
      statusCode(StatusCode.NotFound).and {
        jsonBody[ErrorEnvelope[NotFound]]
          .description(ErrorText.notFoundDescription(possibleReasons: _*))
      },
    )

  def serviceUnavailableError(productName: String, possibleReasons: String*)(implicit
    enc: Encoder[ErrorEnvelope[ServiceUnavailable]],
    dec: Decoder[ErrorEnvelope[ServiceUnavailable]],
    sch: Schema[ErrorEnvelope[ServiceUnavailable]],
  ): EndpointOutput.OneOfVariant[ErrorEnvelope[ServiceUnavailable]] =
    oneOfVariantFromMatchType(
      statusCode(StatusCode.ServiceUnavailable).and {
        jsonBody[ErrorEnvelope[ServiceUnavailable]]
          .description(ErrorText.badRequestDescription(productName, possibleReasons: _*))
      },
    )

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

  private def badRequestDoc(productName: String) =
    s"""Bad Request

  Something in your request is invalid, and $productName could not process it.
  Review your request and attempt to submit it again.

  %s

  Contact support if you continue to have issues.
  """.stripMargin

  private val serverErrorDoc =
    s"""Internal Server Error
      |
      |  Encountered an unexpected condition that prevented processing your request.
      |
      |  %s
      |
      |  Contact support if you continue to have issues.""".stripMargin

  /** Manually generate a markdown bullet list from the list of message strings. */
  private def buildErrorMessage(docs: String, messages: Seq[String]): String =
    if (messages.isEmpty) docs.format("")
    else {
      val bulletSeparator = "\n - "
      val msgString = f"Possible reasons:$bulletSeparator${messages.mkString(bulletSeparator)}"
      docs.format(msgString)
    }

  def badRequestDescription(productName: String, messages: String*): String =
    buildErrorMessage(badRequestDoc(productName), messages)

  def notFoundDescription(messages: String*): String =
    buildErrorMessage(notFoundDoc, messages)

  def serverErrorDescription(messages: String*): String =
    buildErrorMessage(serverErrorDoc, messages)
}

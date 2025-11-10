package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.Future

import io.circe.DecodingFailure
import io.circe.generic.auto._
import sttp.model.{Header, StatusCode}
import sttp.tapir.DecodeResult.Error.JsonDecodeException
import sttp.tapir.generic.auto._
import sttp.tapir.server.interceptor.decodefailure.{DecodeFailureHandler, DefaultDecodeFailureHandler}
import sttp.tapir.server.interceptor.exception.DefaultExceptionHandler
import sttp.tapir.server.model.ValuedEndpointOutput
import sttp.tapir.{DecodeResult, headers, statusCode, stringBody}

import com.thatdot.api.v2.ErrorResponse
import com.thatdot.api.v2.ErrorType.{ApiError, DecodeError}
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas

trait TapirDecodeErrorHandler extends V2IngestApiSchemas {

  /** Wrap 500 codes in [[ErrorResponse.ServerError]] use default behavior for other codes. */
  protected val customExceptionHandler: DefaultExceptionHandler[Future] =
    DefaultExceptionHandler[Future]((code: StatusCode, body: String) =>
      code match {
        case StatusCode.InternalServerError => serverErrorFailureResponse(Nil, body)
        case _ => ValuedEndpointOutput(statusCode.and(stringBody), (code, body))
      },
    )

  private def pretty(df: DecodingFailure): String = {
    val path = df.pathToRootString.getOrElse("").stripPrefix(".")
    if (path.nonEmpty) s"${df.message} at '$path'" else df.message
  }

  /** Drop‑in replacement for Tapir's default [[DecodeFailureHandler]] using [[DefaultDecodeFailureHandler]] functions:
    * [[DefaultDecodeFailureHandler.respond]] and message: [[DefaultDecodeFailureHandler.FailureMessages.failureMessage]].
    * Uses [[pretty]] to print the DecodingFailure resulting from, in our case, a YAML decode failure.  This Default
    * is needed because the default pekko behavior is to drop information about this type of failure.
    *
    * Attach this to [[PekkoHttpServerOptions]]
    * Behavior:
    * 1. Capture YAML errors and produce tapir style message with them.  Add help text for `type` field being wrong.
    *    Circe Decodes to CNil and reports an unhelpful message in this case:
    *     `JSON decoding to CNil should never happen at 'source')`
    * 2. Add help text for `type` field being wrong.
    *    Circe Decodes to CNil and reports an unhelpful message in this case:
    *     `(JSON decoding to CNil should never happen at 'source')`
    * 3. Otherwise: lift the error body into our [[ErrorResponse]] based around status code
    */
  protected val customHandler: DecodeFailureHandler[Future] = DecodeFailureHandler.apply { ctx =>
    Future.successful {
      /* Delegate to the default response handler and update messages and Response Body Type.
         [[respond]] is responsible for:
         - Determine response based around the ctx(Some) or skip and check other endpoints for a successful match(None)
       */
      DefaultDecodeFailureHandler.respond(ctx).map { case (code, headers) =>
        val defaultMsg = DefaultDecodeFailureHandler.FailureMessages.failureMessage(ctx)

        // Lift into our response code
        ctx.failure match {
          // -----------------------------------------------------------------
          // 1. Circe decoding failures (YAML)
          // -----------------------------------------------------------------
          case DecodeResult.Error(_, df: DecodingFailure) =>
            val failureSource = DefaultDecodeFailureHandler.FailureMessages.failureSourceMessage(ctx.failingInput)
            val msg = s"$failureSource (${pretty(df)})"
            val advise =
              if (df.message.contains("CNil"))
                Some("unknown or unsupported one of selection (check the 'type' field)")
              else
                None
            decodeFailureResponse(headers, msg, advise)

          // -----------------------------------------------------------------
          // 1. Circe decoding failures (JSON) CNil appears in message
          // -----------------------------------------------------------------
          case DecodeResult.Error(_, _: JsonDecodeException) if defaultMsg.contains("CNil") =>
            decodeFailureResponse(
              headers,
              defaultMsg,
              Some("unknown or unsupported one of selection (check the 'type' field)"),
            )

          // -----------------------------------------------------------------
          // 3. Otherwise: lift the error body into our [[ErrorResponse]] based around status code
          // -----------------------------------------------------------------
          case _ =>
            code match {
              case StatusCode.BadRequest => decodeFailureResponse(headers, defaultMsg)
              case StatusCode.InternalServerError => serverErrorFailureResponse(headers, defaultMsg)
              case StatusCode.NotFound => notFoundFailureResponse(headers, defaultMsg)
              case _ =>
                // Preserve Tapir's behavior for any status codes we haven't modelled explicitly
                DefaultDecodeFailureHandler
                  .failureResponse(code, headers, DefaultDecodeFailureHandler.FailureMessages.failureMessage(ctx))
            }

        }
      }
    }
  }

  private def decodeFailureResponse(
    headerList: List[Header],
    m: String,
    help: Option[String] = None,
  ): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(
      statusCode(StatusCode.BadRequest).and(headers).and(jsonBody[ErrorResponse.BadRequest]),
      (headerList, ErrorResponse.BadRequest(DecodeError(m, help))),
    )

  private def serverErrorFailureResponse(
    headerList: List[Header],
    m: String,
  ): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(
      statusCode(StatusCode.InternalServerError).and(headers).and(jsonBody[ErrorResponse.ServerError]),
      (headerList, ErrorResponse.ServerError(ApiError(m))),
    )

  private def notFoundFailureResponse(
    headerList: List[Header],
    m: String,
  ): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(
      statusCode(StatusCode.NotFound).and(headers).and(jsonBody[ErrorResponse.NotFound]),
      (headerList, ErrorResponse.NotFound(ApiError(m))),
    )

}

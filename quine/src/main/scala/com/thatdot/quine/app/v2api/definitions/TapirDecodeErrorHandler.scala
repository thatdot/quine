package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.Future

import io.circe.DecodingFailure
import sttp.model.{Header, StatusCode}
import sttp.tapir.DecodeResult.Error.JsonDecodeException
import sttp.tapir.server.interceptor.decodefailure.{DecodeFailureHandler, DefaultDecodeFailureHandler}
import sttp.tapir.server.interceptor.exception.DefaultExceptionHandler
import sttp.tapir.server.model.ValuedEndpointOutput
import sttp.tapir.{DecodeResult, headers, statusCode, stringBody}

import com.thatdot.api.v2.schema.TapirJsonConfig.jsonBody
import com.thatdot.api.v2.{ErrorDetail, ErrorResponse, TypeDiscriminatorConfig}

trait TapirDecodeErrorHandler extends TypeDiscriminatorConfig {

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

  /** Drop-in replacement for Tapir's default [[DecodeFailureHandler]]. Reuses
    * [[DefaultDecodeFailureHandler.respond]] for status-code routing and
    * [[DefaultDecodeFailureHandler.FailureMessages]] for default messaging, then
    * lifts the result into our [[ErrorResponse]] envelope so every decode failure
    * comes back wearing the AIP-193 shape.
    *
    * Attach this to [[PekkoHttpServerOptions]].
    *
    * Cases, in order:
    *   1. Circe `DecodingFailure` (YAML body): pekko's default drops the message,
    *      so reconstruct it via [[pretty]]. If the error mentions `CNil`, attach a
    *      help hint pointing at the `type` discriminator.
    *   2. Circe `JsonDecodeException` with `CNil` in the default message (JSON
    *      body): same `type`-field hint, default message is sufficient.
    *   3. Any other decode error with a usable `getMessage` (e.g. invalid namespace,
    *      invalid QuineId, bad edge direction): include the exception message in
    *      the response body.
    *   4. Otherwise: dispatch on the status code Tapir picked
    *      (400 → BadRequest, 404 → NotFound, 500 → ServerError; fall back to the
    *      default handler for anything else we haven't modelled).
    */
  protected val customHandler: DecodeFailureHandler[Future] = DecodeFailureHandler.apply { ctx =>
    Future.successful {
      DefaultDecodeFailureHandler.respond(ctx).map { case (code, headers) =>
        val defaultMsg = DefaultDecodeFailureHandler.FailureMessages.failureMessage(ctx)

        ctx.failure match {
          // 1. Circe decoding failures (YAML)
          case DecodeResult.Error(_, df: DecodingFailure) =>
            val failureSource = DefaultDecodeFailureHandler.FailureMessages.failureSourceMessage(ctx.failingInput)
            val msg = s"$failureSource (${pretty(df)})"
            val advise =
              if (df.message.contains("CNil"))
                Some("unknown or unsupported one of selection (check the 'type' field)")
              else
                None
            decodeFailureResponse(headers, msg, advise)

          // 2. Circe decoding failures (JSON) — CNil appears in the default message
          case DecodeResult.Error(_, _: JsonDecodeException) if defaultMsg.contains("CNil") =>
            decodeFailureResponse(
              headers,
              defaultMsg,
              Some("unknown or unsupported one of selection (check the 'type' field)"),
            )

          // 3. Non-Circe decode errors with a descriptive exception message
          //    (e.g., invalid namespace, invalid QuineId, bad edge direction)
          case DecodeResult.Error(_, ex) if ex.getMessage != null =>
            val failureSource = DefaultDecodeFailureHandler.FailureMessages.failureSourceMessage(ctx.failingInput)
            decodeFailureResponse(headers, s"$failureSource: ${ex.getMessage}")

          // 4. Otherwise: dispatch on the status code Tapir picked
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
      (headerList, ErrorResponse.BadRequest(m, help.map(ErrorDetail.Help(_)).toList)),
    )

  private def serverErrorFailureResponse(
    headerList: List[Header],
    m: String,
  ): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(
      statusCode(StatusCode.InternalServerError).and(headers).and(jsonBody[ErrorResponse.ServerError]),
      (headerList, ErrorResponse.ServerError(m)),
    )

  private def notFoundFailureResponse(
    headerList: List[Header],
    m: String,
  ): ValuedEndpointOutput[_] =
    ValuedEndpointOutput(
      statusCode(StatusCode.NotFound).and(headers).and(jsonBody[ErrorResponse.NotFound]),
      (headerList, ErrorResponse.NotFound(m)),
    )

}

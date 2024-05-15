package com.thatdot.quine.app.v2api.definitions

import java.nio.charset.StandardCharsets

import scala.annotation.unused
import scala.concurrent.Future

import io.circe.generic.auto._
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.generic.auto.schemaForCaseClass
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.{EndpointOutput, _}

/** Response Envelopes */
case class ErrorEnvelope[T](error: T)
case class ObjectEnvelope[T](data: T)

/** Component definitions for Tapir endpoints. */
trait V2EndpointDefinitions {

  type EndpointOutput[T] = Either[ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T]]

  def memberIdxParameter: EndpointInput[Option[Int]]
  def namespaceParameter: EndpointInput[Option[String]]

  val app: ApplicationApiInterface

  /** Matching error types to api-rendered error outputs. Api Status codes are returned by matching the type of the
    * custom error to the status code matched here
    */
  private val commonErrorOutput
    : EndpointOutput.OneOf[ErrorEnvelope[_ <: CustomError], ErrorEnvelope[_ <: CustomError]] =
    oneOf[ErrorEnvelope[_ <: CustomError]](
      oneOfVariantFromMatchType(
        statusCode(StatusCode.InternalServerError).and(jsonBody[ErrorEnvelope[ServerError]].description("bad request"))
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.BadRequest).and(jsonBody[ErrorEnvelope[BadRequest]].description("bad request"))
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.NotFound).and(jsonBody[ErrorEnvelope[NotFound]].description("not found"))
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.Unauthorized).and(jsonBody[ErrorEnvelope[Unauthorized]].description("unauthorized"))
      ),
      oneOfVariantFromMatchType(statusCode(StatusCode.NoContent).and(emptyOutputAs(ErrorEnvelope(NoContent)))),
      oneOfDefaultVariant(
        statusCode(StatusCode.InternalServerError).and(jsonBody[ErrorEnvelope[Unknown]].description("server error"))
      )
    )

  /** Base for api/v2 endpoints with common errors that expects the universal parameters memberIdx, namespace */
  def baseEndpoint[T](
    basePath: String
  )(implicit
    @unused schema: Schema[T],
    @unused encoder: Encoder[T],
    @unused decoder: Decoder[T]
  ): Endpoint[Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T], Any] = endpoint
    .in("api" / "v2" / basePath)
    .in(memberIdxParameter)
    .errorOut(commonErrorOutput)
    .out(jsonBody[ObjectEnvelope[T]])

  private def yamlBody[T]()(implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T]
  ): EndpointIO.Body[String, T] = stringBodyAnyFormat(YamlCodec.createCodec[T](), StandardCharsets.UTF_8)

  @unused
  def jsonOrYamlBody[T](implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T]
  ): EndpointIO.OneOfBody[T, T] =
    oneOfBody[T](jsonBody[T], yamlBody[T]())

  def runServerLogic[IN: Encoder, OUT: Decoder](
    cmd: ApiCommand,
    idx: Option[Int],
    in: IN,
    f: IN => Future[OUT]
  ): Future[EndpointOutput[OUT]]
}

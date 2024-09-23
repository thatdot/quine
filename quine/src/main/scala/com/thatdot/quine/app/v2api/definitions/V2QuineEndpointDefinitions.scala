package com.thatdot.quine.app.v2api.definitions

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import io.circe.generic.auto._
import io.circe.{Decoder, Encoder, Json}
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.DecodeResult.Value
import sttp.tapir.generic.auto.schemaForCaseClass
import sttp.tapir.json.circe.TapirJsonCirce
import sttp.tapir.{EndpointOutput, endpoint, _}

import com.thatdot.quine.app.v2api.definitions.CustomError.toCustomError
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.{Milliseconds, QuineId, QuineIdProvider}
import com.thatdot.quine.routes.IngestRoutes
import com.thatdot.quine.util.Log._

/** Response Envelopes */
case class ErrorEnvelope[T](error: T)
case class ObjectEnvelope[T](data: T)

/** Component definitions for Tapir endpoints. */
trait V2EndpointDefinitions extends TapirJsonCirce with LazySafeLogging {

  implicit protected def logConfig: LogConfig

  val appMethods: ApplicationApiMethods
  // ------- parallelism -----------
  val parallelismParameter: EndpointInput.Query[Option[Int]] = query[Option[Int]](name = "parallelism")
    .description(s"Operations to execute simultaneously. Default: `${IngestRoutes.defaultWriteParallelism}`")
  // ------- atTime ----------------

  type AtTime = Milliseconds

  /** Since timestamps get encoded as milliseconds since 1970 in the REST API,
    * it is necessary to define the serialization/deserialization to/from a long.
    */
  protected def toAtTime(rawTime: Long): DecodeResult[AtTime] = {
    val now = System.currentTimeMillis
    if (rawTime > now)
      DecodeResult.Error(rawTime.toString, new IllegalArgumentException(s"Times in the future are not supported"))
    else Value(Milliseconds(rawTime))
  }

  /** Schema for an at time */
  implicit val atTimeEndpointCodec: Codec[String, AtTime, TextPlain] = Codec.long.mapDecode(toAtTime)(_.millis)

  val atTimeParameter: EndpointInput.Query[Option[AtTime]] =
    query[Option[AtTime]]("atTime").description(
      "An integer timestamp in milliseconds since the Unix epoch representing the historical moment to query",
    )

  // ------- id ----------------
  protected def toQuineId(s: String): DecodeResult[QuineId] =
    idProvider.qidFromPrettyString(s) match {
      case Success(id) => Value(id)
      case Failure(_) => DecodeResult.Error(s, new IllegalArgumentException(s"'$s' is not a valid QuineId"))
    }

  //TODO Use Tapir Validator IdProvider.validate

  val idProvider: QuineIdProvider = appMethods.graph.idProvider

  implicit val quineIdCodec: Codec[String, QuineId, TextPlain] =
    Codec.string.mapDecode(toQuineId)(idProvider.qidToPrettyString)

  // ------ timeout -------------

  implicit val timeoutCodec: Codec[String, FiniteDuration, TextPlain] =
    Codec.long.mapDecode(l => DecodeResult.Value(FiniteDuration(l, TimeUnit.MILLISECONDS)))(_.toMillis)

  val timeoutParameter: EndpointInput.Query[Option[FiniteDuration]] =
    query[Option[FiniteDuration]]("timeout").description(
      "Milliseconds to wait before the HTTP request times out",
    )

  type EndpointOutput[T] = Either[ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T]]

  /** Matching error types to api-rendered error outputs. Api Status codes are returned by matching the type of the
    * custom error to the status code matched here
    */
  protected val commonErrorOutput
    : EndpointOutput.OneOf[ErrorEnvelope[_ <: CustomError], ErrorEnvelope[_ <: CustomError]] =
    oneOf[ErrorEnvelope[_ <: CustomError]](
      oneOfVariantFromMatchType(
        statusCode(StatusCode.InternalServerError).and(jsonBody[ErrorEnvelope[ServerError]].description("bad request")),
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.BadRequest).and(jsonBody[ErrorEnvelope[BadRequest]].description("bad request")),
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.NotFound).and(jsonBody[ErrorEnvelope[NotFound]].description("not found")),
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.Unauthorized).and(jsonBody[ErrorEnvelope[Unauthorized]].description("unauthorized")),
      ),
      oneOfVariantFromMatchType(
        statusCode(StatusCode.ServiceUnavailable)
          .and(jsonBody[ErrorEnvelope[ServiceUnavailable]].description("service unavailable")),
      ),
      oneOfVariantFromMatchType(statusCode(StatusCode.NoContent).and(emptyOutputAs(ErrorEnvelope(NoContent)))),
      oneOfDefaultVariant(
        statusCode(StatusCode.InternalServerError).and(jsonBody[ErrorEnvelope[Unknown]].description("server error")),
      ),
    )

  protected def toObjectEnvelopeEncoder[T](encoder: Encoder[T]): Encoder[ObjectEnvelope[T]] = (a: ObjectEnvelope[T]) =>
    Json.fromFields(Seq(("data", encoder.apply(a.data))))

  protected def toObjectEnvelopeDecoder[T](decoder: Decoder[T]): Decoder[ObjectEnvelope[T]] =
    c => decoder.apply(c.downField("data").root).map(ObjectEnvelope(_))

  def yamlBody[T]()(implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): EndpointIO.Body[String, T] = stringBodyAnyFormat(YamlCodec.createCodec[T](), StandardCharsets.UTF_8)

  def jsonOrYamlBody[T](implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): EndpointIO.OneOfBody[T, T] =
    oneOfBody[T](jsonBody[T], yamlBody[T]())

  def textBody[T](codec: Codec[String, T, TextPlain]): EndpointIO.Body[String, T] =
    stringBodyAnyFormat(codec, Charset.defaultCharset())

  /** Wrap output types in their corresponding envelopes. */
  protected def wrapOutput[OUT](value: Either[CustomError, OUT]): EndpointOutput[OUT] =
    value.fold(t => Left(ErrorEnvelope(t)), v => Right(ObjectEnvelope(v)))
}

/** Component definitions for Tapir quine endpoints. */
trait V2QuineEndpointDefinitions extends V2EndpointDefinitions {

  val appMethods: QuineApiMethods

  /** OSS Specific behavior defined in [[com.thatdot.quine.v2api.V2OssRoutes]]. */
  def memberIdxParameter: EndpointInput[Option[Int]]

  /** OSS Specific behavior defined in [[V2OssRoutes]]. */
  def namespaceParameter: EndpointInput[Option[String]]

  //TODO port logic from QuineEndpoints NamespaceParameter
  def namespaceFromParam(ns: Option[String]): NamespaceId =
    ns.flatMap(t => Option.when(t != "default")(Symbol(t)))

  def ifNamespaceFound[A](namespaceId: NamespaceId)(
    ifFound: => Future[Either[CustomError, A]],
  ): Future[Either[CustomError, Option[A]]] =
    if (!appMethods.graph.getNamespaces.contains(namespaceId)) Future.successful(Right(None))
    else ifFound.map(_.map(Some(_)))(ExecutionContext.parasitic)

  type EndpointBase = Endpoint[Unit, Option[Int], Unit, Unit, Any]

  /** Base for api/v2 endpoints with common errors that expects the universal parameter memberIdx.
    *
    * We sometimes require access to an endpoint definition with no output type specified, because Tapir will not allow
    * a 204 output response with defined outputs.
    *
    * @param basePaths Provided base Paths will be appended in order, i.e `endpoint("a","b") == /api/v2/a/b`
    */
  def rawEndpoint(
    basePaths: String*,
  ): Endpoint[Unit, Option[Int], Unit, Unit, Any] =
    endpoint
      .in(basePaths.foldLeft("api" / "v2")((path, segment) => path / segment))
      .in(memberIdxParameter)

  def withOutput[T](endpoint: EndpointBase)(implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): Endpoint[Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T], Any] =
    endpoint
      .errorOut(commonErrorOutput)
      .out(jsonBody[ObjectEnvelope[T]](toObjectEnvelopeEncoder(encoder), toObjectEnvelopeDecoder(decoder), schema))

  /** Base endpoint type with output type defined within a common [[ObjectEnvelope]] and common error output.
    * @param basePaths Provided base Paths will be appended in order, i.e `endpoint("a","b") == /api/v2/a/b`
    */
  def baseEndpoint[T](
    basePaths: String*,
  )(implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): Endpoint[Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T], Any] =
    withOutput[T](rawEndpoint(basePaths: _*))

  //TODO split into definitions in extensions of [[TapirRoutes]] that support specific platforms.
  /** Wrap server responses in their respective output envelopes. */
  def runServerLogic[IN, OUT: Decoder](
    cmd: ApiCommand,
    idx: Option[Int],
    in: IN,
    f: IN => Future[OUT],
  ): Future[EndpointOutput[OUT]] = {
    implicit val ctx = ExecutionContext.parasitic
    val g: Future[OUT] => Future[Either[CustomError, OUT]] = gin =>
      gin.map(Right(_)).recover(t => Left(toCustomError(t)))
    runServerLogicWithError(cmd, idx, in, f.andThen(g))

  }

  /** Run server logic, defining some future values as custom error returns. */
  def runServerLogicWithError[IN, OUT: Decoder](
    cmd: ApiCommand,
    idx: Option[Int],
    in: IN,
    f: IN => Future[Either[CustomError, OUT]],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], ObjectEnvelope[OUT]]] = {
    logger.debug(log"Received arguments for API call ${Safe(cmd.toString)}: ${in.toString}")
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    f(in).map(wrapOutput).recover(t => Left(ErrorEnvelope(toCustomError(t))))
  }

}

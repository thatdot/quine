package com.thatdot.quine.app.v2api.definitions

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.DecodeResult.Value
import sttp.tapir.generic.auto._
import sttp.tapir.{EndpointOutput, endpoint, _}

import com.thatdot.common.logging.Log._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.definitions.CustomError.toCustomError
import com.thatdot.quine.app.v2api.definitions.ErrorText.serverErrorDescription
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.routes.IngestRoutes

/** Error Response Envelope */
case class ErrorEnvelope[+T](error: T)

/** Component definitions for Tapir endpoints. */
trait V2EndpointDefinitions extends V2IngestApiSchemas with LazySafeLogging {

  implicit protected def logConfig: LogConfig

  val appMethods: ApplicationApiMethods
  // ------- parallelism -----------
  val parallelismParameter: EndpointInput.Query[Int] = query[Int](name = "parallelism")
    .description(s"Operations to execute simultaneously")
    .default(IngestRoutes.defaultWriteParallelism)
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
    query[Option[AtTime]]("atTime")
      .description(
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

  type EndpointOutput[T] = Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope[T]]

  /** Matching error types to api-rendered error outputs. Api Status codes are returned by matching the type of the
    * custom error to the status code matched here
    * Defines the default case that maps an error not specified in the endpoints definition to an
    * internal server error.
    */
  protected val commonErrorOutput
    : EndpointOutput.OneOf[ErrorEnvelope[_ <: CustomError], ErrorEnvelope[_ <: CustomError]] =
    oneOf[ErrorEnvelope[_ <: CustomError]](
      oneOfDefaultVariant(
        statusCode(StatusCode.InternalServerError).and(
          jsonBody[ErrorEnvelope[CustomError]].description(serverErrorDescription()),
        ),
      ),
    )

  type EndpointBase = Endpoint[Unit, Unit, ErrorEnvelope[_ <: CustomError], Unit, Any]

  /** Base for api/v2 endpoints with common errors
    *
    * @param basePaths Provided base Paths will be appended in order, i.e `endpoint("a","b") == /api/v2/a/b`
    */
  def rawEndpoint(
    basePaths: String*,
  ): Endpoint[Unit, Unit, ErrorEnvelope[_ <: CustomError], Unit, Any] =
    endpoint
      .in(basePaths.foldLeft("api" / "v2")((path, segment) => path / segment))
      .errorOut(commonErrorOutput)

  def yamlBody[T]()(implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): EndpointIO.Body[String, T] = stringBodyAnyFormat(YamlCodec.createCodec[T](), StandardCharsets.UTF_8)

  def jsonOrYamlBody[T](tOpt: Option[T] = None)(implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): EndpointIO.OneOfBody[T, T] = tOpt match {
    case None => oneOfBody[T](jsonBody[T], yamlBody[T]())
    case Some(t) =>
      oneOfBody[T](jsonBody[T].example(t), yamlBody[T]().example(t))
  }

  def textBody[T](codec: Codec[String, T, TextPlain]): EndpointIO.Body[String, T] =
    stringBodyAnyFormat(codec, Charset.defaultCharset())

  def runServerLogicOk[IN, OUT: Decoder](f: Future[IN])(
    outToResponse: IN => SuccessEnvelope.Ok[OUT],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Ok[OUT]]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    runServerLogicFromEitherOk(f.map(Right(_)))(outToResponse)
  }

  def runServerLogicCreated[IN, OUT: Decoder](f: Future[IN])(
    outToResponse: IN => SuccessEnvelope.Created[OUT],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Created[OUT]]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    runServerLogicFromEitherCreated(f.map(Right(_)))(outToResponse)
  }

  def runServerLogicAccepted[IN: Decoder](f: Future[IN])(
    outToResponse: IN => SuccessEnvelope.Accepted,
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Accepted]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    runServerLogicFromEitherAccepted(f.map(Right(_)))(outToResponse)
  }

  def runServerLogicNoContent[IN: Decoder](
    f: Future[IN],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.NoContent.type]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    runServerLogicFromEitherNoContent(f.map(Right(_)))
  }

  def runServerLogicFromEitherOk[IN, OUT: Decoder](
    f: Future[Either[ErrorEnvelope[_ <: CustomError], IN]],
  )(
    outToResponse: IN => SuccessEnvelope.Ok[OUT],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Ok[OUT]]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    f.map(_.map(outToResponse)).recover(t => Left(ErrorEnvelope(toCustomError(t))))
  }

  def runServerLogicFromEitherCreated[IN, OUT: Decoder](
    f: Future[Either[ErrorEnvelope[_ <: CustomError], IN]],
  )(
    outToResponse: IN => SuccessEnvelope.Created[OUT],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Created[OUT]]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    f.map(_.map(outToResponse)).recover(t => Left(ErrorEnvelope(toCustomError(t))))
  }

  def runServerLogicFromEitherAccepted[IN](f: Future[Either[ErrorEnvelope[_ <: CustomError], IN]])(
    outToResponse: IN => SuccessEnvelope.Accepted,
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Accepted]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    f.map(_.map(outToResponse)).recover(t => Left(ErrorEnvelope(toCustomError(t))))
  }

  def runServerLogicFromEitherNoContent[T](
    f: Future[Either[ErrorEnvelope[_ <: CustomError], T]],
  ): Future[Either[ErrorEnvelope[_ <: CustomError], SuccessEnvelope.NoContent.type]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    f.map(_.map(_ => SuccessEnvelope.NoContent)).recover(t => Left(ErrorEnvelope(toCustomError(t))))
  }

}

/** Component definitions for Tapir quine endpoints. */
trait V2QuineEndpointDefinitions extends V2EndpointDefinitions {

  val appMethods: QuineApiMethods

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

}

package com.thatdot.quine.app.v2api.definitions

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import io.circe.{Decoder, Encoder}
import shapeless.ops.coproduct.{Basis, CoproductToEither}
import shapeless.{:+:, CNil, Coproduct}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.DecodeResult.Value
import sttp.tapir._

import com.thatdot.common.logging.Log._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.definitions.ErrorResponse.ServerError
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.toServerError
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.routes.IngestRoutes

/** Component definitions for Tapir endpoints. */
trait V2EndpointDefinitions extends V2IngestApiSchemas with LazySafeLogging {

  implicit protected def logConfig: LogConfig

  val appMethods: ApplicationApiMethods
  // ------- parallelism -----------
  val parallelismParameter: EndpointInput.Query[Int] = query[Int](name = "parallelism")
    .description(s"Number of operations to execute simultaneously.")
    .default(IngestRoutes.defaultWriteParallelism)
  // ------- atTime ----------------

  type AtTime = Milliseconds

  /** Since timestamps get encoded as milliseconds since 1970 in the REST API,
    * it is necessary to define the serialization/deserialization to/from a long.
    */
  protected def toAtTime(rawTime: Long): DecodeResult[AtTime] = {
    val now = System.currentTimeMillis
    if (rawTime > now)
      DecodeResult.Error(rawTime.toString, new IllegalArgumentException(s"Times in the future are not supported."))
    else Value(Milliseconds(rawTime))
  }

  /** Schema for an at time */
  implicit val atTimeEndpointCodec: Codec[String, AtTime, TextPlain] = Codec.long.mapDecode(toAtTime)(_.millis)

  val atTimeParameter: EndpointInput.Query[Option[AtTime]] =
    query[Option[AtTime]]("atTime")
      .description(
        "An integer timestamp in milliseconds since the Unix epoch representing the historical moment to query.",
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

  val timeoutParameter: EndpointInput.Query[FiniteDuration] =
    query[FiniteDuration]("timeout")
      .description("Milliseconds to wait before the HTTP request times out.")
      .default(FiniteDuration.apply(20, TimeUnit.SECONDS))

  type EndpointBase = Endpoint[Unit, Unit, ServerError, Unit, Any]

  /** Base for api/v2 endpoints with common errors
    *
    * @param basePaths Provided base Paths will be appended in order, i.e. `endpoint("a","b") == /api/v2/a/b`
    */
  def rawEndpoint(
    basePaths: String*,
  ): Endpoint[Unit, Unit, Nothing, Unit, Any] =
    infallibleEndpoint
      .in(basePaths.foldLeft("api" / "v2")((path, segment) => path / segment))

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

  /** Used to produce an endpoint that only has ServerErrors that are caught here.
    *
    * - Wraps server logic in tapir endpoints for catching any exception and lifting to ServerError(500 code).
    */
  def recoverServerError[In, Out](
    fa: Future[In],
  )(outToResponse: In => Out): Future[Either[ServerError, Out]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    fa.map(out => Right(outToResponse(out))).recover(t => Left(toServerError(t)))
  }

  /** Recover from errors that could cause the provided future to fail. Errors are represented as any shape Coproduct
    *
    * - Wraps server logic in tapir endpoints for catching any exception and lifting to ServerError(500 code).
    * - Used when the input error type, `Err`, is itself a Coproduct that does not contain ServerError.
    * - The Left of the output Either will itself be a nested either with all coproduct elements accounted for.
    *    This is used for tapir endpoint definition as the errorOut type
    * - When the Coproduct has size greater than 2 the tapir Either and CoproductToEither is swapped.
    *    to fix this map the errorOut to be swapped for the endpoint: `_.mapErrorOut(err => err.swap)(err => err.swap)`
    */
  def recoverServerErrorEither[In, Out, Err <: Coproduct](
    fa: Future[Either[Err, In]],
  )(outToResponse: In => Out)(implicit
    basis: Basis[ServerError :+: Err, Err],
    c2e: CoproductToEither[ServerError :+: Err],
  ): Future[Either[c2e.Out, Out]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    fa.map {
      case Left(err) => Left(c2e(err.embed[ServerError :+: Err]))
      case Right(value) => Right(outToResponse(value))
    }.recover(t => Left(c2e(Coproduct[ServerError :+: Err](toServerError(t)))))
  }

  /** Recover from errors that could cause the provided future to fail. Errors are represented as a Coproduct
    * with ServerError explicitly the head of the Coproduct `Err` in the provided Future.
    *
    * - Wraps server logic in tapir endpoints for catching any exception and lifting to ServerError(500 code).
    * - Used when the input error type, `Err`, is itself a Coproduct that does contain ServerError
    * - The Left of the output Either will itself be a nested either with all coproduct elements accounted for.
    *    This is used for tapir endpoint definition as the errorOut type
    * - When the Coproduct has size greater than 2 the tapir Either and CoproductToEither is swapped.
    *    to fix this map the errorOut to be swapped for the endpoint: `_.mapErrorOut(err => err.swap)(err => err.swap)`
    */
  def recoverServerErrorEitherWithServerError[In, Out, Err <: Coproduct](
    fa: Future[Either[ServerError :+: Err, In]],
  )(outToResponse: In => Out)(implicit
    basis: Basis[ServerError :+: Err, ServerError :+: Err],
    c2e: CoproductToEither[ServerError :+: Err],
  ): Future[Either[c2e.Out, Out]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    fa.map {
      case Left(err) => Left(c2e(err.embed[ServerError :+: Err]))
      case Right(value) => Right(outToResponse(value))
    }.recover(t => Left(c2e(Coproduct[ServerError :+: Err](toServerError(t)))))
  }

  /** Recover from errors that could cause the provided future to fail. Errors should likely not be represented
    * as a Coproduct in the input provided Future
    *
    * - Wraps server logic in tapir endpoints for catching any exception and lifting to ServerError(500 code).
    * - Used when the input error type, `Err`, is not a Coproduct itself.
    * - The Left of the output Either will itself be an Either[ServerError, Err] with all coproduct elements accounted for.
    *    This is used for tapir endpoint definition as the errorOut type
    */
  def recoverServerErrorEitherFlat[In, Out, Err](
    fa: Future[Either[Err, In]],
  )(outToResponse: In => Out)(implicit
    c2e: CoproductToEither[ServerError :+: Err :+: CNil],
  ): Future[Either[c2e.Out, Out]] = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    fa.map {
      case Left(err) => Left(c2e(Coproduct[ServerError :+: Err :+: CNil](err)))
      case Right(value) => Right(outToResponse(value))
    }.recover(t => Left(c2e(Coproduct[ServerError :+: Err :+: CNil](toServerError(t)))))
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
    ifFound: => Future[Either[ServerError, A]],
  ): Future[Either[ServerError, Option[A]]] =
    if (!appMethods.graph.getNamespaces.contains(namespaceId)) Future.successful(Right(None))
    else ifFound.map(_.map(Some(_)))(ExecutionContext.parasitic)

}

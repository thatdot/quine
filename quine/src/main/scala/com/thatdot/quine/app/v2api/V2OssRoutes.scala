package com.thatdot.quine.v2api

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import io.circe.{Decoder, Encoder}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{EndpointInput, query}

import com.thatdot.quine.app.v2api.definitions.CustomError.toCustomError
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.definitions.endpoints.V2AdministrationEndpoints

/** Gathering of Quine OSS tapir-defined routes.
  */
class V2OssRoutes(val app: OssApiInterface) extends TapirRoutes with V2AdministrationEndpoints {

  override val apiEndpoints: List[ServerEndpoint[Any, Future]] = List(propertiesEndpoint)

  /** Wrap server responses in their respective output envelopes. */
  def runServerLogic[IN: Encoder, OUT: Decoder](
    cmd: ApiCommand,
    idx: Option[Int],
    in: IN,
    f: IN => Future[OUT]
  ): Future[Either[ErrorEnvelope[_ <: CustomError], ObjectEnvelope[OUT]]] = {
    val t: Future[Either[CustomError, OUT]] =
      Try(f(in)).toEither
        .fold(
          (e: Throwable) => Future.successful(Left(toCustomError(e))),
          (out: Future[OUT]) => out.map(Right(_))(ExecutionContext.parasitic)
        )
    // Wrap output types in their corresponding envelopes.
    t.map {
      case Left(err) => Left(ErrorEnvelope(err))
      case Right(out) => Right(ObjectEnvelope(out))
    }(ExecutionContext.parasitic)
  }

  //Hidden in OSS api
  def memberIdxParameter: EndpointInput[Option[Int]] =
    query[Option[Int]]("memberIdx").schema(_.hidden(true))
  //Hidden in OSS api
  def namespaceParameter: EndpointInput[Option[String]] =
    query[Option[String]]("namespace").schema(_.hidden(true))
}

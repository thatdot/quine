package com.thatdot.quine.app.v2api.definitions.endpoints

import scala.concurrent.Future

import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.server.ServerEndpoint

import com.thatdot.quine.app.v2api.definitions.{
  AdminGetProperties,
  CustomError,
  ErrorEnvelope,
  ObjectEnvelope,
  V2EndpointDefinitions
}

/** Placeholder routes for ApiV2. This is only intended as a demonstration route and does not intend to represent a final endpoint for our V2 api. */
trait V2AdministrationEndpoints extends V2EndpointDefinitions {

  private def adminEndpoint[T](implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T]
  ) = baseEndpoint[T]("admin")

  //   TODO NOT A FINAL ENDPOINT
  val propertiesEndpoint: ServerEndpoint.Full[Unit, Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[
    Map[String, String]
  ], Any, Future] =
    adminEndpoint[Map[String, String]].get
      .serverLogic(memberIdx =>
        runServerLogic[Unit, Map[String, String]](AdminGetProperties, memberIdx, (), _ => app.getProperties)
      )

}

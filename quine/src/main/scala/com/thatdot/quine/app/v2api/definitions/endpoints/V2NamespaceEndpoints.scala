package com.thatdot.quine.app.v2api.definitions.endpoints

import scala.concurrent.Future

import io.circe.{Decoder, Encoder}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Schema, path}

import com.thatdot.quine.app.v2api.definitions._

/** Placeholder route to demonstrate V2. Not intended to represent a final endpoint. */
trait V2NamespaceEndpoints extends V2EndpointDefinitions {

  private def namespaceEndpoint[T](implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T]
  ) = baseEndpoint[T]("namespace")

  val getNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, Option[Int], ErrorEnvelope[
    _ <: CustomError
  ], ObjectEnvelope[List[String]], Any, Future] =
    namespaceEndpoint[List[String]].get
      .serverLogic(memberIdx =>
        runServerLogic[Unit, List[String]](GetNamespaces, memberIdx, (), _ => app.getNamespaces)
      )

  val createNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, (Option[Int], String), ErrorEnvelope[
    _ <: CustomError
  ], ObjectEnvelope[Boolean], Any, Future] =
    namespaceEndpoint[Boolean]
      .in(path[String]("namespace"))
      .put
      .serverLogic { case (memberIdx, namespace) =>
        runServerLogic[String, Boolean](CreateNamespace, memberIdx, namespace, app.createNamespace)
      }

  val deleteNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, (Option[Int], String), ErrorEnvelope[
    _ <: CustomError
  ], ObjectEnvelope[Boolean], Any, Future] =
    namespaceEndpoint[Boolean]
      .in(path[String]("namespace"))
      .delete
      .serverLogic { case (memberIdx, namespace) =>
        runServerLogic(DeleteNamespace, memberIdx, namespace, app.deleteNamespace)
      }
}

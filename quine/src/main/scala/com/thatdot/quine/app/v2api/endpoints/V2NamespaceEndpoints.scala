package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.{Decoder, Encoder}
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Schema, path}

import com.thatdot.quine.app.v2api.definitions._

/** Placeholder route to demonstrate V2. Not intended to represent a final endpoint. */
trait V2NamespaceEndpoints extends V2QuineEndpointDefinitions {

  private def namespaceEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ) = baseEndpoint[T]("namespaces").tag("Namespaces")

  val getNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, Option[Int], ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[List[String]], Any, Future] =
    namespaceEndpoint[List[String]].get
      .name("List Namespaces")
      .description("Retrieve the list of all existing namespaces")
      .serverLogic(memberIdx =>
        runServerLogic[Unit, List[String]](GetNamespaces, memberIdx, (), _ => appMethods.getNamespaces),
      )

  val createNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, (Option[Int], String), ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[Boolean], Any, Future] =
    namespaceEndpoint[Boolean]
      .name("Create Namespace")
      .description("Create the requested namespace")
      .in(path[String]("namespace"))
      .put
      .serverLogic { case (memberIdx, namespace) =>
        runServerLogic[String, Boolean](CreateNamespace, memberIdx, namespace, appMethods.createNamespace)
      }

  val deleteNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, (Option[Int], String), ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[Boolean], Any, Future] =
    namespaceEndpoint[Boolean]
      .name("Delete Namespace")
      .description("Delete the requested namespace")
      .in(path[String]("namespace"))
      .delete
      .serverLogic { case (memberIdx, namespace) =>
        runServerLogic(DeleteNamespace, memberIdx, namespace, appMethods.deleteNamespace)
      }
}

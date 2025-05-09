package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{path, statusCode}

import com.thatdot.quine.app.v2api.definitions._

/** Placeholder route to demonstrate V2. Not intended to represent a final endpoint. */
trait V2NamespaceEndpoints extends V2QuineEndpointDefinitions with V2ApiConfiguration {

  private def namespaceEndpoint = rawEndpoint("namespaces").tag("Namespaces")

  val getNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, Unit, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Ok[List[String]], Any, Future] =
    namespaceEndpoint.get
      .name("List Namespaces")
      .description("Retrieve the list of all existing namespaces")
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[List[String]]])
      .serverLogic(_ => runServerLogicOk(appMethods.getNamespaces)((inp: List[String]) => SuccessEnvelope.Ok(inp)))

  val createNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, String, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Created[String], Any, Future] =
    namespaceEndpoint
      .name("Create Namespace")
      .description("Create the requested namespace")
      .in(path[String]("namespace"))
      .put
      .out(statusCode(StatusCode.Created))
      .out(jsonBody[SuccessEnvelope.Created[String]])
      .serverLogic { namespace =>
        runServerLogicCreated(appMethods.createNamespace(namespace))((inp: Boolean) =>
          SuccessEnvelope.Created(namespace),
        )
      }

  val deleteNamespaceEndpoint: ServerEndpoint.Full[Unit, Unit, String, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Ok[String], Any, Future] =
    namespaceEndpoint
      .name("Delete Namespace")
      .description("Delete the requested namespace")
      .in(path[String]("namespace"))
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[String]])
      .serverLogic { namespace =>
        runServerLogicOk(appMethods.deleteNamespace(namespace))((_: Boolean) => SuccessEnvelope.Ok(namespace))
      }
}

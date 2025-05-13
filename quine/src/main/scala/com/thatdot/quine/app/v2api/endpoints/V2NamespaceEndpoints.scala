package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{emptyOutputAs, oneOf, oneOfVariantFromMatchType, path, statusCode}

import com.thatdot.quine.app.v2api.definitions.ErrorResponse.ServerError
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.serverError
import com.thatdot.quine.app.v2api.definitions._

/** Placeholder route to demonstrate V2. Not intended to represent a final endpoint. */
trait V2NamespaceEndpoints extends V2QuineEndpointDefinitions with V2ApiConfiguration {

  private def namespaceEndpoint = rawEndpoint("namespaces")
    .tag("Namespaces")
    .errorOut(serverError())

  val getNamespaceEndpoint
    : ServerEndpoint.Full[Unit, Unit, Unit, ServerError, SuccessEnvelope.Ok[List[String]], Any, Future] =
    namespaceEndpoint.get
      .name("List Namespaces")
      .description("Retrieve the list of all existing namespaces")
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[List[String]]])
      .serverLogic(_ => recoverServerError(appMethods.getNamespaces)((inp: List[String]) => SuccessEnvelope.Ok(inp)))

  val createNamespaceEndpoint
    : ServerEndpoint.Full[Unit, Unit, String, ServerError, CreatedOrNoContent[String], Any, Future] =
    namespaceEndpoint
      .name("Create Namespace")
      .description("Create the requested namespace")
      .in(path[String]("namespace"))
      .put
      .out(
        oneOf[CreatedOrNoContent[String]](
          oneOfVariantFromMatchType(
            statusCode(StatusCode.Created).and(
              jsonBody[SuccessEnvelope.Created[String]].description("Namespace Created"),
            ),
          ),
          oneOfVariantFromMatchType(
            statusCode(StatusCode.NoContent).and(
              emptyOutputAs(SuccessEnvelope.NoContent)
                .description("Namespace already exists. No change."),
            ),
          ),
        ),
      )
      .serverLogic[Future] { namespace =>
        recoverServerError(appMethods.createNamespace(namespace)) {
          case true => SuccessEnvelope.Created(namespace)
          case false => SuccessEnvelope.NoContent
        }
      }

  val deleteNamespaceEndpoint
    : ServerEndpoint.Full[Unit, Unit, String, ServerError, SuccessEnvelope.Ok[String], Any, Future] =
    namespaceEndpoint
      .name("Delete Namespace")
      .description("Delete the requested namespace")
      .in(path[String]("namespace"))
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[String]])
      .serverLogic[Future] { namespace =>
        recoverServerError(appMethods.deleteNamespace(namespace))((_: Boolean) => SuccessEnvelope.Ok(namespace))
      }
  protected def namespaceEndpoints: List[ServerEndpoint[Any, Future]] =
    List(
      getNamespaceEndpoint,
      createNamespaceEndpoint,
      deleteNamespaceEndpoint,
    )

}

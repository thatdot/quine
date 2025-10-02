package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.api.v2.ErrorResponse.ServerError
import com.thatdot.api.v2.V2EndpointDefinitions
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineIdProvider

/** Component definitions for Tapir quine endpoints. */
trait V2QuineEndpointDefinitions
    extends V2EndpointDefinitions
    with V2IngestApiSchemas
    with CommonParameters
    with ParallelismParameter {

  val appMethods: QuineApiMethods

  val idProvider: QuineIdProvider = appMethods.graph.idProvider

  def ifNamespaceFound[A](namespaceId: NamespaceId)(
    ifFound: => Future[Either[ServerError, A]],
  ): Future[Either[ServerError, Option[A]]] =
    if (!appMethods.graph.getNamespaces.contains(namespaceId)) Future.successful(Right(None))
    else ifFound.map(_.map(Some(_)))(ExecutionContext.parasitic)
}

package com.thatdot.quine.app.v2api.definitions

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.DecodeResult.Value
import sttp.tapir._

import com.thatdot.api.v2.ErrorResponse.ServerError
import com.thatdot.api.v2.V2EndpointDefinitions
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.routes.IngestRoutes

/** Component definitions for Tapir quine endpoints. */
trait V2QuineEndpointDefinitions extends V2EndpointDefinitions with V2IngestApiSchemas {

  val appMethods: QuineApiMethods

  // ------- parallelism -----------
  val parallelismParameter: EndpointInput.Query[Int] = query[Int](name = "parallelism")
    .description(s"Number of operations to execute simultaneously.")
    .default(IngestRoutes.defaultWriteParallelism)

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

  /** OSS Specific behavior defined in [[com.thatdot.quine.app.v2api.V2OssRoutes]]. */
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

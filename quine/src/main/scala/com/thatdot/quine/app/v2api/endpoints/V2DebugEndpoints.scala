package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.Json
import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Codec, DecodeResult, Endpoint, EndpointInput, Schema, path, query, statusCode}

import com.thatdot.api.v2.ErrorResponse.ServerError
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.{SuccessEnvelope, V2EndpointDefinitions}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}

object V2DebugEndpointEntities {
  import com.thatdot.quine.app.util.StringOps.syntax._

  private val jsonSchema: Schema[Json] = Schema.any[Json]
  private val mapStringJsonSchema: Schema[Map[String, Json]] = Schema.schemaForMap[Json](jsonSchema)

  sealed abstract class TEdgeDirection
  object TEdgeDirection {
    case object Outgoing extends TEdgeDirection
    case object Incoming extends TEdgeDirection
    case object Undirected extends TEdgeDirection

    val values: Seq[TEdgeDirection] = Seq(Outgoing, Incoming, Undirected)

    implicit val schema: Schema[TEdgeDirection] = Schema.derivedEnumeration[TEdgeDirection].defaultStringBased
  }

  @title("Half Edge")
  @description(
    """One "half" of an edge. A full logical graph edge exists in a Quine graph if and only if
      |the two nodes at the edge's endpoints contain half edges that:""".asOneLine +
    """
      |  * Point to each other
      |  * Have the same label
      |  * Have opposite directions """.stripMargin + """(e.g. one side is incoming and the other is outgoing,
      |    or else both sides are undirected)""".asOneLine,
  )
  final case class TRestHalfEdge[ID](
    @description("Label of the edge.") edgeType: String,
    direction: TEdgeDirection,
    @description("Id of node at the other end of the edge.") other: ID,
  )
  object TRestHalfEdge {
    implicit def schema[ID](implicit idSchema: Schema[ID]): Schema[TRestHalfEdge[ID]] =
      Schema.derived[TRestHalfEdge[ID]]
  }

  @title("Node Data")
  @description("Data locally available on a node in the graph.")
  final case class TLiteralNode[ID](
    @description(
      """Properties on the node; note that values are represented as closely as possible
        |to how they would be emitted by
        |[the cypher query endpoint](https://quine.io/reference/rest-api/#/paths/api-v1-query-cypher/post).""".asOneLine,
    )
    properties: Map[String, Json],
    edges: Seq[TRestHalfEdge[ID]],
  )
  object TLiteralNode {
    implicit def schema[ID](implicit idSchema: Schema[ID]): Schema[TLiteralNode[ID]] = {
      implicit val mapSchema: Schema[Map[String, Json]] = mapStringJsonSchema
      implicit val halfEdgeSchema: Schema[TRestHalfEdge[ID]] = TRestHalfEdge.schema[ID]
      Schema.derived[TLiteralNode[ID]]
    }
  }
}

trait V2DebugEndpoints extends V2EndpointDefinitions with V2IngestApiSchemas with CommonParameters with StringOps {
  val appMethods: ApplicationApiMethods with DebugApiMethods

  implicit lazy val quineIdSchema: Schema[QuineId] = Schema.string[QuineId]

  implicit lazy val tEdgeDirectionSchema: Schema[TEdgeDirection] = TEdgeDirection.schema
  implicit def tRestHalfEdgeSchema[ID: Schema]: Schema[TRestHalfEdge[ID]] = TRestHalfEdge.schema[ID]
  implicit def tLiteralNodeSchema[ID: Schema]: Schema[TLiteralNode[ID]] = TLiteralNode.schema[ID]

  implicit val tEdgeDirectionCodec: Codec[String, TEdgeDirection, TextPlain] = {

    def fromString(s: String): DecodeResult[TEdgeDirection] = s.toLowerCase match {
      case "outgoing" => DecodeResult.Value(TEdgeDirection.Outgoing)
      case "incoming" => DecodeResult.Value(TEdgeDirection.Incoming)
      case "undirected" => DecodeResult.Value(TEdgeDirection.Undirected)
      case other => DecodeResult.Error(other, new IllegalArgumentException(s"'$other' is not a valid EdgeDirection"))
    }

    Codec.string.mapDecode(fromString)(_.toString)
  }

  private val idPathElement: EndpointInput.PathCapture[QuineId] = path[QuineId]("id").description("Node ID.")

  private val propKeyParameter: EndpointInput.Query[String] =
    query[String]("key").description("Name of a property")

  private val edgeTypeOptParameter: EndpointInput.Query[Option[String]] =
    query[Option[String]]("type").description("Edge type")

  private val otherOptParameter: EndpointInput.Query[Option[QuineId]] =
    query[Option[QuineId]]("other").description("Other edge endpoint")

  private val limitParameter: EndpointInput.Query[Option[Int]] =
    query[Option[Int]]("limit").description("Maximum number of results to return.")

  private val fullEdgeParameter: EndpointInput.Query[Option[Boolean]] =
    query[Option[Boolean]]("onlyFull").description("Only return full edges.")

  private val edgeDirOptParameter: EndpointInput.Query[Option[TEdgeDirection]] =
    query[Option[TEdgeDirection]]("direction").description("Edge direction. One of: Incoming, Outgoing, Undirected.")

  /*
    final val edgeType: QueryString[String] = qs[String]("type", docs = Some("Edge type"))
    final val propKey: QueryString[String] = qs[String]("key", docs = Some("Name of a property"))
    final val other: QueryString[Id] = qs[Id]("other", docs = Some("Other edge endpoint"))
   // final val otherOpt: QueryString[Option[Id]] = qs[Option[Id]]("other", docs = Some("Other edge endpoint"))
   */

  /** Generate an endpoint at `/api/v2/debug/nodes` */
  private def debugBase: EndpointBase = rawEndpoint("debug", "nodes")
    .tag("Debug Node Operations")
    .errorOut(serverError())

  private val debugEndpointIntentionAddendum: String = "\n\n" +
    """This endpoint's usage, including the structure of the values returned, are implementation-specific and
      |subject to change without warning. This endpoint is not intended for consumption by automated clients.
      |The information returned by this endpoint is formatted for human consumption and is intended to assist the
      |operator(s) of Quine in inspecting specific parts of the internal Quine graph state.""".asOneLine

  protected[endpoints] val debugOpsPropertyGet: Endpoint[
    Unit,
    (QuineId, String, Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[Option[Json]],
    Any,
  ] = debugBase
    .name("Get Property")
    .description(
      """Retrieve a single property from the node; note that values are represented as closely as possible to how they
          |would be emitted by
          |[the cypher query endpoint](https://quine.io/reference/rest-api/#/paths/api-v1-query-cypher/post).""".asOneLine +
      debugEndpointIntentionAddendum,
    )
    .attribute(Visibility.attributeKey, Visibility.Hidden)
    .in(idPathElement)
    .in("props")
    .in(propKeyParameter)
    .in(atTimeParameter)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Option[Json]]])

  protected[endpoints] val debugOpsPropertyGetLogic: ((QuineId, String, Option[AtTime], Option[String])) => Future[
    Either[ServerError, SuccessEnvelope.Ok[Option[Json]]],
  ] = { case (id, propKey, atime, ns) =>
    recoverServerError(appMethods.debugOpsPropertyGet(id, propKey, atime, namespaceFromParam(ns)))(
      (inp: Option[Json]) => SuccessEnvelope.Ok.apply(inp),
    )
  }

  private val debugOpsPropertyGetServerEndpoint: Full[
    Unit,
    Unit,
    (QuineId, String, Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[Option[Json]],
    Any,
    Future,
  ] = debugOpsPropertyGet.serverLogic[Future](debugOpsPropertyGetLogic)

  protected[endpoints] val debugOpsGet: Endpoint[
    Unit,
    (QuineId, Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[TLiteralNode[QuineId]],
    Any,
  ] = debugBase
    .name("List Properties/Edges")
    .description(s"Retrieve a node's list of properties and list of edges." + debugEndpointIntentionAddendum)
    .attribute(Visibility.attributeKey, Visibility.Hidden)
    .in(idPathElement)
    .in(atTimeParameter)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[TLiteralNode[QuineId]]])

  protected[endpoints] val debugOpsGetLogic: ((QuineId, Option[AtTime], Option[String])) => Future[
    Either[ServerError, SuccessEnvelope.Ok[TLiteralNode[QuineId]]],
  ] = { case (id, atime, ns) =>
    recoverServerError(appMethods.debugOpsGet(id, atime, namespaceFromParam(ns)))(
      SuccessEnvelope.Ok.apply(_: TLiteralNode[QuineId]),
    )
  }

  private val debugOpsGetServerEndpoint: Full[
    Unit,
    Unit,
    (QuineId, Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[TLiteralNode[QuineId]],
    Any,
    Future,
  ] = debugOpsGet.serverLogic[Future](debugOpsGetLogic)

  //TODO temporarily outputs string
  protected[endpoints] val debugOpsVerbose: Endpoint[
    Unit,
    (QuineId, Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[String],
    Any,
  ] = debugBase
    .name("List Node State (Verbose)")
    .description(s"Returns information relating to the node's internal state." + debugEndpointIntentionAddendum)
    .attribute(Visibility.attributeKey, Visibility.Hidden)
    .in(idPathElement)
    .in("verbose")
    .in(atTimeParameter)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[String]])

  protected[endpoints] val debugOpsVerboseLogic
    : ((QuineId, Option[AtTime], Option[String])) => Future[Either[ServerError, SuccessEnvelope.Ok[String]]] = {
    case (id, atime, ns) =>
      recoverServerError(appMethods.debugOpsVerbose(id, atime, namespaceFromParam(ns)))(SuccessEnvelope.Ok(_))
  }

  private val debugOpsVerboseServerEndpoint: Full[
    Unit,
    Unit,
    (QuineId, Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[String],
    Any,
    Future,
  ] = debugOpsVerbose.serverLogic[Future](debugOpsVerboseLogic)

  protected[endpoints] val debugOpsEdgesGet: Endpoint[
    Unit,
    (
      QuineId,
      Option[AtTime],
      Option[Int],
      Option[TEdgeDirection],
      Option[QuineId],
      Option[String],
      Option[Boolean],
      Option[String],
    ),
    ServerError,
    SuccessEnvelope.Ok[Vector[TRestHalfEdge[QuineId]]],
    Any,
  ] =
    debugBase
      .name("List Edges")
      .description(s"Retrieve all node edges." + debugEndpointIntentionAddendum)
      .attribute(Visibility.attributeKey, Visibility.Hidden)
      .in(idPathElement)
      .in("edges")
      .in(atTimeParameter)
      .in(limitParameter)
      .in(edgeDirOptParameter)
      .in(otherOptParameter)
      .in(edgeTypeOptParameter)
      .in(fullEdgeParameter)
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Vector[TRestHalfEdge[QuineId]]]])

  protected[endpoints] val debugOpsEdgesGetLogic: (
    (
      QuineId,
      Option[AtTime],
      Option[Int],
      Option[TEdgeDirection],
      Option[QuineId],
      Option[String],
      Option[Boolean],
      Option[String],
    ),
  ) => Future[Either[ServerError, SuccessEnvelope.Ok[Vector[TRestHalfEdge[QuineId]]]]] = {
    case (id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, fullOnly, ns) =>
      recoverServerError(
        if (fullOnly.getOrElse(true))
          appMethods.debugOpsEdgesGet(id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceFromParam(ns))
        else
          appMethods.debugOpsHalfEdgesGet(id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceFromParam(ns)),
      )((inp: Vector[TRestHalfEdge[QuineId]]) => SuccessEnvelope.Ok.apply(inp))
  }

  private val debugOpsEdgesGetServerEndpoint: Full[
    Unit,
    Unit,
    (
      QuineId,
      Option[AtTime],
      Option[Int],
      Option[TEdgeDirection],
      Option[QuineId],
      Option[String],
      Option[Boolean],
      Option[String],
    ),
    ServerError,
    SuccessEnvelope.Ok[Vector[TRestHalfEdge[QuineId]]],
    Any,
    Future,
  ] = debugOpsEdgesGet.serverLogic[Future](debugOpsEdgesGetLogic)

  val debugEndpoints: List[ServerEndpoint[Any, Future]] = List(
    debugOpsPropertyGetServerEndpoint,
    debugOpsGetServerEndpoint,
    debugOpsVerboseServerEndpoint,
    debugOpsEdgesGetServerEndpoint,
  )
}

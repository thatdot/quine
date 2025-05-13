package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.Json
import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Codec, DecodeResult, EndpointInput, path, query, statusCode}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.definitions.ErrorResponse.ServerError
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.serverError
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}

object V2DebugEndpointEntities {
  sealed abstract class TEdgeDirection
  object TEdgeDirection {
    case object Outgoing extends TEdgeDirection
    case object Incoming extends TEdgeDirection
    case object Undirected extends TEdgeDirection

    val values: Seq[TEdgeDirection] = Seq(Outgoing, Incoming, Undirected)
  }

  @title("Node Data")
  @description("Data locally available on a node in the graph.")
  final case class TLiteralNode[ID](
    @title(
      """Properties on the node; note that values are represented as closely as possible
                                        |to how they would be emitted by
                                        |[the cypher query endpoint](https://quine.io/reference/rest-api/#/paths/api-v1-query-cypher/post)
                                        |""".stripMargin.replace('\n', ' ').trim,
    )
    properties: Map[String, Json],
    edges: Seq[TRestHalfEdge[ID]],
  )

  @title("Half Edge")
  @description("""
One "half" of an edge. A full logical graph edge exists in a Quine graph if and only if
the two nodes at the edge's endpoints contain half edges that:

  * Point to each other

  * Have the same label

  * Have opposite directions (eg. one side is incoming and the other is outgoing,
    or else both sides are undirected)
""")
  final case class TRestHalfEdge[ID](
    @description("Label of the edge") edgeType: String,
    direction: TEdgeDirection,
    @description("Id of node at the other end of the edge") other: ID,
  )

}

trait V2DebugEndpoints extends V2QuineEndpointDefinitions with V2ApiConfiguration {

  implicit val tEdgeDirectionCodec: Codec[String, TEdgeDirection, TextPlain] = {

    def fromString(s: String): DecodeResult[TEdgeDirection] = s.toLowerCase match {
      case "outgoing" => DecodeResult.Value(TEdgeDirection.Outgoing)
      case "incoming" => DecodeResult.Value(TEdgeDirection.Incoming)
      case "undirected" => DecodeResult.Value(TEdgeDirection.Undirected)
      case other => DecodeResult.Error(other, new IllegalArgumentException(s"'$other' is not a valid EdgeDirection"))
    }

    Codec.string.mapDecode(fromString)(_.toString)
  }

  val idPathElement: EndpointInput.PathCapture[QuineId] = path[QuineId]("id").description("Node id")
  val propKeyParameter: EndpointInput.Query[String] =
    query[String]("key").description(
      "Name of a property",
    )

  val edgeTypeOptParameter: EndpointInput.Query[Option[String]] =
    query[Option[String]]("type").description(
      "Edge type",
    )

  val otherOptParameter: EndpointInput.Query[Option[QuineId]] =
    query[Option[QuineId]]("other").description(
      "Other edge endpoint",
    )

  val limitParameter: EndpointInput.Query[Option[Int]] =
    query[Option[Int]]("limit").description("Maximum number of results to return")
  val fullEdgeParameter: EndpointInput.Query[Option[Boolean]] =
    query[Option[Boolean]]("onlyFull").description("Only return full edges")
  val edgeDirOptParameter: EndpointInput.Query[Option[TEdgeDirection]] =
    query[Option[TEdgeDirection]]("direction").description("Edge direction. One of: Incoming, Outgoing, Undirected")
  /*
    final val edgeType: QueryString[String] = qs[String]("type", docs = Some("Edge type"))
    final val propKey: QueryString[String] = qs[String]("key", docs = Some("Name of a property"))
    final val other: QueryString[Id] = qs[Id]("other", docs = Some("Other edge endpoint"))
   // final val otherOpt: QueryString[Option[Id]] = qs[Option[Id]]("other", docs = Some("Other edge endpoint"))

   */
  /** Generate an endpoint at  /api/ v2/admin/$path */
  private def debugEndpoint = rawEndpoint("debug", "nodes")
    .tag("Debug Node Operations")
    .errorOut(serverError())

  private val debugOpsPropertyGetEndpoint =
    debugEndpoint
      .name("Get Property")
      .description(
        """Retrieve a single property from the node; note that values are represented as
closely as possible to how they would be emitted by
[the cypher query endpoint](https://quine.io/reference/rest-api/#/paths/api-v1-query-cypher/post)."""
        + "\n\nThis endpoint's usage, including the structure of the values returned, are implementation-specific and subject to change without warning. This endpoint is not intended for consumption by automated clients. The information returned by this endpoint is formatted for human consumption and is intended to assist the operator[s] of Quine in inspecting specific parts of the internal Quine graph state.",
      )
      .in(idPathElement)
      .in("props")
      .in(propKeyParameter)
      .in(atTimeParameter)
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Option[Json]]])
      .serverLogic[Future] { case (id, propKey, atime, ns) =>
        recoverServerError(appMethods.debugOpsPropertyGet(id, propKey, atime, namespaceFromParam(ns)))(
          (inp: Option[Json]) => SuccessEnvelope.Ok.apply(inp),
        )
      }

  private val debugOpsGetEndpoint
    : Full[Unit, Unit, (QuineId, Option[AtTime], Option[String]), ServerError, SuccessEnvelope.Ok[
      TLiteralNode[
        QuineId,
      ],
    ], Any, Future] =
    debugEndpoint
      .name("List Properties/Edges")
      .description(
        "Retrieve a node's list of properties and list of edges." +
        "\n\nThis endpoint's usage, including the structure of the values returned, are implementation-specific and subject to change without warning. This endpoint is not intended for consumption by automated clients. The information returned by this endpoint is formatted for human consumption and is intended to assist the operator[s] of Quine in inspecting specific parts of the internal Quine graph state.",
      )
      .in(idPathElement)
      .in(atTimeParameter)
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[TLiteralNode[QuineId]]])
      .serverLogic[Future] { case (id, atime, ns) =>
        recoverServerError(appMethods.debugOpsGet(id, atime, namespaceFromParam(ns)))((inp: TLiteralNode[QuineId]) =>
          SuccessEnvelope.Ok.apply(inp),
        )
      }

  //TODO temporarily outputs string
  private val debugOpsVerboseEndpoint = debugEndpoint
    .name("List Node State (Verbose)")
    .description(
      "Returns information relating to the node's internal state." +
      "\n\nThis endpoint's usage, including the structure of the values returned, are implementation-specific and subject to change without warning. This endpoint is not intended for consumption by automated clients. The information returned by this endpoint is formatted for human consumption and is intended to assist the operator[s] of Quine in inspecting specific parts of the internal Quine graph state.",
    )
    .in(idPathElement)
    .in("verbose")
    .in(atTimeParameter)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[String]])
    .serverLogic[Future] { case (id, atime, ns) =>
      recoverServerError(appMethods.debugOpsVerbose(id, atime, namespaceFromParam(ns)))((inp: String) =>
        SuccessEnvelope.Ok.apply(inp),
      )
    }

  private val debugOpsEdgesGetEndpoint =
    debugEndpoint
      .name("List Edges")
      .description(
        "Retrieve all node edges." +
        "\n\nThis endpoint's usage, including the structure of the values returned, are implementation-specific and subject to change without warning. This endpoint is not intended for consumption by automated clients. The information returned by this endpoint is formatted for human consumption and is intended to assist the operator[s] of Quine in inspecting specific parts of the internal Quine graph state.",
      )
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
      .serverLogic[Future] { case (id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, fullOnly, ns) =>
        recoverServerError(
          if (fullOnly.getOrElse(true))
            appMethods.debugOpsEdgesGet(id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceFromParam(ns))
          else
            appMethods.debugOpsHalfEdgesGet(id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceFromParam(ns)),
        )((inp: Vector[TRestHalfEdge[QuineId]]) => SuccessEnvelope.Ok.apply(inp))
      }

  val debugEndpoints: List[ServerEndpoint[Any, Future]] = List(
    debugOpsPropertyGetEndpoint,
    debugOpsGetEndpoint,
    debugOpsVerboseEndpoint,
    debugOpsEdgesGetEndpoint,
  )

}

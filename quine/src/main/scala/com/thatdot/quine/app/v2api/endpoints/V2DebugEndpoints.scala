package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.auto._
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Codec, DecodeResult, Endpoint, EndpointInput, Schema, path, query}

import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineId

object V2DebugEndpointEntities {
  sealed abstract class TEdgeDirection
  object TEdgeDirection {
    case object Outgoing extends TEdgeDirection
    case object Incoming extends TEdgeDirection
    case object Undirected extends TEdgeDirection

    val values: Seq[TEdgeDirection] = Seq(Outgoing, Incoming, Undirected)
  }

  implicit val tEdgeDirectionCodec: Codec[String, TEdgeDirection, TextPlain] = {

    def fromString(s: String): DecodeResult[TEdgeDirection] = s.toLowerCase match {
      case "outgoing" => DecodeResult.Value(TEdgeDirection.Outgoing)
      case "incoming" => DecodeResult.Value(TEdgeDirection.Incoming)
      case "undirected" => DecodeResult.Value(TEdgeDirection.Undirected)
      case other => DecodeResult.Error(other, new IllegalArgumentException(s"'$other' is not a valid EdgeDirection"))
    }

    Codec.string.mapDecode(fromString)(_.toString)
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

trait V2DebugEndpoints extends V2QuineEndpointDefinitions {
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
  val edgeDirOptParameter: EndpointInput.Query[Option[TEdgeDirection]] =
    query[Option[TEdgeDirection]]("direction").description("Edge direction. One of: Incoming, Outgoing, Undirected")
  /*
    final val edgeType: QueryString[String] = qs[String]("type", docs = Some("Edge type"))
    final val propKey: QueryString[String] = qs[String]("key", docs = Some("Name of a property"))
    final val other: QueryString[Id] = qs[Id]("other", docs = Some("Other edge endpoint"))
   // final val otherOpt: QueryString[Option[Id]] = qs[Option[Id]]("other", docs = Some("Other edge endpoint"))

   */
  /** Generate an endpoint at  /api/ v2/admin/$path */
  private def debugEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): Endpoint[Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T], Any] =
    baseEndpoint[T]("debug").tag("Debug Node Operations")

  private val debugOpsPropertyGetEndpoint =
    debugEndpoint[Option[Json]]
      .name("Get Property")
      .description("""Retrieve a single property from the node; note that values are represented as
closely as possible to how they would be emitted by
[the cypher query endpoint](https://quine.io/reference/rest-api/#/paths/api-v1-query-cypher/post).
""").in(idPathElement)
      .in("props")
      .in(propKeyParameter)
      .in(atTimeParameter)
      .in(namespaceParameter)
      .get
      .serverLogic { case (memberIdx, id, propKey, atime, ns) =>
        runServerLogic[(QuineId, String, Option[AtTime], NamespaceId), Option[Json]](
          DebugOpsPropertygetApiCmd,
          memberIdx,
          (id, propKey, atime, namespaceFromParam(ns)),
          t => appMethods.debugOpsPropertyGet(t._1, t._2, t._3, t._4),
        )
      }

  private val debugOpsGetEndpoint
    : Full[Unit, Unit, (Option[Int], QuineId, Option[AtTime], Option[String]), ErrorEnvelope[
      _ <: CustomError,
    ], ObjectEnvelope[TLiteralNode[QuineId]], Any, Future] =
    debugEndpoint[TLiteralNode[QuineId]]
      .name("List Properties/Edges")
      .description("Retrieve a node's list of properties and list of edges.")
      .in(idPathElement)
      .in(atTimeParameter)
      .in(namespaceParameter)
      .get
      .serverLogic { case (memberIdx, id, atime, ns) =>
        runServerLogic[(QuineId, Option[AtTime], NamespaceId), TLiteralNode[QuineId]](
          DebugOpsGetApiCmd,
          memberIdx,
          (id, atime, namespaceFromParam(ns)),
          t => appMethods.debugOpsGet(t._1, t._2, t._3),
        )
      }

  //TODO temporarily outputs string
  private val debugOpsVerboseEndpoint = debugEndpoint[String]
    .name("List Node State (Verbose)")
    .description("Returns information relating to the node's internal state.")
    .in(idPathElement)
    .in("verbose")
    .in(atTimeParameter)
    .in(namespaceParameter)
    .get
    .serverLogic { case (memberIdx, id, atime, ns) =>
      runServerLogic[(QuineId, Option[AtTime], NamespaceId), String](
        DebugVerboseApiCmd,
        memberIdx,
        (id, atime, namespaceFromParam(ns)),
        t => appMethods.debugOpsVerbose(t._1, t._2, t._3),
      )
    }

  private val debugOpsEdgesGetEndpoint =
    debugEndpoint[Vector[TRestHalfEdge[QuineId]]]
      .in("edges")
      .name("List Edges")
      .description("Retrieve all node edges.")
      .in(idPathElement)
      .in("edges")
      .in(atTimeParameter)
      .in(limitParameter)
      .in(edgeDirOptParameter)
      .in(otherOptParameter)
      .in(edgeTypeOptParameter)
      .in(namespaceParameter)
      .get
      .serverLogic { case (memberIdx, id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, ns) =>
        runServerLogic[
          (QuineId, Option[AtTime], Option[Int], Option[TEdgeDirection], Option[QuineId], Option[String], NamespaceId),
          Vector[TRestHalfEdge[QuineId]],
        ](
          DebugEdgesGetApiCmd,
          memberIdx,
          (id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceFromParam(ns)),
          t => appMethods.debugOpsEdgesGet(t._1, t._2, t._3, t._4, t._5, t._6, t._7),
        )
      }

  private val debugOpsHalfEdgesGetEndpoint =
    debugEndpoint[Vector[TRestHalfEdge[QuineId]]]
      .name("List Half Edges")
      .description("Retrieve all half edges associated with a node.")
      .in(idPathElement)
      .in("edges")
      .in("half")
      .in(atTimeParameter)
      .in(limitParameter)
      .in(edgeDirOptParameter)
      .in(otherOptParameter)
      .in(edgeTypeOptParameter)
      .in(namespaceParameter)
      .get
      .serverLogic { case (memberIdx, id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, ns) =>
        runServerLogic[
          (QuineId, Option[AtTime], Option[Int], Option[TEdgeDirection], Option[QuineId], Option[String], NamespaceId),
          Vector[TRestHalfEdge[QuineId]],
        ](
          DebugHalfEdgesGetApiCmd,
          memberIdx,
          (id, atime, limit, edgeDirOpt, otherOpt, edgeTypeOpt, namespaceFromParam(ns)),
          t => appMethods.debugOpsEdgesGet(t._1, t._2, t._3, t._4, t._5, t._6, t._7),
        )
      }

  val debugEndpoints: List[ServerEndpoint[Any, Future]] = List(
    debugOpsPropertyGetEndpoint,
    debugOpsGetEndpoint,
    debugOpsVerboseEndpoint,
    debugOpsEdgesGetEndpoint,
    debugOpsHalfEdgesGetEndpoint,
  )

}

package com.thatdot.quine.routes

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title}

/* TODO:
 *
 *   - edge literal instructions
 */

sealed abstract class EdgeDirection
object EdgeDirection {
  case object Outgoing extends EdgeDirection
  case object Incoming extends EdgeDirection
  case object Undirected extends EdgeDirection

  val values: Seq[EdgeDirection] = Seq(Outgoing, Incoming, Undirected)
}

@title("Node Data")
@docs("Data locally available on a node in the graph.")
final case class LiteralNode[Id, BStr](
  @docs("properties on the node; note that values are the base64-encoded serialized bytes")
  properties: Map[String, BStr],
  edges: Seq[RestHalfEdge[Id]]
)

@title("Half Edge")
@docs("""
One "half" of an edge. A full logical graph edge exists in a Quine graph if and only if
the two nodes at the edge's endpoints contain half edges that:

  * point to each other

  * have the same label

  * have opposite directions (eg. one side is incoming and the other is outgoing,
    or else both sides are undirected)
""")
final case class RestHalfEdge[Id](
  @docs("name of the edge") edgeType: String,
  direction: EdgeDirection,
  @docs("id of node at the other end of the edge") other: Id
)

trait LiteralRoutes
    extends endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints
    with exts.AnySchema {

  implicit final lazy val literalNodeSchema: JsonSchema[LiteralNode[Id, BStr]] =
    genericJsonSchema[LiteralNode[Id, BStr]]

  implicit final lazy val edgeDirectionSchema: JsonSchema[EdgeDirection] =
    stringEnumeration[EdgeDirection](EdgeDirection.values)(_.toString)
      .withTitle("Edge direction")
      .withDescription("direction of an edge in the graph")

  implicit final lazy val restHalfEdgeSchema: JsonSchema[RestHalfEdge[Id]] =
    genericJsonSchema[RestHalfEdge[Id]]

  implicit final lazy val edgeDirectionQueryStringParam: QueryStringParam[EdgeDirection] =
    stringQueryString.xmapWithCodec[EdgeDirection](
      endpoints4s.Codec.parseStringCatchingExceptions(
        `type` = "edge direction",
        parse = {
          case "Outgoing" => EdgeDirection.Outgoing
          case "Incoming" => EdgeDirection.Incoming
          case "Undirected" => EdgeDirection.Undirected
          case "outgoing" => EdgeDirection.Outgoing
          case "incoming" => EdgeDirection.Incoming
          case "undirected" => EdgeDirection.Undirected
          case "out" => EdgeDirection.Outgoing
          case "in" => EdgeDirection.Incoming
          case "un" => EdgeDirection.Undirected
        },
        print = _.toString
      )
    )

  final val limit: QueryString[Option[Int]] =
    qs[Option[Int]]("limit", docs = Some("maximum number of results to return"))
  final val edgeDir: QueryString[EdgeDirection] = qs[EdgeDirection](
    "direction",
    docs = Some("edge direction. One of: Incoming, Outgoing, Undirected")
  )
  final val edgeDirOpt: QueryString[Option[EdgeDirection]] = qs[Option[EdgeDirection]](
    "direction",
    docs = Some("edge direction. One of: Incoming, Outgoing, Undirected")
  )
  final val edgeType: QueryString[String] = qs[String]("type", docs = Some("edge type"))
  final val edgeTypeOpt: QueryString[Option[String]] = qs[Option[String]]("type", docs = Some("edge type"))
  final val propKey: QueryString[String] = qs[String]("key", docs = Some("name of a property"))
  final val other: QueryString[Id] = qs[Id]("other", docs = Some("other edge endpoint"))
  final val otherOpt: QueryString[Option[Id]] = qs[Option[Id]]("other", docs = Some("other edge endpoint"))

  private val api = path / "api" / "v1"
  private val literalPrefix = api / "query" / "literal"
  private val literal = literalPrefix / nodeIdSegment

  private[this] val literalTag = Tag("Literal Node Operations")
    .withDescription(
      Some(
        "Operations that are lower level and involve requests to individual nodes in the graph."
      )
    )

  final val literalGet: Endpoint[(Id, AtTime), LiteralNode[Id, BStr]] =
    endpoint(
      request = get(literal /? atTime),
      response = ok(jsonResponse[LiteralNode[Id, BStr]]),
      docs = EndpointDocs()
        .withSummary(Some("fetch properties and edges of a node"))
        .withDescription(
          Some(
            "Fetch off of a graph node all of the local information it has " +
            "stored about properties and edges."
          )
        )
        .withTags(List(literalTag))
    )

  final val literalPost: Endpoint[(Id, LiteralNode[Id, BStr]), Unit] =
    endpoint(
      request = post(
        url = literal,
        entity = jsonRequest[LiteralNode[Id, BStr]]
      ),
      ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("add or update properties and edges"))
        .withTags(List(literalTag))
    )

  final val literalDelete: Endpoint[Id, Unit] =
    endpoint(
      request = delete(literal),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("clear all properties and edges"))
        .withTags(List(literalTag))
    )

  final val literalDebug: Endpoint[(Id, AtTime), ujson.Value] =
    endpoint(
      request = get(literal / "debug" /? atTime),
      response = ok(jsonResponse(anySchema(None))),
      docs = EndpointDocs()
        .withSummary(Some("inspect internal node state"))
        .withTags(List(literalTag))
    )

  final val literalEdgesGet: Endpoint[
    (Id, (AtTime, Option[Int], Option[EdgeDirection], Option[Id], Option[String])),
    Seq[RestHalfEdge[Id]]
  ] =
    endpoint(
      request = get(literal / "edges" /? (atTime & limit & edgeDirOpt & otherOpt & edgeTypeOpt)),
      response = ok(jsonResponse[Seq[RestHalfEdge[Id]]]),
      docs = EndpointDocs()
        .withSummary(Some("fetch edges associated with a node"))
        .withTags(List(literalTag))
    )

  final val literalEdgePut: Endpoint[(Id, Seq[RestHalfEdge[Id]]), Unit] =
    endpoint(
      request = put(
        url = literal / "edges",
        entity = jsonRequest[Seq[RestHalfEdge[Id]]]
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("add full edges"))
        .withTags(List(literalTag))
    )

  final val literalEdgeDelete: Endpoint[(Id, Seq[RestHalfEdge[Id]]), Unit] =
    endpoint(
      request = request(
        Delete,
        url = literal / "edges",
        entity = jsonRequest[Seq[RestHalfEdge[Id]]]
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("remove full edges"))
        .withTags(List(literalTag))
    )

  final val literalHalfEdgesGet: Endpoint[
    (Id, (AtTime, Option[Int], Option[EdgeDirection], Option[Id], Option[String])),
    Seq[RestHalfEdge[Id]]
  ] =
    endpoint(
      request = get(literal / "edges" / "half" /? (atTime & limit & edgeDirOpt & otherOpt & edgeTypeOpt)),
      response = ok(jsonResponse[Seq[RestHalfEdge[Id]]]),
      docs = EndpointDocs()
        .withSummary(Some("fetch half edges associated with a node"))
        .withTags(List(literalTag))
    )

  final val literalPropertyGet: Endpoint[(Id, String, AtTime), Option[BStr]] =
    endpoint(
      request = get(literal / "props" /? (propKey & atTime)),
      response = wheneverFound(ok(jsonResponse[BStr])),
      docs = EndpointDocs()
        .withSummary(Some("fetch a property on a node"))
        .withTags(List(literalTag))
    )

  final val literalPropertyPut: Endpoint[(Id, String, BStr), Unit] =
    endpoint(
      request = put(
        url = literal / "props" /? propKey,
        entity = jsonRequest[BStr]
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("set a property on a node"))
        .withTags(List(literalTag))
    )

  final val literalPropertyDelete: Endpoint[(Id, String), Unit] =
    endpoint(
      request = delete(literal / "props" /? propKey),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("remove a property on a node"))
        .withTags(List(literalTag))
    )
}

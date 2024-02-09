package com.thatdot.quine.routes

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import io.circe.Json

import com.thatdot.quine.routes.exts.NamespaceParameterWrapper.NamespaceParameter

sealed abstract class EdgeDirection
object EdgeDirection {
  case object Outgoing extends EdgeDirection
  case object Incoming extends EdgeDirection
  case object Undirected extends EdgeDirection

  val values: Seq[EdgeDirection] = Seq(Outgoing, Incoming, Undirected)
}

@unnamed
@title("Node Data")
@docs("Data locally available on a node in the graph.")
final case class LiteralNode[Id](
  @docs(
    """Properties on the node; note that values are represented as closely as possible
                                      |to how they would be emitted by
                                      |[the cypher query endpoint](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-query-cypher/post)
                                      |""".stripMargin.replace('\n', ' ').trim
  )
  properties: Map[String, Json],
  edges: Seq[RestHalfEdge[Id]]
)

@unnamed
@title("Half Edge")
@docs("""
One "half" of an edge. A full logical graph edge exists in a Quine graph if and only if
the two nodes at the edge's endpoints contain half edges that:

  * Point to each other

  * Have the same label

  * Have opposite directions (eg. one side is incoming and the other is outgoing,
    or else both sides are undirected)
""")
final case class RestHalfEdge[Id](
  @docs("Label of the edge") edgeType: String,
  direction: EdgeDirection,
  @docs("Id of node at the other end of the edge") other: Id
)

trait DebugOpsRoutes
    extends endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints
    with exts.AnySchema {

  private val DebugOpsDisclaimer: String =
    """
      |
      |This endpoint's usage, including the structure of the values returned,
      |are implementation-specific and subject to change without warning. This
      |endpoint is not intended for consumption by automated clients. The information
      |returned by this endpoint is formatted for human consumption and is intended
      |to assist the operator[s] of Quine in inspecting specific parts of the internal
      |Quine graph state.
      |
      |For querying from an automated system, use [one of the language-specific interfaces]""".stripMargin
      .+("(https://docs.quine.io/reference/rest-api.html#/paths/api-v1-query-cypher/post)")

  /** Schema to be used for QuineValues -- this is specifically left explicit, as `Json` is too generic a type to have
    * a useful implicit schema around for.
    */
  private val anySchemaQVMapExample: JsonSchema[Json] = anySchema(Some("quine-value")).withExample(
    Json.obj(
      "name" -> Json.fromString("fruits-collection"),
      "fruits" -> Json.arr(Json.fromString("apple"), Json.fromString("orange"), Json.fromString("grape"))
    )
  )

  implicit final lazy val literalNodeSchema: Record[LiteralNode[Id]] = {
    implicit val propertiesMapSchema: JsonSchema[Map[String, Json]] =
      mapJsonSchema(anySchemaQVMapExample).withExample(
        Map(
          "prop1" -> Json.obj(
            "hello" -> Json.fromString("world")
          ),
          "prop2" -> Json.fromInt(128),
          "another-prop" -> Json.False
        )
      )
    genericRecord[LiteralNode[Id]]
  }

  implicit final lazy val edgeDirectionSchema: Enum[EdgeDirection] =
    stringEnumeration[EdgeDirection](EdgeDirection.values)(_.toString)
      .withTitle("Edge direction")
      .withDescription("Direction of an edge in the graph")

  implicit final lazy val restHalfEdgeSchema: Record[RestHalfEdge[Id]] =
    genericRecord[RestHalfEdge[Id]]

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
    qs[Option[Int]]("limit", docs = Some("Maximum number of results to return"))
  final val edgeDir: QueryString[EdgeDirection] = qs[EdgeDirection](
    "direction",
    docs = Some("Edge direction. One of: Incoming, Outgoing, Undirected")
  )
  final val edgeDirOpt: QueryString[Option[EdgeDirection]] = qs[Option[EdgeDirection]](
    "direction",
    docs = Some("Edge direction. One of: Incoming, Outgoing, Undirected")
  )
  final val edgeType: QueryString[String] = qs[String]("type", docs = Some("Edge type"))
  final val edgeTypeOpt: QueryString[Option[String]] = qs[Option[String]]("type", docs = Some("Edge type"))
  final val propKey: QueryString[String] = qs[String]("key", docs = Some("Name of a property"))
  final val other: QueryString[Id] = qs[Id]("other", docs = Some("Other edge endpoint"))
  final val otherOpt: QueryString[Option[Id]] = qs[Option[Id]]("other", docs = Some("Other edge endpoint"))

  private val api = path / "api" / "v1"
  private val debugPrefix = api / "debug"
  private val debugNode = debugPrefix / nodeIdSegment

  private[this] val debugOpsTag = Tag("Debug Node Operations")
    .withDescription(
      Some(
        "Operations that are lower level and involve sending requests to individual nodes in the graph."
      )
    )

  final val debugOpsGet: Endpoint[(Id, AtTime, NamespaceParameter), LiteralNode[Id]] =
    endpoint(
      request = get(debugNode /? (atTime & namespace)),
      response = ok(jsonResponse[LiteralNode[Id]]),
      docs = EndpointDocs()
        .withSummary(Some("List Properties/Edges"))
        .withDescription(
          Some(
            "Retrieve a node's list of properties and list of edges." + DebugOpsDisclaimer
          )
        )
        .withTags(List(debugOpsTag))
    )

  final val debugOpsPut: Endpoint[(Id, NamespaceParameter, LiteralNode[Id]), Unit] =
    endpoint(
      request = put(
        url = debugNode /? namespace,
        entity = jsonOrYamlRequest[LiteralNode[Id]]
      ),
      ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Update Properties/Edges"))
        .withDescription(Some("""
                                |Add or update properties and edges.
                                |
                                |Any properties or edges that do not already exist on the node will replace existing values.
                                |Any new properties or edges will be appended to existing values.
                                |Properties must be specified as JSON values, the format of which should match
                                |how the same values would be emitted by
                                |[the cypher query endpoint](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-query-cypher/post).
                                |""".stripMargin.trim + DebugOpsDisclaimer))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsDelete: Endpoint[(Id, NamespaceParameter), Unit] =
    endpoint(
      request = delete(debugNode /? namespace),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Delete Properties/Edges"))
        .withDescription(Some("Delete all properties and edges from a node." + DebugOpsDisclaimer))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsVerbose: Endpoint[(Id, AtTime, NamespaceParameter), Json] =
    endpoint(
      request = get(debugNode / "verbose" /? (atTime & namespace)),
      response = ok(jsonResponse(anySchemaQVMapExample)),
      docs = EndpointDocs()
        .withSummary(Some("List Node State (Verbose)"))
        .withDescription(
          Some(
            "Returns information relating to the node's internal state." + DebugOpsDisclaimer
          )
        )
        .withTags(List(debugOpsTag))
    )

  final val debugOpsEdgesGet: Endpoint[
    (Id, (AtTime, Option[Int], Option[EdgeDirection], Option[Id], Option[String], NamespaceParameter)),
    Seq[RestHalfEdge[Id]]
  ] =
    endpoint(
      request = get(debugNode / "edges" /? (atTime & limit & edgeDirOpt & otherOpt & edgeTypeOpt & namespace)),
      response = ok(jsonResponse[Seq[RestHalfEdge[Id]]]),
      docs = EndpointDocs()
        .withSummary(Some("List Edges"))
        .withDescription(Some("Retrieve all node edges." + DebugOpsDisclaimer))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsEdgesPut: Endpoint[(Id, NamespaceParameter, Seq[RestHalfEdge[Id]]), Unit] =
    endpoint(
      request = put(
        url = debugNode / "edges" /? namespace,
        entity = jsonOrYamlRequest[Seq[RestHalfEdge[Id]]]
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Add Full Edges"))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsEdgeDelete: Endpoint[(Id, NamespaceParameter, Seq[RestHalfEdge[Id]]), Unit] =
    endpoint(
      request = request(
        Delete,
        url = debugNode / "edges" /? namespace,
        entity = jsonOrYamlRequest[Seq[RestHalfEdge[Id]]]
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Delete Full Edges"))
        .withDescription(Some("Delete the specified full edges from this node." + DebugOpsDisclaimer))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsHalfEdgesGet: Endpoint[
    (Id, (AtTime, Option[Int], Option[EdgeDirection], Option[Id], Option[String], NamespaceParameter)),
    Seq[RestHalfEdge[Id]]
  ] =
    endpoint(
      request = get(debugNode / "edges" / "half" /? (atTime & limit & edgeDirOpt & otherOpt & edgeTypeOpt & namespace)),
      response = ok(jsonResponse[Seq[RestHalfEdge[Id]]]),
      docs = EndpointDocs()
        .withSummary(Some("List Half Edges"))
        .withDescription(Some("Retrieve all half edges associated with a node." + DebugOpsDisclaimer))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsPropertyGet: Endpoint[(Id, String, AtTime, NamespaceParameter), Option[Json]] =
    endpoint(
      request = get(debugNode / "props" /? (propKey & atTime & namespace)),
      response = wheneverFound(ok(jsonResponse[Json](anySchemaQVMapExample))),
      docs = EndpointDocs()
        .withSummary(Some("Get Property"))
        .withDescription(
          Some(
            """Retrieve a single property from the node; note that values are represented as
              |closely as possible to how they would be emitted by
              |[the cypher query endpoint](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-query-cypher/post).
              |""".stripMargin.replace('\n', ' ').trim + DebugOpsDisclaimer
          )
        )
        .withTags(List(debugOpsTag))
    )

  final val debugOpsPropertyPut: Endpoint[(Id, String, NamespaceParameter, Json), Unit] =
    endpoint(
      request = put(
        url = debugNode / "props" /? (propKey & namespace),
        entity = jsonOrYamlRequest[Json](anySchemaQVMapExample)
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Set Property"))
        .withDescription(Some("Set a single named property on a node." + DebugOpsDisclaimer))
        .withTags(List(debugOpsTag))
    )

  final val debugOpsPropertyDelete: Endpoint[(Id, String, NamespaceParameter), Unit] =
    endpoint(
      request = delete(debugNode / "props" /? (propKey & namespace)),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Delete Property"))
        .withTags(List(debugOpsTag))
    )
}

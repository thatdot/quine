package com.thatdot.quine.v2api.routes

import scala.concurrent.duration.FiniteDuration

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import io.circe.Json

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter

/** V2 API response wrapper to match server-side SuccessEnvelope structure */
@unnamed
@title("Success Response")
@docs("API v2 success response wrapper")
final case class V2SuccessResponse[Content](
  @docs("Response content") content: Content,
  @docs("Optional message") message: Option[String] = None,
  @docs("Warning messages") warnings: List[String] = Nil,
)

/** Browser-compatible V2 UI Node type (using String ID instead of QuineId) */
@unnamed
@title("V2 Graph Node")
@docs("Information needed by the Query UI to display a node in the graph (V2 API)")
final case class V2UiNode(
  @docs("node id as string") id: String,
  @docs("index of the cluster host responsible for this node") hostIndex: Int,
  @docs("categorical classification") label: String,
  @docs("properties on the node") properties: Map[String, Json],
)

/** Browser-compatible V2 UI Edge type (using String ID instead of QuineId) */
@unnamed
@title("V2 Graph Edge")
@docs("Information needed by the Query UI to display an edge in the graph (V2 API)")
final case class V2UiEdge(
  @docs("Node at the start of the edge") from: String,
  @docs("Name of the edge") edgeType: String,
  @docs("Node at the end of the edge") to: String,
  @docs("Whether the edge is directed or undirected") isDirected: Boolean = true,
)

trait V2QuerySchemas extends endpoints4s.generic.JsonSchemas with exts.AnySchema with exts.IdSchema {

  implicit lazy val v2UiNodeSchema: Record[V2UiNode] = {
    implicit val property = anySchema(None)
    genericRecord[V2UiNode]
      .withExample(
        V2UiNode(
          id = "sample-node-id",
          hostIndex = 0,
          label = "test-label",
          properties = Map(
            "string_property" -> Json.fromString("string"),
            "number_property" -> Json.fromInt(1980),
          ),
        ),
      )
  }

  implicit lazy val v2UiEdgeSchema: Record[V2UiEdge] =
    genericRecord[V2UiEdge]
      .withExample(
        V2UiEdge(
          from = "from-node-id",
          edgeType = "knows",
          to = "to-node-id",
          isDirected = true,
        ),
      )

  // JsonSchema for V2SuccessResponse that matches server's SuccessEnvelope.Ok structure
  // Server sends: {"content": [...], "message": null, "warnings": []}
  implicit def v2SuccessResponseSchema[T](implicit contentSchema: JsonSchema[T]): JsonSchema[V2SuccessResponse[T]] = {
    implicit val stringOptSchema: JsonSchema[Option[String]] = optionalSchema(stringJsonSchema(None))
    implicit val stringListSchema: JsonSchema[List[String]] = arrayJsonSchema(stringJsonSchema(None), implicitly)
    genericRecord[V2SuccessResponse[T]]
  }

}

trait V2QueryUiRoutes extends QueryUiRoutes with V2QuerySchemas {

  final protected val v2Query: Path[Unit] = path / "api" / "v2" / "cypher-queries"

  protected val v2CypherTag: Tag = Tag("Cypher Query Language V2")

  final type V2QueryInputs[A] = (AtTime, Option[FiniteDuration], NamespaceParameter, A)

  // Helper function to convert V2UiNode to UiNode[Id] for compatibility
  protected def convertV2NodeToV1(v2Node: V2UiNode): UiNode[Id] = {
    val decodedId = idCodec.decode(v2Node.id) match {
      case endpoints4s.Valid(id) => id
      case endpoints4s.Invalid(errors) => throw new RuntimeException(s"Invalid ID: ${v2Node.id}, errors: $errors")
    }
    UiNode(decodedId, v2Node.hostIndex, v2Node.label, v2Node.properties)
  }

  // Helper function to convert V2UiEdge to UiEdge[Id] for compatibility
  protected def convertV2EdgeToV1(v2Edge: V2UiEdge): UiEdge[Id] = {
    val decodedFromId = idCodec.decode(v2Edge.from) match {
      case endpoints4s.Valid(id) => id
      case endpoints4s.Invalid(errors) => throw new RuntimeException(s"Invalid ID: ${v2Edge.from}, errors: $errors")
    }
    val decodedToId = idCodec.decode(v2Edge.to) match {
      case endpoints4s.Valid(id) => id
      case endpoints4s.Invalid(errors) => throw new RuntimeException(s"Invalid ID: ${v2Edge.to}, errors: $errors")
    }
    UiEdge(decodedFromId, v2Edge.edgeType, decodedToId, v2Edge.isDirected)
  }

  val cypherPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[CypherQueryResult]]] =
    endpoint(
      request = post(
        url = v2Query / "query-graph" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[CypherQuery](
          CypherQuery("RETURN $x+$y AS three", Map(("x" -> Json.fromInt(1)), ("y" -> Json.fromInt(2)))),
        ).orElse(textRequestWithExample("RETURN 1 + 2 AS three"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[CypherQueryResult]],
            ).xmap(response => response.content)(result =>
              V2SuccessResponse(result),
            ), // Unwrap SuccessEnvelope structure
          ),
        ),
      docs = EndpointDocs()
        .withSummary(Some("Cypher Query V2"))
        .withDescription(Some(s"Execute an arbitrary [Cypher](${cypherLanguageUrl}) query using API v2."))
        .withTags(List(v2CypherTag)),
    )

  val cypherNodesPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[Seq[UiNode[Id]]]]] =
    endpoint(
      request = post(
        url = v2Query / "query-nodes" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[CypherQuery](
          CypherQuery(
            "MATCH (n) RETURN n LIMIT $lim",
            Map(("lim" -> Json.fromInt(1))),
          ),
        ).orElse(textRequestWithExample("MATCH (n) RETURN n LIMIT 1"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[Seq[V2UiNode]]],
            ).xmap(response => response.content.map(convertV2NodeToV1))(nodes =>
              V2SuccessResponse(nodes.map(n => V2UiNode(n.id.toString, n.hostIndex, n.label, n.properties))),
            ), // Unwrap SuccessEnvelope and convert between node types
          ),
        ),
      docs = EndpointDocs()
        .withSummary(Some("Cypher Query Return Nodes V2"))
        .withDescription(Some(s"""Execute a [Cypher](${cypherLanguageUrl}) query that returns nodes using API v2.
               |Queries that do not return nodes will fail with a type error.""".stripMargin))
        .withTags(List(v2CypherTag)),
    )

  val cypherEdgesPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[Seq[UiEdge[Id]]]]] =
    endpoint(
      request = post(
        url = v2Query / "query-edges" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[CypherQuery](
          CypherQuery(
            "MATCH ()-[r]->() RETURN r LIMIT $lim",
            Map(("lim" -> Json.fromInt(1))),
          ),
        ).orElse(textRequestWithExample("MATCH ()-[r]->() RETURN r LIMIT 1"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[Seq[V2UiEdge]]],
            ).xmap(response => response.content.map(convertV2EdgeToV1))(edges =>
              V2SuccessResponse(edges.map(e => V2UiEdge(e.from.toString, e.edgeType, e.to.toString, e.isDirected))),
            ), // Unwrap SuccessEnvelope and convert between edge types
          ),
        ),
      docs = EndpointDocs()
        .withSummary(Some("Cypher Query Return Edges V2"))
        .withDescription(Some(s"""Execute a [Cypher](${cypherLanguageUrl}) query that returns edges using API v2.
               |Queries that do not return edges will fail with a type error.""".stripMargin))
        .withTags(List(v2CypherTag)),
    )
}

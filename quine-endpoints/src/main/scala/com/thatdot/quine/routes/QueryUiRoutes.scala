package com.thatdot.quine.routes

import scala.concurrent.duration.FiniteDuration

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import io.circe.Json

import com.thatdot.quine.routes.exts.EndpointsWithCustomErrorText
import com.thatdot.quine.routes.exts.NamespaceParameterWrapper.NamespaceParameter

/** Nodes in the UI
  *
  * This is the expected format to return for endpoints serving up nodes
  *
  * @param id string representation of the ID of the node
  * @param hostIdx JVM responsible node (relevant in a multi-JVM Quine cluster)
  * @param label sort of node (this gets displayed under the node)
  * @param properties key values on the node
  */
@unnamed
@title("Graph Node")
@docs("Information needed by the Query UI to display a node in the graph.")
final case class UiNode[Id](
  @docs("node id") id: Id,
  @docs("index of the cluster host responsible for this node") hostIndex: Int,
  @docs("categorical classification") label: String,
  @docs("properties on the node") properties: Map[String, Json]
)

/** Edges in the UI
  *
  * This is the expected format to return for endpoints serving up edges
  *
  * @param from string representation of the ID of one node endpoint
  * @param edgeType sort of edge (this gets displayed under the edge)
  * @param direction direction of the edge
  */
@unnamed
@title("Graph Edge")
@docs("Information needed by the Query UI to display an edge in the graph.")
final case class UiEdge[Id](
  @docs("Node at the start of the edge") from: Id,
  @docs("Name of the edge") edgeType: String,
  @docs("Node at the end of the edge") to: Id,
  @docs("Whether the edge is directed or undirected") isDirected: Boolean = true
)

/** Result of issuing a generic Cypher query
  *
  * @param columns variables returned by the query
  * @param results rows returned, where each row has the same length as `columns`
  */
@unnamed
@title("Cypher Query Result")
@docs("""Cypher queries are designed to return data in a table format. This gets
        |encoded into JSON with `columns` as the header row and each element in `results`
        |being another row of results. As a consequence Consequently, every array element
        |in `results` will have the same length, and all will have the same length as the
        |`columns` array.
        |""".stripMargin)
final case class CypherQueryResult(
  @docs("Return values of the Cypher query") columns: Seq[String],
  @docs("Rows of results") results: Seq[Seq[Json]]
)

/** A (possibly-parameterized) cypher query
  * @param text
  * @param parameters
  */
@title("Cypher Query")
final case class CypherQuery(
  @docs("Text of the query to execute") text: String,
  @docs("Parameters the query expects, if any") parameters: Map[String, Json] = Map.empty
)

/** A (possibly-parameterized) gremlin query
  * @param text
  * @param parameters
  */
@title("Gremlin Query")
final case class GremlinQuery(
  @docs("Text of the query to execute") text: String,
  @docs("Parameters the query expects, if any") parameters: Map[String, Json] = Map.empty
)

trait QuerySchemas extends endpoints4s.generic.JsonSchemas with exts.AnySchema with exts.IdSchema {

  implicit lazy val graphNodeSchema: Record[UiNode[Id]] = {
    implicit val property = anySchema(None)
    genericRecord[UiNode[Id]]
      .withExample(
        UiNode(
          id = sampleId(),
          hostIndex = 0,
          label = "Harry",
          properties = Map(
            "first_name" -> Json.fromString("Harry"),
            "last_name" -> Json.fromString("Potter"),
            "birth_year" -> Json.fromInt(1980)
          )
        )
      )

  }

  implicit lazy val graphEdgeSchema: Record[UiEdge[Id]] =
    genericRecord[UiEdge[Id]]
      .withExample(
        UiEdge(
          from = sampleId(),
          edgeType = "likes",
          to = sampleId()
        )
      )
}

trait QueryUiRoutes
    extends EndpointsWithCustomErrorText
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints
    with exts.AnySchema
    with QuerySchemas {

  implicit lazy val cypherQueryResultSchema: Record[CypherQueryResult] = {
    implicit val queryResult = anySchema(Some("cypher-value"))
    genericRecord[CypherQueryResult]
  }
  implicit lazy val cypherQuerySchema: Record[CypherQuery] = {
    implicit val parameter = anySchema(Some("cypher-value"))
    genericRecord[CypherQuery]
  }
  implicit lazy val gremlinQuerySchema: Record[GremlinQuery] = {
    implicit val parameter = anySchema(Some("gremlin-value"))
    genericRecord[GremlinQuery]
  }

  final protected val query: Path[Unit] = path / "api" / "v1" / "query"

  protected val cypherTag: Tag = Tag("Cypher Query Language")
  protected val gremlinTag: Tag = Tag("Gremlin Query Language")

  final type QueryInputs[A] = (AtTime, Option[FiniteDuration], NamespaceParameter, A)

  val gremlinLanguageUrl = "https://tinkerpop.apache.org/gremlin.html"
  val cypherLanguageUrl = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"
  val cypherPost: Endpoint[QueryInputs[CypherQuery], Either[ClientErrors, CypherQueryResult]] =
    endpoint(
      request = post(
        url = query / "cypher" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[CypherQuery](
          CypherQuery("RETURN $x+$y AS three", Map(("x" -> Json.fromInt(1)), ("y" -> Json.fromInt(2))))
        ).orElse(textRequestWithExample("RETURN 1 + 2 AS three"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq))
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(
          ok(
            jsonResponseWithExample[CypherQueryResult](
              example = CypherQueryResult(Seq("three"), Seq(Seq(Json.fromInt(3))))
            )
          )
        ),
      docs = EndpointDocs()
        .withSummary(Some("Cypher Query"))
        .withDescription(Some(s"Execute an arbitrary [Cypher]($cypherLanguageUrl) query"))
        .withTags(List(cypherTag))
    )

  val cypherNodesPost: Endpoint[QueryInputs[CypherQuery], Either[ClientErrors, Seq[UiNode[Id]]]] =
    endpoint(
      request = post(
        url = query / "cypher" / "nodes" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[CypherQuery](
          CypherQuery(
            "MATCH (n) RETURN n LIMIT $lim",
            Map(("lim" -> Json.fromInt(1)))
          )
        ).orElse(textRequestWithExample("MATCH (n) RETURN n LIMIT 1"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq))
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(ok(jsonResponse[Seq[UiNode[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("Cypher Query Return Nodes"))
        .withDescription(Some(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns nodes.
               |Queries that do not return nodes will fail with a type error.""".stripMargin))
        .withTags(List(cypherTag))
    )

  val cypherEdgesPost: Endpoint[QueryInputs[CypherQuery], Either[ClientErrors, Seq[UiEdge[Id]]]] =
    endpoint(
      request = post(
        url = query / "cypher" / "edges" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[CypherQuery](
          CypherQuery(
            "MATCH ()-[e]->() RETURN e LIMIT $lim",
            Map(("lim" -> Json.fromInt(1)))
          )
        ).orElse(textRequestWithExample("MATCH ()-[e]->() RETURN e LIMIT 1"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq))
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(ok(jsonResponse[Seq[UiEdge[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("Cypher Query Return Edges"))
        .withDescription(Some(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns edges.
              |Queries that do not return edges will fail with a type error.""".stripMargin))
        .withTags(List(cypherTag))
    )

  val gremlinPost: Endpoint[QueryInputs[GremlinQuery], Either[ClientErrors, Seq[Json]]] = {
    implicit val queryResult = anySchema(Some("gremlin JSON"))
    endpoint(
      request = post(
        url = query / "gremlin" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[GremlinQuery](
          GremlinQuery("g.V().valueMap().limit(lim)", Map("lim" -> Json.fromInt(1)))
        ).orElse(textRequestWithExample("g.V().valueMap().limit(1)"))
          .xmap[GremlinQuery](_.map(GremlinQuery(_)).merge)(gq =>
            if (gq.parameters.isEmpty) Right(gq.text) else Left(gq)
          )
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(
          ok(
            jsonResponseWithExample[Seq[Json]](
              example = Seq(
                Json.obj(
                  "first_name" -> Json.fromString("Harry"),
                  "last_name" -> Json.fromString("Potter"),
                  "birth_year" -> Json.fromInt(1980)
                )
              )
            )
          )
        ),
      docs = EndpointDocs()
        .withSummary(Some("Gremlin Query"))
        .withDescription(
          Some(s"Execute a [Gremlin]($gremlinLanguageUrl) query. Note that we only support a simple subset of Gremlin.")
        )
        .withTags(List(gremlinTag))
    )
  }

  val gremlinNodesPost: Endpoint[QueryInputs[GremlinQuery], Either[ClientErrors, Seq[UiNode[Id]]]] =
    endpoint(
      request = post(
        url = query / "gremlin" / "nodes" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[GremlinQuery](
          GremlinQuery("g.V().limit(lim)", Map("lim" -> Json.fromInt(1)))
        ).orElse(textRequestWithExample("g.V().limit(1)"))
          .xmap[GremlinQuery](_.map(GremlinQuery(_)).merge)(gq =>
            if (gq.parameters.isEmpty) Right(gq.text) else Left(gq)
          )
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(ok(jsonResponse[Seq[UiNode[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("Gremlin Query Return Nodes"))
        .withDescription(Some(s"""Execute a [Gremlin]($gremlinLanguageUrl) query that returns nodes.
              |Queries that do not return nodes will fail with a type error.""".stripMargin))
        .withTags(List(gremlinTag))
    )

  val gremlinEdgesPost: Endpoint[QueryInputs[GremlinQuery], Either[ClientErrors, Seq[UiEdge[Id]]]] =
    endpoint(
      request = post(
        url = query / "gremlin" / "edges" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequestWithExample[GremlinQuery](
          GremlinQuery("g.V().outE().limit(lim)", Map("lim" -> Json.fromInt(1)))
        ).orElse(textRequestWithExample("g.V().outE().limit(1)"))
          .xmap[GremlinQuery](_.map(GremlinQuery(_)).merge)(gq =>
            if (gq.parameters.isEmpty) Right(gq.text) else Left(gq)
          )
      ),
      response = customBadRequest("runtime error in the query")
        .orElse(ok(jsonResponse[Seq[UiEdge[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("Gremlin Query Return Edges"))
        .withDescription(Some(s"""Execute a [Gremlin]($gremlinLanguageUrl) query that returns edges.
               |Queries that do not return edges will fail with a type error.""".stripMargin))
        .withTags(List(gremlinTag))
    )
}

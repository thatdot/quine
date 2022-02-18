package com.thatdot.quine.routes

import scala.concurrent.duration.FiniteDuration

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title}

/** Nodes in the UI
  *
  * This is the expected format to return for endpoints serving up nodes
  *
  * @param id string representation of the ID of the node
  * @param hostIdx JVM responsible node (relevant in a multi-JVM Quine cluster)
  * @param label sort of node (this gets displayed under the node)
  * @param properties key values on the node
  */
@title("Graph Node")
@docs("Information needed by the Query UI to display a node in the graph.")
final case class UiNode[Id](
  @docs("node id") id: Id,
  @docs("index of the cluster host responsible for this node") hostIndex: Int,
  @docs("categorical classification") label: String,
  @docs("properties on the node") properties: Map[String, ujson.Value]
)

/** Edges in the UI
  *
  * This is the expected format to return for endpoints serving up edges
  *
  * @param from string representation of the ID of one node endpoint
  * @param edgeType sort of edge (this gets displayed under the edge)
  * @param direction direction of the edge
  */
@title("Graph Edge")
@docs("Information needed by the Query UI to display an edge in the graph.")
final case class UiEdge[Id](
  @docs("node at the start of the edge") from: Id,
  @docs("name of the edge") edgeType: String,
  @docs("node at the end of the edge") to: Id,
  @docs("whether the edge is directed or undirected") isDirected: Boolean = true
)

/** Result of issuing a generic Cypher query
  *
  * @param columns variables returned by the query
  * @param results rows returned, where each row has the same length as `columns`
  */
@title("Cypher Query Result")
@docs("""Cypher queries are designed to return data in a table format. This gets
        |encoded into JSON with `columns` as the header row and each element in `results`
        |being another row of results. As a consequence Consequently, every array element
        |in `results` will have the same length, and all will have the same length as the
        |`columns` array.
        |""".stripMargin)
final case class CypherQueryResult(
  @docs("return values of the Cypher query") columns: Seq[String],
  @docs("rows of results") results: Seq[Seq[ujson.Value]]
)

/** A (possibly-parameterized) cypher query
  * @param text
  * @param parameters
  */
@title("Cypher Query")
final case class CypherQuery(
  @docs("the text of the query to execute") text: String,
  @docs("parameters the query expects, if any") parameters: Map[String, ujson.Value] = Map.empty
)

/** A (possibly-parameterized) gremlin query
  * @param text
  * @param parameters
  */
@title("Gremlin Query")
final case class GremlinQuery(
  @docs("text of the query to execute") text: String,
  @docs("parameters the query expects, if any") parameters: Map[String, ujson.Value] = Map.empty
)

trait QuerySchemas extends endpoints4s.generic.JsonSchemas with exts.AnySchema with exts.IdSchema {

  implicit lazy val graphNodeSchema: JsonSchema[UiNode[Id]] = {
    implicit val property = anySchema(None)
    genericJsonSchema[UiNode[Id]]
      .withExample(
        UiNode(
          id = sampleId(),
          hostIndex = 0,
          label = "Harry",
          properties = Map(
            "first_name" -> ujson.Str("Harry"),
            "last_name" -> ujson.Str("Potter"),
            "birth_year" -> ujson.Num(1980)
          )
        )
      )

  }

  implicit lazy val graphEdgeSchema: JsonSchema[UiEdge[Id]] =
    genericJsonSchema[UiEdge[Id]]
      .withExample(
        UiEdge(
          from = sampleId(),
          edgeType = "likes",
          to = sampleId()
        )
      )
}

trait QueryUiRoutes
    extends endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints
    with exts.AnySchema
    with QuerySchemas {

  implicit lazy val cypherQueryResultSchema: JsonSchema[CypherQueryResult] = {
    implicit val queryResult = anySchema(Some("cypher-value"))
    genericJsonSchema[CypherQueryResult]
  }
  implicit lazy val cypherQuerySchema: JsonSchema[CypherQuery] = {
    implicit val parameter = anySchema(Some("cypher-value"))
    genericJsonSchema[CypherQuery]
  }
  implicit lazy val gremlinQuerySchema: JsonSchema[GremlinQuery] = {
    implicit val parameter = anySchema(Some("gremlin-value"))
    genericJsonSchema[GremlinQuery]
  }

  final protected val query: Path[Unit] = path / "api" / "v1" / "query"

  protected val cypherTag: Tag = Tag("Cypher Query Language")
  protected val gremlinTag: Tag = Tag("Gremlin Query Language")

  final type QueryInputs[A] = (AtTime, Option[FiniteDuration], A)

  val cypherPost: Endpoint[QueryInputs[CypherQuery], Either[ClientErrors, CypherQueryResult]] =
    endpoint(
      request = post(
        url = query / "cypher" /? (atTime & reqTimeout),
        entity = jsonRequestWithExample[CypherQuery](
          CypherQuery("RETURN $x+$y AS three", Map(("x" -> ujson.Num(1)), ("y" -> ujson.Num(2))))
        ).orElse(textRequestWithExample("RETURN 1 + 2 AS three"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq))
      ),
      response = badRequest(docs = Some("runtime error in the query"))
        .orElse(
          ok(
            jsonResponseWithExample[CypherQueryResult](
              example = CypherQueryResult(Seq("three"), Seq(Seq(ujson.Num(3))))
            )
          )
        ),
      docs = EndpointDocs()
        .withSummary(Some("issue a Cypher query"))
        .withDescription(
          Some {
            val link = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"
            s"Make an arbitrary [Cypher]($link) query"
          }
        )
        .withTags(List(cypherTag))
    )

  val cypherNodesPost: Endpoint[QueryInputs[CypherQuery], Either[ClientErrors, Seq[UiNode[Id]]]] =
    endpoint(
      request = post(
        url = query / "cypher" / "nodes" /? (atTime & reqTimeout),
        entity = jsonRequestWithExample[CypherQuery](
          CypherQuery(
            "MATCH (n) RETURN n LIMIT $lim",
            Map(("lim" -> ujson.Num(1)))
          )
        ).orElse(textRequestWithExample("MATCH (n) RETURN n LIMIT 1"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq))
      ),
      response = badRequest(docs = Some("runtime error in the query"))
        .orElse(ok(jsonResponse[Seq[UiNode[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("issue a Cypher query that returns nodes"))
        .withDescription(
          Some(
            "Make a Cypher query that returns nodes. " +
            "Queries that do not return nodes will fail with a type error."
          )
        )
        .withTags(List(cypherTag))
    )

  val cypherEdgesPost: Endpoint[QueryInputs[CypherQuery], Either[ClientErrors, Seq[UiEdge[Id]]]] =
    endpoint(
      request = post(
        url = query / "cypher" / "edges" /? (atTime & reqTimeout),
        entity = jsonRequestWithExample[CypherQuery](
          CypherQuery(
            "MATCH ()-[e]->() RETURN e LIMIT $lim",
            Map(("lim" -> ujson.Num(1)))
          )
        ).orElse(textRequestWithExample("MATCH ()-[e]->() RETURN e LIMIT 1"))
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq))
      ),
      response = badRequest(docs = Some("runtime error in the query"))
        .orElse(ok(jsonResponse[Seq[UiEdge[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("issue a Cypher query that returns edges"))
        .withDescription(
          Some(
            "Make a Cypher query that returns edges. " +
            "Queries that do not return edges will fail with a type error."
          )
        )
        .withTags(List(cypherTag))
    )

  val gremlinPost: Endpoint[QueryInputs[GremlinQuery], Either[ClientErrors, Seq[ujson.Value]]] = {
    implicit val queryResult = anySchema(Some("gremlin JSON"))
    endpoint(
      request = post(
        url = query / "gremlin" /? (atTime & reqTimeout),
        entity = jsonRequestWithExample[GremlinQuery](
          GremlinQuery("g.V().valueMap().limit(lim)", Map("lim" -> ujson.Num(1)))
        ).orElse(textRequestWithExample("g.V().valueMap().limit(1)"))
          .xmap[GremlinQuery](_.map(GremlinQuery(_)).merge)(gq =>
            if (gq.parameters.isEmpty) Right(gq.text) else Left(gq)
          )
      ),
      response = badRequest(docs = Some("runtime error in the query"))
        .orElse(
          ok(
            jsonResponseWithExample[Seq[ujson.Value]](
              example = Seq(
                ujson.Obj(
                  "first_name" -> ujson.Str("Harry"),
                  "last_name" -> ujson.Str("Potter"),
                  "birth_year" -> ujson.Num(1980)
                )
              )
            )
          )
        ),
      docs = EndpointDocs()
        .withSummary(Some("issue a Gremlin query"))
        .withDescription(Some {
          val link = "https://tinkerpop.apache.org/gremlin.html"
          s"Make a [Gremlin]($link) query. Note that we only support a simple subset of Gremlin."
        })
        .withTags(List(gremlinTag))
    )
  }

  val gremlinNodesPost: Endpoint[QueryInputs[GremlinQuery], Either[ClientErrors, Seq[UiNode[Id]]]] =
    endpoint(
      request = post(
        url = query / "gremlin" / "nodes" /? (atTime & reqTimeout),
        entity = jsonRequestWithExample[GremlinQuery](
          GremlinQuery("g.V().limit(lim)", Map("lim" -> ujson.Num(1)))
        ).orElse(textRequestWithExample("g.V().limit(1)"))
          .xmap[GremlinQuery](_.map(GremlinQuery(_)).merge)(gq =>
            if (gq.parameters.isEmpty) Right(gq.text) else Left(gq)
          )
      ),
      response = badRequest(docs = Some("runtime error in the query"))
        .orElse(ok(jsonResponse[Seq[UiNode[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("issue a Gremlin query returning nodes"))
        .withDescription(
          Some(
            "Make a Gremlin query that returns nodes. " +
            "Queries that do not return nodes will fail with a type error."
          )
        )
        .withTags(List(gremlinTag))
    )

  val gremlinEdgesPost: Endpoint[QueryInputs[GremlinQuery], Either[ClientErrors, Seq[UiEdge[Id]]]] =
    endpoint(
      request = post(
        url = query / "gremlin" / "edges" /? (atTime & reqTimeout),
        entity = jsonRequestWithExample[GremlinQuery](
          GremlinQuery("g.V().outE().limit(lim)", Map("lim" -> ujson.Num(1)))
        ).orElse(textRequestWithExample("g.V().outE().limit(1)"))
          .xmap[GremlinQuery](_.map(GremlinQuery(_)).merge)(gq =>
            if (gq.parameters.isEmpty) Right(gq.text) else Left(gq)
          )
      ),
      response = badRequest(docs = Some("runtime error in the query"))
        .orElse(ok(jsonResponse[Seq[UiEdge[Id]]])),
      docs = EndpointDocs()
        .withSummary(Some("issue a Gremlin query returning edges"))
        .withDescription(
          Some(
            "Make a Gremlin query that returns edges. " +
            "Queries that do not return edges will fail with a type error."
          )
        )
        .withTags(List(gremlinTag))
    )
}

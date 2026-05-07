package com.thatdot.quine.v2api.routes

import scala.concurrent.duration.FiniteDuration

import endpoints4s.Invalid
import endpoints4s.algebra.{Endpoints, JsonEntitiesFromSchemas, StatusCodes}
import endpoints4s.generic.{docs, title, unnamed}
import io.circe.Json

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter

/** Mirrors the AIP-193 wire shape: every error response is `{"error": {code, status, message}}`.
  * `details` is intentionally omitted — the UI renders only the human-readable `message`,
  * and unknown fields decode silently.
  */
final case class V2ErrorBody(code: Int, status: String, message: String)
final case class V2ErrorResponse(error: V2ErrorBody)

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

trait V2ErrorResponseSchema extends endpoints4s.generic.JsonSchemas {
  implicit lazy val v2ErrorBodySchema: JsonSchema[V2ErrorBody] = genericRecord[V2ErrorBody]
  implicit lazy val v2ErrorResponseSchema: JsonSchema[V2ErrorResponse] = genericRecord[V2ErrorResponse]
}

trait V2QuerySchemas
    extends endpoints4s.generic.JsonSchemas
    with V2ErrorResponseSchema
    with exts.AnySchema
    with exts.IdSchema {

  implicit lazy val v2UiNodeSchema: Record[V2UiNode] = {
    implicit val property = anySchema(None)
    genericRecord[V2UiNode]
  }

  implicit lazy val v2UiEdgeSchema: Record[V2UiEdge] =
    genericRecord[V2UiEdge]
}

trait ErrorResponses extends Endpoints with V2QuerySchemas with JsonEntitiesFromSchemas {
  def convertV2ErrorResponseToClientErrors(
    errorResponse: Either[Either[V2ErrorResponse, V2ErrorResponse], String],
  ): ClientErrors =
    errorResponse match {
      case Left(eitherV2ErrorResponse) => Invalid(Seq(eitherV2ErrorResponse.merge.error.message))
      case Right(errorMsg) => Invalid(Seq(errorMsg))
    }

  def convertClientErrorsToV2ErrorResponse(
    clientErrors: ClientErrors,
  ): Either[Either[V2ErrorResponse, V2ErrorResponse], String] =
    clientErrors match {
      case Invalid(errors) =>
        Left(
          Left(V2ErrorResponse(V2ErrorBody(code = 400, status = "INVALID_ARGUMENT", message = errors.mkString("; ")))),
        )
    }

  def errorResponses(): Response[ClientErrors] =
    response(statusCode = BadRequest, entity = jsonResponse[V2ErrorResponse])
      .orElse(response(statusCode = Unauthorized, entity = jsonResponse[V2ErrorResponse]))
      .orElse(response(statusCode = InternalServerError, entity = textResponse))
      .xmap[ClientErrors](convertV2ErrorResponseToClientErrors)(convertClientErrorsToV2ErrorResponse)
}

trait V2QueryUiRoutes extends QueryUiRoutes with V2QuerySchemas with ErrorResponses with StatusCodes {

  implicit private lazy val v2NamespaceSegment: Segment[NamespaceParameter] =
    stringSegment.xmapWithCodec(NamespaceParameter.namespaceCodec)

  private val graphScopedV2Base: Path[NamespaceParameter] =
    path / "api" / "v2" / "graph" / segment[NamespaceParameter]("graphName")

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

  /** Reorders the path-then-query tuple to match the existing [[V2QueryInputs]] shape so
    * browser callers don't have to change.
    */
  private def v2QueryInputs[A](
    url: Url[(NamespaceParameter, AtTime, Option[FiniteDuration])],
    entity: RequestEntity[A],
  ): Request[V2QueryInputs[A]] =
    post(url, entity).xmap[V2QueryInputs[A]] { case (ns, atTime, timeout, body) =>
      (atTime, timeout, ns, body)
    } { case (atTime, timeout, ns, body) =>
      (ns, atTime, timeout, body)
    }

  val cypherPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[CypherQueryResult]]] =
    endpoint(
      request = v2QueryInputs(
        url = graphScopedV2Base / "cypher:query" /? (atTime & reqTimeout),
        entity = jsonOrYamlRequest[CypherQuery]
          .orElse(textRequest)
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = errorResponses()
        .orElse(wheneverFound(ok(jsonResponse[CypherQueryResult]))),
    )

  val cypherNodesPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[Seq[UiNode[Id]]]]] =
    endpoint(
      request = v2QueryInputs(
        url = graphScopedV2Base / "cypher:queryNodes" /? (atTime & reqTimeout),
        entity = jsonOrYamlRequest[CypherQuery]
          .orElse(textRequest)
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = errorResponses()
        .orElse(
          wheneverFound(
            ok(jsonResponse[Seq[V2UiNode]])
              .xmap(_.map(convertV2NodeToV1))(nodes =>
                nodes.map(n => V2UiNode(n.id.toString, n.hostIndex, n.label, n.properties)),
              ),
          ),
        ),
    )

  val cypherEdgesPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[Seq[UiEdge[Id]]]]] =
    endpoint(
      request = v2QueryInputs(
        url = graphScopedV2Base / "cypher:queryEdges" /? (atTime & reqTimeout),
        entity = jsonOrYamlRequest[CypherQuery]
          .orElse(textRequest)
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = errorResponses()
        .orElse(
          wheneverFound(
            ok(jsonResponse[Seq[V2UiEdge]])
              .xmap(_.map(convertV2EdgeToV1))(edges =>
                edges.map(e => V2UiEdge(e.from.toString, e.edgeType, e.to.toString, e.isDirected)),
              ),
          ),
        ),
    )
}

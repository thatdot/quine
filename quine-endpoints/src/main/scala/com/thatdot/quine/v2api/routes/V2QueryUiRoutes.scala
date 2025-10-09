package com.thatdot.quine.v2api.routes

import scala.concurrent.duration.FiniteDuration

import endpoints4s.Invalid
import endpoints4s.algebra.{Endpoints, JsonEntitiesFromSchemas, StatusCodes}
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

final case class V2Error(message: String, `type`: String)
final case class V2ErrorResponse(errors: Seq[V2Error])

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

trait V2SuccessResponseSchema extends endpoints4s.generic.JsonSchemas with exts.AnySchema {
  implicit def v2SuccessResponseSchema[T](implicit contentSchema: JsonSchema[T]): JsonSchema[V2SuccessResponse[T]] = {
    implicit val stringOptSchema: JsonSchema[Option[String]] = optionalSchema(stringJsonSchema(None))
    implicit val stringListSchema: JsonSchema[List[String]] = arrayJsonSchema(stringJsonSchema(None), implicitly)
    genericRecord[V2SuccessResponse[T]]
  }
}

trait V2ErrorResponseSchema extends endpoints4s.generic.JsonSchemas {
  implicit lazy val cypherErrorSchema: JsonSchema[V2Error] = genericRecord[V2Error]
  implicit lazy val v2ErrorResponseSchema: JsonSchema[V2ErrorResponse] = genericRecord[V2ErrorResponse]
}

trait V2QuerySchemas
    extends endpoints4s.generic.JsonSchemas
    with V2SuccessResponseSchema
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
      case Left(eitherV2ErrorResponse) => Invalid(eitherV2ErrorResponse.merge.errors.map(_.message))
      case Right(errorMsg) => Invalid(Seq(errorMsg))
    }

  def convertClientErrorsToV2ErrorResponse(
    clientErrors: ClientErrors,
  ): Either[Either[V2ErrorResponse, V2ErrorResponse], String] =
    clientErrors match {
      case Invalid(errors) =>
        Left(Left(V2ErrorResponse(errors.map(message => V2Error(message = message, `type` = "CypherError")))))
    }

  def errorResponses(): Response[ClientErrors] =
    response(statusCode = BadRequest, entity = jsonResponse[V2ErrorResponse])
      .orElse(response(statusCode = Unauthorized, entity = jsonResponse[V2ErrorResponse]))
      .orElse(response(statusCode = InternalServerError, entity = textResponse))
      .xmap[ClientErrors](convertV2ErrorResponseToClientErrors)(convertClientErrorsToV2ErrorResponse)
}

trait V2QueryUiRoutes extends QueryUiRoutes with V2QuerySchemas with ErrorResponses with StatusCodes {
  final protected val v2Query: Path[Unit] = path / "api" / "v2" / "cypher-queries"

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
        entity = jsonOrYamlRequest[CypherQuery]
          .orElse(textRequest)
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = errorResponses()
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[CypherQueryResult]],
            ).xmap(response => response.content)(result => V2SuccessResponse(result)),
          ),
        ),
    )

  val cypherNodesPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[Seq[UiNode[Id]]]]] =
    endpoint(
      request = post(
        url = v2Query / "query-nodes" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequest[CypherQuery]
          .orElse(textRequest)
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = errorResponses()
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[Seq[V2UiNode]]],
            ).xmap(response => response.content.map(convertV2NodeToV1))(nodes =>
              V2SuccessResponse(nodes.map(n => V2UiNode(n.id.toString, n.hostIndex, n.label, n.properties))),
            ),
          ),
        ),
    )

  val cypherEdgesPostV2: Endpoint[V2QueryInputs[CypherQuery], Either[ClientErrors, Option[Seq[UiEdge[Id]]]]] =
    endpoint(
      request = post(
        url = v2Query / "query-edges" /? (atTime & reqTimeout & namespace),
        entity = jsonOrYamlRequest[CypherQuery]
          .orElse(textRequest)
          .xmap[CypherQuery](_.map(CypherQuery(_)).merge)(cq => if (cq.parameters.isEmpty) Right(cq.text) else Left(cq)),
      ),
      response = errorResponses()
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[Seq[V2UiEdge]]],
            ).xmap(response => response.content.map(convertV2EdgeToV1))(edges =>
              V2SuccessResponse(edges.map(e => V2UiEdge(e.from.toString, e.edgeType, e.to.toString, e.isDirected))),
            ),
          ),
        ),
    )
}

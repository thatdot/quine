package com.thatdot.quine.app.v2api.endpoints

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import endpoints4s.generic.title
import io.circe.generic.auto._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.description
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe.TapirJsonCirce
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Codec, DecodeResult, Schema, oneOfBody}

import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{TCypherQuery, cypherQueryAsStringCodec}
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.routes.{CypherQuery, CypherQueryResult, UiEdge, UiNode}

object V2CypherEndpointEntities extends TapirJsonCirce {
  @title("Cypher Query")
  final case class TCypherQuery(
    @description("Text of the query to execute") text: String,
    @description("Parameters the query expects, if any") parameters: Map[String, Json] = Map.empty,
  )
  implicit val cypherQuerySchema: Schema[TCypherQuery] = Schema
    .derived[TCypherQuery]
    .encodedExample(
      TCypherQuery(
        "MATCH (n) RETURN n LIMIT $lim",
        Map("lim" -> Json.fromInt(1)),
      ).asJson,
    )

  val cypherQueryAsStringCodec: Codec[String, TCypherQuery, TextPlain] =
    Codec.string.mapDecode(s => DecodeResult.Value(TCypherQuery(s)))(_.text)

  implicit lazy val mapSchema: Schema[Map[String, Json]] = Schema
    .schemaForMap[String, Json](identity)

}
trait V2CypherEndpoints extends V2QuineEndpointDefinitions {

  implicit val cypherQueryResultDecoder: Decoder[CypherQueryResult] = deriveDecoder[CypherQueryResult]
  implicit val cypherQueryResultEncoder: Encoder[CypherQueryResult] = deriveEncoder[CypherQueryResult]
  implicit val cypherQueryEncoder: Encoder[TCypherQuery] = deriveEncoder[TCypherQuery]
  implicit val cypherQueryDecoder: Decoder[TCypherQuery] = deriveDecoder[TCypherQuery]
  implicit val quineIdNodeEncoder: Encoder[UiNode[QuineId]] = deriveEncoder[UiNode[QuineId]]
  implicit val quineIdNodeDecoder: Decoder[UiNode[QuineId]] = deriveDecoder[UiNode[QuineId]]
  implicit val quineIdEdgeEncoder: Encoder[UiEdge[QuineId]] = deriveEncoder[UiEdge[QuineId]]
  implicit val quineIdEdgeDecoder: Decoder[UiEdge[QuineId]] = deriveDecoder[UiEdge[QuineId]]
  private val cypherLanguageUrl = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"

  /** SQ Base path */
  private def cypherQueryEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ) = baseEndpoint[T]("query", "cypher").tag("Cypher Query Language")

  private def queryBody =
    oneOfBody[TCypherQuery](jsonBody[TCypherQuery], yamlBody[TCypherQuery](), textBody(cypherQueryAsStringCodec))
  // TODO timeout duration: Temporary!
  private def toConcreteDuration(duration: Option[FiniteDuration]): FiniteDuration =
    duration.getOrElse(FiniteDuration.apply(20, TimeUnit.SECONDS)) //akka default http timeout

  private val cypherEndpoint =
    cypherQueryEndpoint[CypherQueryResult]
      .name("Cypher Query")
      .description(s"Execute an arbitrary [Cypher]($cypherLanguageUrl) query")
      .in(atTimeParameter)
      .in(timeoutParameter)
      .in(namespaceParameter)
      .in(queryBody)
      .post
      .serverLogic { case (memberIdx, atTime, timeout, namespace, query) =>
        runServerLogicWithError[(Option[AtTime], Option[FiniteDuration], NamespaceId, CypherQuery), CypherQueryResult](
          CypherPostApiCmd,
          memberIdx,
          (atTime, timeout, namespaceFromParam(namespace), CypherQuery(query.text, query.parameters)),
          t => appMethods.cypherPost(t._1, toConcreteDuration(t._2), t._3, t._4),
        )
      }

  private val cypherNodesEndpoint = cypherQueryEndpoint[Seq[UiNode[QuineId]]]
    .name("Cypher Query Return Nodes")
    .description(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns nodes.
                      |Queries that do not return nodes will fail with a type error.""".stripMargin)
    .in("nodes")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .serverLogic { case (memberIdx, atTime, timeout, namespace, query) =>
      runServerLogicWithError[(Option[AtTime], Option[FiniteDuration], NamespaceId, CypherQuery), Seq[UiNode[QuineId]]](
        CypherNodesPostApiCmd,
        memberIdx,
        (atTime, timeout, namespaceFromParam(namespace), CypherQuery(query.text, query.parameters)),
        t => appMethods.cypherNodesPost(t._1, toConcreteDuration(t._2), t._3, t._4),
      )
    }

  private val cypherEdgesEndpoint = cypherQueryEndpoint[Seq[UiEdge[QuineId]]]
    .name("Cypher Query Return Edges")
    .description(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns edges.
         |Queries that do not return edges will fail with a type error.""".stripMargin)
    .in("edges")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .serverLogic { case (memberIdx, atTime, timeout, namespace, query) =>
      runServerLogicWithError[(Option[AtTime], Option[FiniteDuration], NamespaceId, CypherQuery), Seq[UiEdge[QuineId]]](
        CypherEdgesPostApiCmd,
        memberIdx,
        (atTime, timeout, namespaceFromParam(namespace), CypherQuery(query.text, query.parameters)),
        t => appMethods.cypherEdgesPost(t._1, toConcreteDuration(t._2), t._3, t._4),
      )
    }

  val cypherEndpoints: List[ServerEndpoint[Any, Future]] = List(
    cypherEndpoint,
    cypherNodesEndpoint,
    cypherEdgesEndpoint,
  )
}

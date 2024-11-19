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
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
  cypherQueryAsStringCodec,
}
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineId

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

  @title("Cypher Query Result")
  @description("""Cypher queries are designed to return data in a table format. This gets
      |encoded into JSON with `columns` as the header row and each element in `results`
      |being another row of results. As a consequence Consequently, every array element
      |in `results` will have the same length, and all will have the same length as the
      |`columns` array.
      |""".stripMargin)
  case class TCypherQueryResult(
    @description("Return values of the Cypher query") columns: Seq[String],
    @description("Rows of results") results: Seq[Seq[Json]],
  )

  case class TUiNode(id: QuineId, hostIndex: Int, label: String, properties: Map[String, Json])

  case class TUiEdge(from: QuineId, edgeType: String, to: QuineId, isDirected: Boolean = true)
}
trait V2CypherEndpoints extends V2QuineEndpointDefinitions {

  implicit val cypherQueryResultDecoder: Decoder[TCypherQueryResult] = deriveDecoder[TCypherQueryResult]
  implicit val cypherQueryResultEncoder: Encoder[TCypherQueryResult] = deriveEncoder[TCypherQueryResult]
  implicit val cypherQueryEncoder: Encoder[TCypherQuery] = deriveEncoder[TCypherQuery]
  implicit val cypherQueryDecoder: Decoder[TCypherQuery] = deriveDecoder[TCypherQuery]
  implicit val quineIdNodeEncoder: Encoder[TUiNode] = deriveEncoder[TUiNode]
  implicit val quineIdNodeDecoder: Decoder[TUiNode] = deriveDecoder[TUiNode]
  implicit val quineIdEdgeEncoder: Encoder[TUiEdge] = deriveEncoder[TUiEdge]
  implicit val quineIdEdgeDecoder: Decoder[TUiEdge] = deriveDecoder[TUiEdge]
  private val cypherLanguageUrl = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"

  /** SQ Base path */
  private def cypherQueryEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ) = baseEndpoint[T]("cypher-queries").tag("Cypher Query Language")

  private def queryBody =
    oneOfBody[TCypherQuery](jsonBody[TCypherQuery], yamlBody[TCypherQuery](), textBody(cypherQueryAsStringCodec))
  // TODO timeout duration: Temporary!
  private def toConcreteDuration(duration: Option[FiniteDuration]): FiniteDuration =
    duration.getOrElse(FiniteDuration.apply(20, TimeUnit.SECONDS)) //akka default http timeout

  private val cypherEndpoint =
    cypherQueryEndpoint[TCypherQueryResult]
      .name("Cypher Query")
      .description(s"Execute an arbitrary [Cypher]($cypherLanguageUrl) query")
      .in("query-graph")
      .in(atTimeParameter)
      .in(timeoutParameter)
      .in(namespaceParameter)
      .in(queryBody)
      .post
      .serverLogic { case (memberIdx, atTime, timeout, namespace, query) =>
        runServerLogicFromEither[
          (Option[AtTime], Option[FiniteDuration], NamespaceId, TCypherQuery),
          TCypherQueryResult,
        ](
          CypherPostApiCmd,
          memberIdx,
          (atTime, timeout, namespaceFromParam(namespace), TCypherQuery(query.text, query.parameters)),
          t => appMethods.cypherPost(t._1, toConcreteDuration(t._2), t._3, t._4),
        )
      }

  private val cypherNodesEndpoint = cypherQueryEndpoint[Seq[TUiNode]]
    .name("Cypher Query Return Nodes")
    .description(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns nodes.
                      |Queries that do not return nodes will fail with a type error.""".stripMargin)
    .in("query-nodes")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .serverLogic { case (memberIdx, atTime, timeout, namespace, query) =>
      runServerLogicFromEither[(Option[AtTime], Option[FiniteDuration], NamespaceId, TCypherQuery), Seq[
        TUiNode,
      ]](
        CypherNodesPostApiCmd,
        memberIdx,
        (atTime, timeout, namespaceFromParam(namespace), TCypherQuery(query.text, query.parameters)),
        t => appMethods.cypherNodesPost(t._1, toConcreteDuration(t._2), t._3, t._4),
      )
    }

  private val cypherEdgesEndpoint = cypherQueryEndpoint[Seq[TUiEdge]]
    .name("Cypher Query Return Edges")
    .description(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns edges.
         |Queries that do not return edges will fail with a type error.""".stripMargin)
    .in("query-edges")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .serverLogic { case (memberIdx, atTime, timeout, namespace, query) =>
      runServerLogicFromEither[(Option[AtTime], Option[FiniteDuration], NamespaceId, TCypherQuery), Seq[
        TUiEdge,
      ]](
        CypherEdgesPostApiCmd,
        memberIdx,
        (atTime, timeout, namespaceFromParam(namespace), TCypherQuery(query.text, query.parameters)),
        t => appMethods.cypherEdgesPost(t._1, toConcreteDuration(t._2), t._3, t._4),
      )
    }

  val cypherEndpoints: List[ServerEndpoint[Any, Future]] = List(
    cypherEndpoint,
    cypherNodesEndpoint,
    cypherEdgesEndpoint,
  )
}

package com.thatdot.quine.app.v2api.endpoints

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import endpoints4s.generic.title
import io.circe.Json
import io.circe.generic.extras.auto._
import io.circe.syntax.EncoderOps
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.description
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe.TapirJsonCirce
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Codec, DecodeResult, Schema, oneOfBody, statusCode}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}

object V2CypherEndpointEntities extends TapirJsonCirce {
  @title("Cypher Query")
  final case class TCypherQuery(
    @description("Text of the query to execute") text: String,
    @description("Parameters the query expects, if any") parameters: Map[String, Json] = Map.empty,
  )

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
trait V2CypherEndpoints extends V2QuineEndpointDefinitions with V2ApiConfiguration {

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

  private val cypherLanguageUrl = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"

  /** SQ Base path */
  private def cypherQueryEndpoint =
    rawEndpoint("cypher-queries").tag("Cypher Query Language")

  private val textEx = TCypherQuery(
    "MATCH (n) RETURN n LIMIT $lim",
    Map("lim" -> Json.fromInt(1)),
  )

  private def queryBody =
    oneOfBody[TCypherQuery](
      jsonBody[TCypherQuery].example(textEx),
      yamlBody[TCypherQuery]().example(textEx),
      textBody(cypherQueryAsStringCodec).example(textEx),
    )
  // TODO timeout duration: Temporary!
  private def toConcreteDuration(duration: Option[FiniteDuration]): FiniteDuration =
    duration.getOrElse(FiniteDuration.apply(20, TimeUnit.SECONDS)) //akka default http timeout

  private val cypherEndpoint =
    cypherQueryEndpoint
      .name("Cypher Query")
      .description(s"Execute an arbitrary [Cypher]($cypherLanguageUrl) query")
      .in("query-graph")
      .in(atTimeParameter)
      .in(timeoutParameter)
      .in(namespaceParameter)
      .in(queryBody)
      .post
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[TCypherQueryResult]])
      .serverLogic { case (atTime, timeout, namespace, query) =>
        runServerLogicFromEitherOk(
          appMethods
            .cypherPost(
              atTime,
              toConcreteDuration(timeout),
              namespaceFromParam(namespace),
              TCypherQuery(query.text, query.parameters),
            )
            .map(f => f.left.map(ErrorEnvelope.apply))(ExecutionContext.parasitic),
        )((inp: TCypherQueryResult) => SuccessEnvelope.Ok.apply(inp))
      }

  private val cypherNodesEndpoint = cypherQueryEndpoint
    .name("Cypher Query Return Nodes")
    .description(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns nodes.
                      |Queries that do not return nodes will fail with a type error.""".stripMargin)
    .in("query-nodes")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Seq[TUiNode]]])
    .serverLogic { case (atTime, timeout, namespace, query) =>
      runServerLogicFromEitherOk(
        appMethods
          .cypherNodesPost(atTime, toConcreteDuration(timeout), namespaceFromParam(namespace), query)
          .map(f => f.left.map(ErrorEnvelope.apply))(ExecutionContext.parasitic),
      )((inp: Seq[TUiNode]) => SuccessEnvelope.Ok.apply(inp))
    }

  private val cypherEdgesEndpoint = cypherQueryEndpoint
    .name("Cypher Query Return Edges")
    .description(s"""Execute a [Cypher]($cypherLanguageUrl) query that returns edges.
         |Queries that do not return edges will fail with a type error.""".stripMargin)
    .in("query-edges")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Seq[TUiEdge]]])
    .serverLogic { case (atTime, timeout, namespace, query) =>
      runServerLogicFromEitherOk(
        appMethods
          .cypherEdgesPost(
            atTime,
            toConcreteDuration(timeout),
            namespaceFromParam(namespace),
            TCypherQuery(query.text, query.parameters),
          )
          .map(f => f.left.map(ErrorEnvelope.apply))(ExecutionContext.parasitic),
      )((inp: Seq[TUiEdge]) => SuccessEnvelope.Ok.apply(inp))
    }

  val cypherEndpoints: List[ServerEndpoint[Any, Future]] = List(
    cypherEndpoint,
    cypherNodesEndpoint,
    cypherEdgesEndpoint,
  )
}

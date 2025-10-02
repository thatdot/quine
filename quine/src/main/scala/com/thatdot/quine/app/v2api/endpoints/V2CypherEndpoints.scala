package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import endpoints4s.generic.title
import io.circe.generic.extras.auto._
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.description
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Codec, DecodeResult, Endpoint, EndpointInput, Schema, oneOfBody, statusCode}

import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, serverError}
import com.thatdot.api.v2.{ErrorResponse, SuccessEnvelope, V2EndpointDefinitions}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}
import com.thatdot.quine.model.Milliseconds

object V2CypherEndpointEntities {
  import StringOps.syntax._

  @title("Cypher Query")
  final case class TCypherQuery(
    @description("Text of the query to execute.") text: String,
    @description("Parameters the query expects, if any.") parameters: Map[String, Json] = Map.empty,
  )

  @title("Cypher Query Result")
  @description(
    """Cypher queries are designed to return data in a table format.
      |This gets encoded into JSON with `columns` as the header row and each element in `results` being another row
      |of results. Consequently, every array element in `results` will have the same length, and all will have the
      |same length as the `columns` array.""".asOneLine,
  )
  case class TCypherQueryResult(
    @description("Return values of the Cypher query.") columns: Seq[String],
    @description("Rows of results.") results: Seq[Seq[Json]],
  )

  case class TUiNode(id: QuineId, hostIndex: Int, label: String, properties: Map[String, Json])

  case class TUiEdge(from: QuineId, edgeType: String, to: QuineId, isDirected: Boolean = true)
}

trait V2CypherEndpoints extends V2EndpointDefinitions with V2IngestApiSchemas with CommonParameters with StringOps { // V2EndpointDefinitions with V2ApiSchemas
  val appMethods: ApplicationApiMethods with CypherApiMethods

  import com.thatdot.api.v2.schema.V2ApiSchemas._

  def namespaceParameter: EndpointInput[Option[String]]
  def memberIdxParameter: EndpointInput[Option[Int]]

  // QuineId JSON serialization support for UI APIv2 handling
  implicit val quineIdEncoder: Encoder[QuineId] = Encoder.encodeString.contramap(idProvider.qidToPrettyString)
  implicit val quineIdDecoder: Decoder[QuineId] = Decoder.decodeString.emap { str =>
    idProvider.qidFromPrettyString(str).toEither.left.map(_.getMessage)
  }
  implicit val quineIdSchema: Schema[QuineId] = Schema.string[QuineId]
  implicit val tuiNodeSchema: Schema[TUiNode] = Schema.derived[TUiNode]
  implicit val tuiEdgeSchema: Schema[TUiEdge] = Schema.derived[TUiEdge]

  implicit val cypherQuerySchema: Schema[TCypherQuery] = Schema
    .derived[TCypherQuery]
    .encodedExample(
      TCypherQuery(
        "MATCH (n) RETURN n LIMIT $lim",
        Map("lim" -> Json.fromInt(1)),
      ).asJson,
    )

  private val cypherQueryAsStringCodec: Codec[String, TCypherQuery, TextPlain] =
    Codec.string.mapDecode(s => DecodeResult.Value(TCypherQuery(s)))(_.text)

  implicit lazy val mapSchema: Schema[Map[String, Json]] = Schema
    .schemaForMap[String, Json](identity)

  private val cypherLanguageUrl = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"

  /** SQ Base path */
  protected[endpoints] val cypherQueryBase: EndpointBase = rawEndpoint("cypher-queries")
    .tag("Cypher Query Language")
    .errorOut(serverError())

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

  val cypher: Endpoint[
    Unit,
    (Option[AtTime], FiniteDuration, Option[String], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[TCypherQueryResult],
    Any,
  ] = cypherQueryBase
    .name("Cypher Query")
    .description(s"Execute an arbitrary [Cypher]($cypherLanguageUrl) query.")
    .in("query-graph")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .errorOutEither(badRequestError("Invalid Query"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[TCypherQueryResult]])

  private val cypherLogic: ((Option[AtTime], FiniteDuration, Option[String], TCypherQuery)) => Future[
    Either[Either[ErrorResponse.ServerError, ErrorResponse.BadRequest], SuccessEnvelope.Ok[TCypherQueryResult]],
  ] = { case (atTime, timeout, namespace, query) =>
    recoverServerErrorEitherFlat(
      appMethods
        .cypherPost(
          atTime,
          timeout,
          namespaceFromParam(namespace),
          TCypherQuery(query.text, query.parameters),
        ),
    )((inp: TCypherQueryResult) => SuccessEnvelope.Ok.apply(inp))
  }

  private val cypherServerEndpoint: Full[
    Unit,
    Unit,
    (Option[AtTime], FiniteDuration, Option[String], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[TCypherQueryResult],
    Any,
    Future,
  ] = cypher.serverLogic[Future](cypherLogic)

  val cypherNodes: Endpoint[
    Unit,
    (Option[AtTime], FiniteDuration, Option[String], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[Seq[TUiNode]],
    Any,
  ] = cypherQueryBase
    .name("Cypher Query Return Nodes")
    .description(
      s"""Execute a [Cypher]($cypherLanguageUrl) query that returns nodes.
        |Queries that do not return nodes will fail with a type error.""".asOneLine,
    )
    .in("query-nodes")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .errorOutEither(badRequestError("Invalid Query"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Seq[TUiNode]]])

  val cypherNodesLogic: (
    (
      Option[Milliseconds],
      FiniteDuration,
      Option[String],
      TCypherQuery,
    ),
  ) => Future[Either[
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[Seq[TUiNode]],
  ]] = { case (atTime, timeout, namespace, query) =>
    recoverServerErrorEitherFlat(
      appMethods
        .cypherNodesPost(atTime, timeout, namespaceFromParam(namespace), query),
    )((inp: Seq[TUiNode]) => SuccessEnvelope.Ok.apply(inp))
  }

  private val cypherNodesServerEndpoint: Full[
    Unit,
    Unit,
    (Option[Milliseconds], FiniteDuration, Option[String], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[Seq[TUiNode]],
    Any,
    Future,
  ] = cypherNodes.serverLogic[Future](cypherNodesLogic)

  val cypherEdges: Endpoint[
    Unit,
    (Option[AtTime], FiniteDuration, Option[String], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[Seq[TUiEdge]],
    Any,
  ] = cypherQueryBase
    .name("Cypher Query Return Edges")
    .description(
      s"""Execute a [Cypher]($cypherLanguageUrl) query that returns edges.
         |Queries that do not return edges will fail with a type error.""".asOneLine,
    )
    .in("query-edges")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .errorOutEither(badRequestError("Invalid Query"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Seq[TUiEdge]]])

  val cypherEdgesLogic: ((Option[AtTime], FiniteDuration, Option[String], TCypherQuery)) => Future[
    Either[Either[ErrorResponse.ServerError, ErrorResponse.BadRequest], SuccessEnvelope.Ok[Seq[TUiEdge]]],
  ] = { case (atTime, timeout, namespace, query) =>
    recoverServerErrorEitherFlat(
      appMethods
        .cypherEdgesPost(
          atTime,
          timeout,
          namespaceFromParam(namespace),
          TCypherQuery(query.text, query.parameters),
        ),
    )((inp: Seq[TUiEdge]) => SuccessEnvelope.Ok.apply(inp))
  }

  private val cypherEdgesServerEndpoint: Full[
    Unit,
    Unit,
    (Option[Milliseconds], FiniteDuration, Option[String], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[Seq[TUiEdge]],
    Any,
    Future,
  ] = cypherEdges.serverLogic[Future](cypherEdgesLogic)

  val cypherEndpoints: List[ServerEndpoint[Any, Future]] = List(
    cypherServerEndpoint,
    cypherNodesServerEndpoint,
    cypherEdgesServerEndpoint,
  )
}

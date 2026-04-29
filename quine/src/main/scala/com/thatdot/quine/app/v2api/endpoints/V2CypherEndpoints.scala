package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import endpoints4s.generic.title
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.Schema.annotations.description
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Codec, DecodeResult, Endpoint, EndpointInput, Schema, oneOfBody, statusCode}

import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, serverError}
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.api.v2.schema.ThirdPartySchemas.circe.{mapStringJsonSchema, seqSeqJsonSchema}
import com.thatdot.api.v2.{ErrorResponse, V2EndpointDefinitions}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.routes.exts.NamespaceParameter

object V2CypherEndpointEntities {
  import StringOps.syntax._

  @title("Cypher Query")
  final case class TCypherQuery(
    @description("Text of the query to execute.") text: String,
    @description("Parameters the query expects, if any.") parameters: Map[String, Json] = Map.empty,
  )
  object TCypherQuery {
    implicit val encoder: Encoder[TCypherQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[TCypherQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[TCypherQuery] = Schema
      .derived[TCypherQuery]
      .encodedExample(
        TCypherQuery(
          "MATCH (n) RETURN n LIMIT $lim",
          Map("lim" -> Json.fromInt(1)),
        ).asJson,
      )
  }

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
  object TCypherQueryResult {
    implicit val encoder: Encoder[TCypherQueryResult] = deriveConfiguredEncoder
    implicit val decoder: Decoder[TCypherQueryResult] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[TCypherQueryResult] = Schema.derived
  }

  case class TUiNode(id: QuineId, hostIndex: Int, label: String, properties: Map[String, Json])
  object TUiNode extends QuineIdSchemas {
    implicit def encoder(implicit quineIdEncoder: Encoder[QuineId]): Encoder[TUiNode] = deriveConfiguredEncoder
    implicit def decoder(implicit quineIdDecoder: Decoder[QuineId]): Decoder[TUiNode] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[TUiNode] = Schema.derived
  }

  case class TUiEdge(from: QuineId, edgeType: String, to: QuineId, isDirected: Boolean = true)
  object TUiEdge extends QuineIdSchemas {
    implicit def encoder(implicit quineIdEncoder: Encoder[QuineId]): Encoder[TUiEdge] = deriveConfiguredEncoder
    implicit def decoder(implicit quineIdDecoder: Decoder[QuineId]): Decoder[TUiEdge] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[TUiEdge] = Schema.derived
  }
}

trait V2CypherEndpoints extends V2EndpointDefinitions with QuineIdCodec with CommonParameters with StringOps {
  val appMethods: ApplicationApiMethods with CypherApiMethods
  val idProvider: QuineIdProvider

  def namespaceParameter: EndpointInput[Option[NamespaceParameter]]
  def memberIdxParameter: EndpointInput[Option[Int]]

  private val cypherQueryAsStringCodec: Codec[String, TCypherQuery, TextPlain] =
    Codec.string.mapDecode(s => DecodeResult.Value(TCypherQuery(s)))(_.text)

  private val cypherLanguageUrl = "https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf"

  /** Build a base endpoint for a `cypher:<verb>` AIP-136 custom method. */
  protected[endpoints] def cypherColonVerb(verb: String): EndpointBase =
    rawEndpoint(s"cypher:$verb")
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
    (Option[AtTime], FiniteDuration, Option[NamespaceParameter], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    TCypherQueryResult,
    Any,
  ] = cypherColonVerb("query")
    .name("query-cypher")
    .summary("Cypher Query")
    .description(s"Execute an arbitrary [Cypher]($cypherLanguageUrl) query.")
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .errorOutEither(badRequestError("Invalid Query"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[TCypherQueryResult])

  private val cypherLogic: ((Option[AtTime], FiniteDuration, Option[NamespaceParameter], TCypherQuery)) => Future[
    Either[Either[ErrorResponse.ServerError, ErrorResponse.BadRequest], TCypherQueryResult],
  ] = { case (atTime, timeout, namespace, query) =>
    recoverServerErrorEitherFlat(
      appMethods
        .cypherPost(
          atTime,
          timeout,
          namespaceFromParam(namespace),
          TCypherQuery(query.text, query.parameters),
        ),
    )((inp: TCypherQueryResult) => identity(inp))
  }

  private val cypherServerEndpoint: Full[
    Unit,
    Unit,
    (Option[AtTime], FiniteDuration, Option[NamespaceParameter], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    TCypherQueryResult,
    Any,
    Future,
  ] = cypher.serverLogic[Future](cypherLogic)

  val cypherNodes: Endpoint[
    Unit,
    (Option[AtTime], FiniteDuration, Option[NamespaceParameter], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    Seq[TUiNode],
    Any,
  ] = cypherColonVerb("queryNodes")
    .name("query-cypher-nodes")
    .summary("Cypher Query Return Nodes")
    .description(
      s"""Execute a [Cypher]($cypherLanguageUrl) query that returns nodes.
        |Queries that do not return nodes will fail with a type error.""".asOneLine,
    )
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .errorOutEither(badRequestError("Invalid Query"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[Seq[TUiNode]])

  val cypherNodesLogic: (
    (
      Option[Milliseconds],
      FiniteDuration,
      Option[NamespaceParameter],
      TCypherQuery,
    ),
  ) => Future[Either[
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    Seq[TUiNode],
  ]] = { case (atTime, timeout, namespace, query) =>
    recoverServerErrorEitherFlat(
      appMethods
        .cypherNodesPost(atTime, timeout, namespaceFromParam(namespace), query),
    )((inp: Seq[TUiNode]) => identity(inp))
  }

  private val cypherNodesServerEndpoint: Full[
    Unit,
    Unit,
    (Option[Milliseconds], FiniteDuration, Option[NamespaceParameter], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    Seq[TUiNode],
    Any,
    Future,
  ] = cypherNodes.serverLogic[Future](cypherNodesLogic)

  val cypherEdges: Endpoint[
    Unit,
    (Option[AtTime], FiniteDuration, Option[NamespaceParameter], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    Seq[TUiEdge],
    Any,
  ] = cypherColonVerb("queryEdges")
    .name("query-cypher-edges")
    .summary("Cypher Query Return Edges")
    .description(
      s"""Execute a [Cypher]($cypherLanguageUrl) query that returns edges.
         |Queries that do not return edges will fail with a type error.""".asOneLine,
    )
    .in(atTimeParameter)
    .in(timeoutParameter)
    .in(namespaceParameter)
    .in(queryBody)
    .post
    .errorOutEither(badRequestError("Invalid Query"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[Seq[TUiEdge]])

  val cypherEdgesLogic: ((Option[AtTime], FiniteDuration, Option[NamespaceParameter], TCypherQuery)) => Future[
    Either[Either[ErrorResponse.ServerError, ErrorResponse.BadRequest], Seq[TUiEdge]],
  ] = { case (atTime, timeout, namespace, query) =>
    recoverServerErrorEitherFlat(
      appMethods
        .cypherEdgesPost(
          atTime,
          timeout,
          namespaceFromParam(namespace),
          TCypherQuery(query.text, query.parameters),
        ),
    )((inp: Seq[TUiEdge]) => identity(inp))
  }

  private val cypherEdgesServerEndpoint: Full[
    Unit,
    Unit,
    (Option[Milliseconds], FiniteDuration, Option[NamespaceParameter], TCypherQuery),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    Seq[TUiEdge],
    Any,
    Future,
  ] = cypherEdges.serverLogic[Future](cypherEdgesLogic)

  val cypherEndpoints: List[ServerEndpoint[Any, Future]] = List(
    cypherServerEndpoint,
    cypherNodesServerEndpoint,
    cypherEdgesServerEndpoint,
  )
}

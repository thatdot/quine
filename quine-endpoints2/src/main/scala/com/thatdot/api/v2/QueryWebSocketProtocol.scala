package com.thatdot.api.v2

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.{Schema, Validator}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig

/** V2 WebSocket query protocol messages.
  *
  * The wire format uses flat JSON objects with a `"type"` discriminator field:
  * {{{
  * {"type": "RunQuery", "queryId": 0, "query": "MATCH (n) RETURN n", "sort": "Node", "interpreter": "Cypher"}
  * {"type": "QueryStarted", "queryId": 0, "isReadOnly": true, "canContainAllNodeScan": false}
  * {"type": "NodeResults", "queryId": 0, "results": [...]}
  * {"type": "QueryFinished", "queryId": 0}
  * }}}
  */
object QueryWebSocketProtocol {

  // ---- Enums ----

  /** The query language used to write a query. */
  sealed abstract class QueryLanguage

  /** The compiler/interpreter backend used to execute a query.
    *
    * A single [[QueryLanguage]] may support multiple interpreters. For example, Cypher queries can be executed by the
    * default Cypher interpreter or by the QuinePattern interpreter.
    */
  sealed abstract class QueryInterpreter

  object QueryLanguage {
    case object Cypher extends QueryLanguage

    implicit val encoder: Encoder[QueryLanguage] = Encoder.encodeString.contramap { case Cypher => "Cypher" }
    implicit val decoder: Decoder[QueryLanguage] = Decoder.decodeString.emap {
      case "Cypher" => Right(Cypher)
      case other => Left(s"Unknown query language: $other")
    }
  }

  object QueryInterpreter {

    /** The default Cypher interpreter. */
    case object Cypher extends QueryInterpreter

    /** The QuinePattern compiler and interpreter, supporting both ad-hoc and standing queries.
      * Not yet released; still accepted by the encoder/decoder for internal use, but intentionally
      * excluded from the OpenAPI schema below.
      */
    case object QuinePattern extends QueryInterpreter

    implicit val encoder: Encoder[QueryInterpreter] = Encoder.encodeString.contramap {
      case Cypher => "Cypher"
      case QuinePattern => "QuinePattern"
    }
    implicit val decoder: Decoder[QueryInterpreter] = Decoder.decodeString.emap {
      case "Cypher" => Right(Cypher)
      case "QuinePattern" => Right(QuinePattern)
      case other => Left(s"Unknown query interpreter: $other")
    }
    implicit val schema: Schema[QueryInterpreter] =
      Schema.string[QueryInterpreter].validate(Validator.enumeration(List(Cypher), v => Some(v.toString)))
  }

  sealed abstract class QuerySort
  object QuerySort {
    case object Node extends QuerySort
    case object Edge extends QuerySort
    case object Text extends QuerySort

    implicit val encoder: Encoder[QuerySort] = Encoder.encodeString.contramap {
      case Node => "Node"
      case Edge => "Edge"
      case Text => "Text"
    }
    implicit val decoder: Decoder[QuerySort] = Decoder.decodeString.emap {
      case "Node" => Right(Node)
      case "Edge" => Right(Edge)
      case "Text" => Right(Text)
      case other => Left(s"Unknown query sort: $other")
    }
  }

  // ---- Graph element types ----

  /** A graph node as represented in query results.
    *
    * @param id string representation of the node ID
    * @param hostIndex index of the cluster host responsible for this node
    * @param label categorical classification of the node
    * @param properties key-value properties on the node
    */
  final case class UiNode(
    id: String,
    hostIndex: Int,
    label: String,
    properties: Map[String, Json],
  )
  object UiNode {
    implicit val encoder: Encoder[UiNode] = deriveConfiguredEncoder
    implicit val decoder: Decoder[UiNode] = deriveConfiguredDecoder
  }

  /** A graph edge as represented in query results.
    *
    * @param from node ID at the start of the edge
    * @param edgeType name/label of the edge
    * @param to node ID at the end of the edge
    * @param isDirected whether the edge is directed or undirected
    */
  final case class UiEdge(
    from: String,
    edgeType: String,
    to: String,
    isDirected: Boolean = true,
  )
  object UiEdge {
    implicit val encoder: Encoder[UiEdge] = deriveConfiguredEncoder
    implicit val decoder: Decoder[UiEdge] = deriveConfiguredDecoder
  }

  // ---- Client messages ----

  /** Messages sent from the client to the server over the WebSocket. */
  sealed abstract class ClientMessage

  /** Instruct the server to start running a query.
    *
    * @param queryId client-assigned ID used to refer to the query in subsequent messages
    * @param query raw source of the query
    * @param sort what type of results should the query produce
    * @param parameters constants bound in the query
    * @param language the query language (currently only Cypher)
    * @param interpreter which compiler/interpreter backend to use for execution
    * @param atTime optional historical timestamp to query against (epoch millis)
    * @param maxResultBatch max rows per result batch (`None` means no limit)
    * @param resultsWithinMillis max delay in ms between result batches (`None` means no delay)
    */
  final case class RunQuery(
    queryId: Int,
    query: String,
    sort: QuerySort,
    parameters: Map[String, Json] = Map.empty,
    language: QueryLanguage = QueryLanguage.Cypher,
    interpreter: QueryInterpreter = QueryInterpreter.Cypher,
    atTime: Option[Long] = None,
    maxResultBatch: Option[Int] = None,
    resultsWithinMillis: Option[Int] = None,
  ) extends ClientMessage

  /** Instruct the server to cancel a running query.
    *
    * @param queryId which query to cancel
    */
  final case class CancelQuery(
    queryId: Int,
  ) extends ClientMessage

  object ClientMessage {
    implicit val encoder: Encoder[ClientMessage] = deriveConfiguredEncoder
    implicit val decoder: Decoder[ClientMessage] = deriveConfiguredDecoder
  }

  // ---- Server messages ----

  /** Messages sent from the server to the client over the WebSocket.
    *
    * These are either direct responses to client requests (matched by message ordering) or asynchronous notifications
    * about running queries (identified by `queryId`).
    */
  sealed abstract class ServerMessage

  /** Direct response to a client request, matched by message ordering. */
  sealed abstract class ServerResponseMessage extends ServerMessage

  /** Asynchronous notification about a running query. */
  sealed abstract class ServerAsyncNotification extends ServerMessage {
    val queryId: Int
  }

  /** Error response to the most recent client request.
    *
    * @param error human-readable error description
    */
  final case class MessageError(
    error: String,
  ) extends ServerResponseMessage

  /** Success acknowledgement for a client request that has no other meaningful response. */
  case object MessageOk extends ServerResponseMessage

  /** Confirmation that a query has been accepted and started.
    *
    * @param queryId which query was started
    * @param isReadOnly whether the query is definitely free of side-effects
    * @param canContainAllNodeScan whether the query may trigger an all-node scan
    * @param columns column names for the result set, if applicable
    */
  final case class QueryStarted(
    queryId: Int,
    isReadOnly: Boolean,
    canContainAllNodeScan: Boolean,
    columns: Option[Seq[String]] = None,
  ) extends ServerResponseMessage

  /** A batch of tabular query results.
    *
    * @param queryId which query produced these results
    * @param columns column names
    * @param results rows of values, each row corresponding to `columns`
    */
  final case class TabularResults(
    queryId: Int,
    columns: Seq[String],
    results: Seq[Seq[Json]],
  ) extends ServerAsyncNotification

  /** A batch of node query results.
    *
    * @param queryId which query produced these results
    * @param results batch of graph nodes
    */
  final case class NodeResults(
    queryId: Int,
    results: Seq[UiNode],
  ) extends ServerAsyncNotification

  /** A batch of edge query results.
    *
    * @param queryId which query produced these results
    * @param results batch of graph edges
    */
  final case class EdgeResults(
    queryId: Int,
    results: Seq[UiEdge],
  ) extends ServerAsyncNotification

  /** Notification that a query has failed.
    *
    * @param queryId which query failed
    * @param message error description
    */
  final case class QueryFailed(
    queryId: Int,
    message: String,
  ) extends ServerAsyncNotification

  /** Notification that a query has finished producing results.
    *
    * @param queryId which query is done
    */
  final case class QueryFinished(
    queryId: Int,
  ) extends ServerAsyncNotification

  object ServerMessage {
    implicit val encoder: Encoder[ServerMessage] = deriveConfiguredEncoder
    implicit val decoder: Decoder[ServerMessage] = deriveConfiguredDecoder
  }
}

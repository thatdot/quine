package com.thatdot.quine.app.ingest2.source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest2.V2IngestEntities.QuineIngestConfiguration
import com.thatdot.quine.app.ingest2.V2IngestEntities.StreamingFormat.DropFormat
import com.thatdot.quine.app.util.AtLeastOnceCypherQuery
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.{CompiledQuery, Location}
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, cypher}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log.LogConfig

trait QuineIngestQuery {
  def apply(
    deserialized: cypher.Value,
  ): Future[Unit]
}

case class QuineValueIngestQuery(
  graph: CypherOpsGraph,
  query: CompiledQuery[Location.Anywhere],
  parameter: String,
  namespaceId: NamespaceId,
)(implicit logConfig: LogConfig)
    extends (cypher.Value => Future[Unit])
    with QuineIngestQuery {
  lazy val atLeastOnceQuery: AtLeastOnceCypherQuery =
    AtLeastOnceCypherQuery(query, parameter, "ingest-query")

  def apply(
    deserialized: cypher.Value,
  ): Future[Unit] =
    atLeastOnceQuery
      .stream(deserialized, namespaceId)(graph)
      .run()(graph.materializer)
      .map(_ => ())(ExecutionContext.parasitic)

}

case object QuineDropIngestQuery extends QuineIngestQuery {
  def apply(
    deserialized: cypher.Value,
  ): Future[Unit] = Future.successful(())

}

object QuineValueIngestQuery extends LazyLogging {

  def apply(config: QuineIngestConfiguration, graph: CypherOpsGraph, namespaceId: NamespaceId)(implicit
    logConfig: LogConfig,
  ): QuineIngestQuery = config.source.format match {
    case DropFormat => QuineDropIngestQuery
    case _ => QuineValueIngestQuery.build(graph, config.query, config.parameter, namespaceId).get
  }

  def apply(
    config: IngestStreamConfiguration, //v1
    graph: CypherOpsGraph,
    namespaceId: NamespaceId,
  )(implicit logConfig: LogConfig): QuineIngestQuery = {

    def fromStreamedRecordFormat(f: StreamedRecordFormat): QuineIngestQuery = f match {
      case StreamedRecordFormat.Drop => QuineDropIngestQuery
      case s: IngestQuery => QuineValueIngestQuery.build(graph, s.query, s.parameter, namespaceId).get
      case _ => throw new UnsupportedOperationException(s"Can't extract query and parameters from $f")
    }

    def fromFileIngestFormat(f: FileIngestFormat): QuineIngestQuery =
      QuineValueIngestQuery.build(graph, f.query, f.parameter, namespaceId).get

    config match {
      case k: KafkaIngest => fromStreamedRecordFormat(k.format)
      case k: KinesisIngest => fromStreamedRecordFormat(k.format)
      case s: ServerSentEventsIngest => fromStreamedRecordFormat(s.format)
      case s: SQSIngest => fromStreamedRecordFormat(s.format)
      case s: WebsocketSimpleStartupIngest => fromStreamedRecordFormat(s.format)
      case s: FileIngest => fromFileIngestFormat(s.format)
      case s: S3Ingest => fromFileIngestFormat(s.format)
      case s: StandardInputIngest => fromFileIngestFormat(s.format)
      case s: NumberIteratorIngest => fromFileIngestFormat(s.format)
      case _ => throw new UnsupportedOperationException(s"Can't extract ingest query from $config")
    }
  }

  def build(
    graph: CypherOpsGraph,
    query: String,
    parameter: String,
    namespaceId: NamespaceId,
  )(implicit logConfig: LogConfig): Try[QuineValueIngestQuery] =
    Try(compiler.cypher.compile(query, unfixedParameters = Seq(parameter))).map {
      compiled: CompiledQuery[Location.Anywhere] =>
        if (compiled.query.canContainAllNodeScan) {
          // TODO this should be lifted to an (overridable, see allowAllNodeScan in SQ outputs) API error
          logger.warn(
            "Cypher query may contain full node scan; for improved performance, re-write without full node scan. " +
            "The provided query was: " + compiled.queryText,
          )
        }
        if (!compiled.query.isIdempotent) {
          // TODO allow user to override this (see: allowAllNodeScan) and only retry when idempotency is asserted
          logger.warn(
            """Could not verify that the provided ingest query is idempotent. If timeouts occur, query
              |execution may be retried and duplicate data may be created.""".stripMargin.replace('\n', ' '),
          )
        }
        QuineValueIngestQuery(graph, compiled, parameter, namespaceId)
    }

}

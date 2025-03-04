package com.thatdot.quine.app.ingest.serialization

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.Sink

import com.codahale.metrics.Timer
import com.typesafe.config.ConfigFactory
import io.circe.jawn.CirceSupportParser

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.app.util.AtLeastOnceCypherQuery
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.quinepattern.{
  EagerQuinePatternQueryPlanner,
  QueryPlan,
  QueryTarget,
  QuinePatternInterpreter,
}
import com.thatdot.quine.graph.cypher.{CompiledQuery, Location, QueryContext}
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, cypher}

/** Describes formats that Quine can import
  * Deserialized type refers to the (nullable) type to be produced by invocations of this [[ImportFormat]]
  */
trait ImportFormat {

  /** Attempt to import raw data as a [[cypher.Value]]. This will get called for each value to be imported
    *
    * @param data the raw data to decode
    * @return A Success if and only if a [[cypher.Value]] can be produced from the provided data,
    *         otherwise, a Failure describing the error during deserialization. These Failures should never
    *         be fatal.
    */
  protected def importBytes(data: Array[Byte]): Try[cypher.Value]

  /** Defers to [[importBytes]] but also checks that input data can (probably) be safely sent via pekko clustered messaging.
    * This is checked based on [[ImportFormat.pekkoMessageSizeLimit]]
    *
    * @param data         byte payload
    * @param isSingleHost is the cluster just one host (in which case there is no risk of oversize payloads)
    * @return
    */
  final def importMessageSafeBytes(
    data: Array[Byte],
    isSingleHost: Boolean,
    deserializationTimer: Timer,
  ): Try[cypher.Value] =
    if (!isSingleHost && data.length > pekkoMessageSizeLimit)
      Failure(
        new Exception(
          s"Attempted to decode ${data.length} bytes, but records larger than $pekkoMessageSizeLimit bytes are prohibited.",
        ),
      )
    else {
      val timer = deserializationTimer.time()
      val deserialized = importBytes(data)
      deserialized.foreach(_ => timer.stop()) // only time successful deserializations
      deserialized
    }

  /** A description of the import format.
    */
  def label: String

  /** An estimated limit on record size (based on the pekko remote frame size with 15kb of headspace) */
  lazy val pekkoMessageSizeLimit: Long =
    ConfigFactory.load().getBytes("pekko.remote.artery.advanced.maximum-frame-size") - 15 * 1024

  def writeValueToGraph(
    graph: CypherOpsGraph,
    intoNamespace: NamespaceId,
    deserialized: cypher.Value,
  ): Future[Done]
}

class TestOnlyDrop extends ImportFormat {
  override val label = "TestOnlyDrop"

  override def importBytes(data: Array[Byte]): Try[cypher.Value] = Success(cypher.Expr.Null)
  override def writeValueToGraph(
    graph: CypherOpsGraph,
    intoNamespace: NamespaceId,
    deserialized: cypher.Value,
  ): Future[Done] = Future.successful(Done)
}

abstract class CypherImportFormat(query: String, parameter: String) extends ImportFormat with LazySafeLogging {

  override val label: String = "Cypher " + query
  implicit protected def logConfig: LogConfig

  // TODO: think about error handling of failed compilation
  val compiled: CompiledQuery[Location.Anywhere] = compiler.cypher.compile(query, unfixedParameters = Seq(parameter))
  lazy val atLeastOnceQuery: AtLeastOnceCypherQuery = AtLeastOnceCypherQuery(compiled, parameter, "ingest-query")

  if (compiled.query.canContainAllNodeScan) {
    // TODO this should be lifted to an (overridable, see allowAllNodeScan in SQ outputs) API error
    logger.warn(
      safe"Cypher query may contain full node scan; for improved performance, re-write without full node scan. " +
      compiled.queryText.fold(safe"")(q => safe"The provided query was: ${Safe(q)}"),
    )
  }
  if (!compiled.query.isIdempotent) {
    // TODO allow user to override this (see: allowAllNodeScan) and only retry when idempotency is asserted
    logger.warn(
      safe"""Could not verify that the provided ingest query is idempotent. If timeouts occur, query
            |execution may be retried and duplicate data may be created.""".cleanLines,
    )
  }
  def writeValueToGraph(
    graph: CypherOpsGraph,
    intoNamespace: NamespaceId,
    deserialized: cypher.Value,
  ): Future[Done] =
    atLeastOnceQuery
      .stream(deserialized, intoNamespace)(graph)
      .runWith(Sink.ignore)(graph.materializer)
}

/** An abstract implementation of the `ImportFormat` trait that allows importing
  * data into Quine graphs, utilizing the Quine Pattern query language.
  *
  * @constructor Creates a new instance of `QuinePatternImportFormat`.
  * @param query     the Quine Pattern query that defines how the data should be interpreted.
  * @param parameter the symbol in the query to be replaced with deserialized data during execution.
  *
  *                  This class processes a defined query using the Quine Pattern query pipeline,
  *                  which includes lexing, parsing, symbol analysis, and query planning. The resulting
  *                  `QueryPlan` is used for interpreting data and writing it into a Quine graph.
  *
  *                  The class checks the system property `qp.enabled` to ensure the Quine Pattern
  *                  functionality is enabled, throwing an error if not configured correctly.
  *
  *                  The `writeValueToGraph` method interprets the compiled query with the provided
  *                  deserialized data and writes it to the target namespace in the Quine graph.
  */
abstract class QuinePatternImportFormat(query: String, parameter: String) extends ImportFormat with LazySafeLogging {
  val maybeIsQPEnabled: Option[Boolean] = for {
    pv <- Option(System.getProperty("qp.enabled"))
    b <- pv.toBooleanOption
  } yield b

  maybeIsQPEnabled match {
    case Some(true) => ()
    case _ => sys.error("Quine pattern must be enabled using -Dqp.enabled=true to use this feature.")
  }

  override val label: String = "QuinePattern " + query
  implicit protected def logConfig: LogConfig

  import com.thatdot.language.phases.UpgradeModule._

  val compiled: QueryPlan = {
    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (_, result) = parser.process(query).value.run(LexerState(Nil)).value
    EagerQuinePatternQueryPlanner.generatePlan(result.get)
  }

  def writeValueToGraph(
    graph: CypherOpsGraph,
    intoNamespace: NamespaceId,
    deserialized: cypher.Value,
  ): Future[Done] = {

    // Typecast is required here because `ImportFormat` is hard coded
    // to existing Quine structures
    val hack = graph.asInstanceOf[QuinePatternOpsGraph]

    QuinePatternInterpreter
      .interpret(compiled, QueryTarget.None, intoNamespace, Map(Symbol(parameter) -> deserialized), QueryContext.empty)(
        hack,
      )
      .runWith(Sink.ignore)(graph.materializer)

  }
}

//"Drop Format" should not run a query but should still read from ...

class CypherJsonInputFormat(query: String, parameter: String)(implicit val logConfig: LogConfig)
    extends CypherImportFormat(query, parameter) {

  override def importBytes(data: Array[Byte]): Try[cypher.Value] =
    // deserialize bytes into JSON without going through string
    new CirceSupportParser(maxValueSize = None, allowDuplicateKeys = false)
      .parseFromByteArray(data)
      .map(cypher.Value.fromJson)

}

class QuinePatternJsonInputFormat(query: String, parameter: String)(implicit val logConfig: LogConfig)
    extends QuinePatternImportFormat(query, parameter) {
  override def importBytes(data: Array[Byte]): Try[cypher.Value] =
    new CirceSupportParser(maxValueSize = None, allowDuplicateKeys = false)
      .parseFromByteArray(data)
      .map(cypher.Value.fromJson)
}

class CypherStringInputFormat(query: String, parameter: String, charset: String)(implicit val logConfig: LogConfig)
    extends CypherImportFormat(query, parameter) {

  override def importBytes(arr: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Str(new String(arr, charset)))

}

class QuinePatternStringInputFormat(query: String, parameter: String, charset: String)(implicit
  val logConfig: LogConfig,
) extends QuinePatternImportFormat(query, parameter) {
  override protected def importBytes(data: Array[Byte]): Try[cypher.Value] = Success(
    cypher.Expr.Str(new String(data, charset)),
  )
}

class QuinePatternRawInputFormat(query: String, parameter: String)(implicit val logConfig: LogConfig)
    extends QuinePatternImportFormat(query, parameter) {
  override def importBytes(arr: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Bytes(arr, representsId = false))
}

class CypherRawInputFormat(query: String, parameter: String)(implicit val logConfig: LogConfig)
    extends CypherImportFormat(query, parameter) {

  override def importBytes(arr: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Bytes(arr, representsId = false))

}

class ProtobufInputFormat(query: String, parameter: String, parser: ProtobufParser)(implicit val logConfig: LogConfig)
    extends CypherImportFormat(query, parameter) {

  override protected def importBytes(data: Array[Byte]): Try[cypher.Value] = Try(parser.parseBytes(data))
}

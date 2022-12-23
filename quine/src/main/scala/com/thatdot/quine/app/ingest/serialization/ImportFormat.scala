package com.thatdot.quine.app.ingest.serialization

import java.net.URL

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import akka.Done
import akka.stream.scaladsl.Sink

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import ujson.circe.CirceJson

import com.thatdot.quine.app.util.AtLeastOnceCypherQuery
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.{CypherOpsGraph, cypher}

/** Describes formats that Quine can import
  * Deserialized type refers to the the (nullable) type to be produced by invocations of this [[ImportFormat]]
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

  /** Defers to [[importBytes]] but also checks that input data can (probably) be safely sent via akka clustered messaging.
    * This is checked based on [[ImportFormat.akkaMessageSizeLimit]]
    *
    * @param data         byte payload
    * @param isSingleHost is the cluster just one host (in which case there is no risk of oversize payloads)
    * @return
    */
  final def importMessageSafeBytes(data: Array[Byte], isSingleHost: Boolean): Try[cypher.Value] =
    if (!isSingleHost && data.length > akkaMessageSizeLimit)
      Failure(
        new Exception(
          s"Attempted to decode ${data.length} bytes, but records larger than $akkaMessageSizeLimit bytes are prohibited."
        )
      )
    else importBytes(data)

  /** A description of the import format.
    */
  def label: String

  /** An estimated limit on record size (based on the akka remote frame size with 15kb of headspace) */
  lazy val akkaMessageSizeLimit: Long =
    ConfigFactory.load().getBytes("akka.remote.artery.advanced.maximum-frame-size") - 15 * 1024

  def writeValueToGraph(
    graph: CypherOpsGraph,
    deserialized: cypher.Value
  ): Future[Done]
}

class TestOnlyDrop extends ImportFormat {
  override val label = "TestOnlyDrop"

  override def importBytes(data: Array[Byte]): Try[cypher.Value] = Success(cypher.Expr.Null)
  override def writeValueToGraph(
    graph: CypherOpsGraph,
    deserialized: cypher.Value
  ): Future[Done] = Future.successful(Done)

}

abstract class CypherImportFormat(query: String, parameter: String) extends ImportFormat with LazyLogging {

  override val label: String = "Cypher " + query

  // TODO: think about error handling of failed compilation
  val compiled: cypher.CompiledQuery = compiler.cypher.compile(query, unfixedParameters = Seq(parameter))
  lazy val atLeastOnceQuery: AtLeastOnceCypherQuery = AtLeastOnceCypherQuery(compiled, parameter, "ingest-query")

  if (compiled.query.canContainAllNodeScan) {
    // TODO this should be lifted to an (overrideable, see allowAllNodeScan in SQ outputs) API error
    logger.warn(
      "Cypher query may contain full node scan; for improved performance, re-write without full node scan. " +
      "The provided query was: " + compiled.queryText
    )
  }
  if (!compiled.query.isIdempotent) {
    // TODO allow user to override this (see: allowAllNodeScan) and only retry when idempotency is asserted
    logger.warn(
      """Could not verify that the provided ingest query is idempotent. If timeouts occur, query
        |execution may be retried and duplicate data may be created.""".stripMargin.replace('\n', ' ')
    )
  }
  def writeValueToGraph(
    graph: CypherOpsGraph,
    deserialized: cypher.Value
  ): Future[Done] =
    atLeastOnceQuery
      .stream(deserialized)(graph)
      .runWith(Sink.ignore)(graph.materializer)
}
//"Drop Format" should not run a query but should still read from ...

class CypherJsonInputFormat(query: String, parameter: String) extends CypherImportFormat(query, parameter) {

  override def importBytes(data: Array[Byte]): Try[cypher.Value] = Try {
    // deserialize bytes into JSON without going through string
    // val json: ujson.Value = ujson.read(data)
    //val json : Json = CirceJson(data)
    // cypher.Value.fromJson(ujson.read(data) )
    cypher.Value.fromCirceJson(CirceJson(data))
  }

}

class CypherStringInputFormat(query: String, parameter: String, charset: String)
    extends CypherImportFormat(query, parameter) {

  override def importBytes(arr: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Str(new String(arr, charset)))

}

class CypherRawInputFormat(query: String, parameter: String) extends CypherImportFormat(query, parameter) {

  override def importBytes(arr: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Bytes(arr, representsId = false))

}

class ProtobufInputFormat(query: String, parameter: String, schemaUrl: URL, typeName: String)
    extends CypherImportFormat(query, parameter) {
  private val parser = new ProtobufParser(schemaUrl, typeName)

  override protected def importBytes(data: Array[Byte]): Try[cypher.Value] = Try(parser.parseBytes(data))
}

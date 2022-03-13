package com.thatdot.quine.app.ingest.serialization

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest.serialization.ImportFormat.RetriableIngestFailure
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.messaging.ExactlyOnceTimeoutException
import com.thatdot.quine.graph.{CypherOpsGraph, GraphNotReadyException, cypher}

/** Describes formats that Quine can import
  */
trait ImportFormat {

  /** The (nullable) type to be produced by invocations of this [[ImportFormat]]
    */
  type Deserialized

  /** Attempt to import raw data as a [[Deserialized]]. This will get called for each value to be imported
    * @param data the raw data to decode
    * @return A Success if and only if a [[Deserialized]] can be produced from the provided data,
    *         otherwise, a Failure describing the error during deserialization. These Failures should never
    *         be fatal.
    */
  protected def importBytes(data: Array[Byte]): Try[Deserialized]

  /** Defers to [[importBytes]] but also checks that [[data]] can (probably) be safely sent via akka clustered messaging.
    * This is checked based on [[ImportFormat.akkaMessageSizeLimit]]
    *
    * @param data byte payload
    * @param isSingleHost is the cluster just one host (in which case there is no risk of oversize payloads)
    * @return
    */
  final def importMessageSafeBytes(data: Array[Byte], isSingleHost: Boolean): Try[Deserialized] =
    if (!isSingleHost && data.length > ImportFormat.akkaMessageSizeLimit)
      Failure(
        new Exception(
          s"Attempted to decode ${data.length} bytes, but records larger than ${ImportFormat.akkaMessageSizeLimit} bytes are prohibited."
        )
      )
    else importBytes(data)

  /** A description of the import format.
    */
  def label: String

  /** Writes [[Deserialized]] instances into the graph.
    */
  def writeToGraph(
    graph: CypherOpsGraph,
    deserialized: Deserialized
  ): Future[Done]
}

abstract class CypherImportFormat(query: String, parameter: String) extends ImportFormat with LazyLogging {

  override type Deserialized = cypher.Value

  override val label: String = "Cypher " + query

  // TODO: think about error handling of failed compilation
  val compiled: cypher.CompiledQuery = compiler.cypher.compile(query, unfixedParameters = Seq(parameter))

  if (!compiled.query.isIdempotent) {
    // TODO allow user to override this (see: allowAllNodeScan) and only retry when idempotency is asserted
    logger.warn(
      """Could not verify that the provided ingest query is idempotent. If timeouts occur, query
        |execution may be retried and duplicate data may be created.""".stripMargin.replace('\n', ' ')
    )
  }

  override def writeToGraph(
    graph: CypherOpsGraph,
    deserialized: cypher.Value
  ): Future[Done] = {
    // this Source represents the work that would be needed to query over one specific `value`
    // Work does not begin until the source is `run` (after the recovery strategy is hooked up below)
    // If a recoverable error occurs, instead return a Source that will fail after a small delay
    // so that recoverWithRetries (below) can retry the query
    def cypherQuerySource: Source[Vector[Value], NotUsed] =
      try compiled
        .run(parameters = Map(parameter -> deserialized))(graph)
        .results
      catch {
        case RetriableIngestFailure(e) =>
          // TODO arbitrary timeout delays repeated failing calls to clusterOp in implementation of .run above
          Source.future(akka.pattern.after(100.millis)(Future.failed(e))(graph.system))
      }

    cypherQuerySource
      .recoverWithRetries(
        attempts = -1, // retry forever, relying on the relayAsk timer itself to slow down attempts
        { case RetriableIngestFailure(e) =>
          logger.info(
            s"""Suppressed '$e' during execution of ingest query, retrying now.
                   |Ingested item: $deserialized. Query: "$query. :
                   """".stripMargin.replace('\n', ' ')
          )
          cypherQuerySource
        }
      )
      .runWith(Sink.ignore)(graph.materializer)

  }
}

object ImportFormat {
  // An estimated limit on record size (based on the akka remote frame size with 15kb of headspace)
  val akkaMessageSizeLimit: Long =
    ConfigFactory.load().getBytes("akka.remote.artery.advanced.maximum-frame-size") - 15 * 1024

  /** Helper to recognize errors that can be caught and retried during ingest (for example, errors that could occur
    * as a result of cluster topology changing, or GC pauses)
    *
    * These exceptions should include any that can occur as the result of cluster latency (eg temporary network
    * failures), but should not include any exceptions that will always get thrown on subsequent retries (eg
    * deserialization errors)
    *
    * Inspired by [[scala.util.control.NonFatal]]
    */
  object RetriableIngestFailure {
    def unapply(e: Throwable): Option[Throwable] = e match {
      // A relayAsk-based protocol timed out, but might succeed when retried
      case _: ExactlyOnceTimeoutException => Some(e)
      // Graph is not currently ready, but may be in the future
      case _: GraphNotReadyException => Some(e)
      // Retriable failures related to StreamRefs
      case _: akka.stream.RemoteStreamRefActorTerminatedException => Some(e)
      case _: akka.stream.StreamRefSubscriptionTimeoutException => Some(e)
      case _: akka.stream.InvalidSequenceNumberException => Some(e)
      case _ => None
    }
  }

  class CypherJson(query: String, parameter: String) extends CypherImportFormat(query, parameter) {

    override def importBytes(data: Array[Byte]): Try[cypher.Value] = Try {
      // deserialize bytes into JSON without going through string
      val json: ujson.Value = ujson.read(data)
      cypher.Value.fromJson(json)
    }

  }

  class CypherRaw(query: String, parameter: String) extends CypherImportFormat(query, parameter) {

    override def importBytes(arr: Array[Byte]): Try[cypher.Value] = Success(cypher.Expr.Bytes(arr))

  }

  class TestOnlyDrop extends ImportFormat {
    override type Deserialized = Unit
    override val label = "TestOnlyDrop"
    override def importBytes(data: Array[Byte]): Try[Deserialized] = Success(())
    override def writeToGraph(
      graph: CypherOpsGraph,
      deserialized: Deserialized
    ): Future[Done] = Future.successful(Done)
  }

}

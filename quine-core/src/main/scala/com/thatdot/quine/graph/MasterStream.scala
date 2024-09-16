package com.thatdot.quine.graph

import scala.annotation.unused
import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{Materializer, UniqueKillSwitch}
import org.apache.pekko.{Done, NotUsed}

import org.apache.pekko

import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.PekkoStreams.errorSuppressingMergeHub

class MasterStream(implicit val mat: Materializer, val logConfig: LogConfig) extends LazySafeLogging {
  import MasterStream._

  private val (_ingestHub, ingestSource) =
    errorSuppressingMergeHub[IngestSrcExecToken]("master-stream-ingest-mergehub").preMaterialize()
  private val (_sqResultsHub, sqResultsSource) =
    errorSuppressingMergeHub[SqResultsExecToken]("master-stream-sq-results-mergehub").preMaterialize()
  private val (_nodeSleepHub, nodeSleepSource) =
    errorSuppressingMergeHub[NodeSleepExecToken]("master-stream-node-sleeps-mergehub").preMaterialize()
  private val (_persistorHub, persistorSource) =
    errorSuppressingMergeHub[PersistorExecToken]("master-stream-persistor-mergehub").preMaterialize()

  // Pekko docs are misleading. `false` means that the new source is being added via mergePreferred is preferred
  // over the original source / receiver. E.g, Source.repeat(IdleToken) will have the lowest preference, followed
  // by ingestSource, etc.
  private val preferNewHubOverUpstream = false
  @unused
  private[this] def demonstratePekkoDocsMisleadingness(): Unit = {
    val left = Source.repeat("Left")
    val right = Source.repeat("Right")

    var preferRight = true // Pekko docs issue here!
    val _ = left
      .mergePreferred(right, preferRight)
      .runForeach(println(_)) // Logs "Left" forever, even though "preferRight = true"

    preferRight = false
    val _ = left
      .mergePreferred(right, preferRight)
      .runForeach(println(_)) // Logs "Right" forever, even though "preferRight = false"
  }

  // These sinks are the main interface to the master stream -- each accepts completion tokens for a single kind of work
  val ingestCompletionsSink = _ingestHub
  val standingOutputsCompletionSink = _sqResultsHub
  val nodeSleepCompletionsSink = _nodeSleepHub
  val persistorCompletionsSink = _persistorHub

  // Sink to give the stream an overall outlet (and thus allow it to actually run).
  // NB this never backpressures, so all the upstreams will be allowed to run as quickly as the `mergePreferred`
  // overhead allows.
  private val loggingSink: Sink[ExecutionToken, Future[Done]] =
    Sink.foreach[ExecutionToken](x => logger.trace(safe"${Safe(x.name)}")).named("master-stream-logging-sink")

  Source
    .repeat(IdleToken)
    .throttle(1, 1.second)
    .mergePreferred(ingestSource, preferNewHubOverUpstream)
    .mergePreferred(sqResultsSource, preferNewHubOverUpstream)
    .mergePreferred(nodeSleepSource, preferNewHubOverUpstream)
    .mergePreferred(persistorSource, preferNewHubOverUpstream)
    .runWith(loggingSink)(mat)
}

case object MasterStream {

  sealed trait ExecutionToken { val name: String }
  case object IdleToken extends ExecutionToken { val name: String = this.toString }
  final case class IngestSrcExecToken(name: String) extends ExecutionToken
  final case class SqResultsExecToken(name: String) extends ExecutionToken
  final case class NodeSleepExecToken(name: String) extends ExecutionToken
  final case class PersistorExecToken(name: String) extends ExecutionToken

  type IngestSrcType = Source[IngestSrcExecToken, NotUsed]
  type SqResultsSrcType = Source[SqResultsExecToken, UniqueKillSwitch]
  type NodeSleepSrcType = Source[NodeSleepExecToken, UniqueKillSwitch]
  type PersistorSrcType = Source[PersistorExecToken, UniqueKillSwitch]
}

trait IngestControl {
  def pause(): Future[Boolean]
  def unpause(): Future[Boolean]
  def terminate(): Future[pekko.Done]
}

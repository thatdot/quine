package com.thatdot.quine.graph

import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{Materializer, UniqueKillSwitch}
import org.apache.pekko.{Done, NotUsed}

import org.apache.pekko

import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.PekkoStreams.errorSuppressingMergeHub

class MasterStream(mat: Materializer)(implicit val logConfig: LogConfig) extends LazySafeLogging {
  import MasterStream._

  def addIngestSrc(src: IngestSrcType): NotUsed = ingestHub.runWith(src)(mat)
  def addSqResultsSrc(src: SqResultsSrcType): UniqueKillSwitch = sqResultsHub.runWith(src)(mat)
  def addNodeSleepSrc(src: NodeSleepSrcType): UniqueKillSwitch = nodeSleepHub.runWith(src)(mat)
  def addPersistorSrc(src: PersistorSrcType): UniqueKillSwitch = persistorHub.runWith(src)(mat)

  private val (ingestHub, ingestSource) =
    errorSuppressingMergeHub[IngestSrcExecToken]("master-stream-ingest-mergehub", mat)
  private val (sqResultsHub, sqResultsSource) =
    errorSuppressingMergeHub[SqResultsExecToken]("master-stream-sq-results-mergehub", mat)
  private val (nodeSleepHub, nodeSleepSource) =
    errorSuppressingMergeHub[NodeSleepExecToken]("master-stream-node-sleeps-mergehub", mat)
  private val (persistorHub, persistorSource) =
    errorSuppressingMergeHub[PersistorExecToken]("master-stream-persistor-mergehub", mat)
  private val preferNewHubOverUpstream = false // Pekko docs are misleading. `false` gives the desired merge preference.

  val loggingSink: Sink[ExecutionToken, Future[Done]] =
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

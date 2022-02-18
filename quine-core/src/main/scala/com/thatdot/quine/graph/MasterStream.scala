package com.thatdot.quine.graph

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.stream.scaladsl.{MergeHub, Sink, Source}
import akka.stream.{Materializer, UniqueKillSwitch}

import com.typesafe.scalalogging.LazyLogging

class MasterStream(mat: Materializer) extends LazyLogging {
  import MasterStream._

  def addIngestSrc[T <: IngestControl](src: IngestSrcType[T]): Future[T] = ingestHub.runWith(src)(mat)
  def addSqResultsSrc(src: SqResultsSrcType): UniqueKillSwitch = sqResultsHub.runWith(src)(mat)
  def addNodeSleepSrc(src: NodeSleepSrcType): UniqueKillSwitch = nodeSleepHub.runWith(src)(mat)
  def addPersistorSrc(src: PersistorSrcType): UniqueKillSwitch = persistorHub.runWith(src)(mat)

  private val (ingestHub, ingestSource) = MergeHub.source[IngestSrcExecToken].preMaterialize()(mat)
  private val (sqResultsHub, sqResultsSource) = MergeHub.source[SqResultsExecToken].preMaterialize()(mat)
  private val (nodeSleepHub, nodeSleepSource) = MergeHub.source[NodeSleepExecToken].preMaterialize()(mat)
  private val (persistorHub, persistorSource) = MergeHub.source[PersistorExecToken].preMaterialize()(mat)
  private val preferNewHubOverUpstream = false // Akka docs are misleading. `false` gives the desired merge preference.

  val loggingSink: Sink[ExecutionToken, Future[Done]] = Sink.foreach[ExecutionToken](x => logger.trace(x.name))

  Source
    .repeat(IdleToken)
    .throttle(1, 1 second)
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

  type IngestSrcType[+T <: IngestControl] = Source[IngestSrcExecToken, Future[T]]
  type SqResultsSrcType = Source[SqResultsExecToken, UniqueKillSwitch]
  type NodeSleepSrcType = Source[NodeSleepExecToken, UniqueKillSwitch]
  type PersistorSrcType = Source[PersistorExecToken, UniqueKillSwitch]
}

trait IngestControl {
  def pause(): Future[Boolean]
  def unpause(): Future[Boolean]
  def terminate(): Future[akka.Done]
}

package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub, Sink, Source, SourceQueueWithComplete}
import org.apache.pekko.stream.{
  BoundedSourceQueue,
  KillSwitch,
  KillSwitches,
  Materializer,
  OverflowStrategy,
  QueueOfferResult,
}

import io.circe.Json

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest2.source.DecodedSource
import com.thatdot.quine.app.model.ingest2.sources
import com.thatdot.quine.app.routes.IngestMeter

/** One record pushed into a [[QuineSource]], tagged with the connection it arrived on so that its
  * reply can be sent back on the same connection.
  */
final case class QuineFrame(connectionId: Long, value: Json)

/** Source for the "Quine" ingest: standing-query results pushed by Quine's Novelty output over any
  * number of connections (one per Quine host), merged into this one ingest, with each record's
  * replies routed back to the connection that sent it.
  *
  * A connection [[attach]]es to get a sink for its inbound records and a source of its outbound
  * frames. The ingest runs independently of its connections: connections may attach and detach at
  * any time.
  *
  * Replies are delivered through [[replies]]: the ingest runner sends each record's replies paired
  * with the record's [[QuineFrame]], and each reply is queued on that frame's connection. A record
  * gets as many replies as its transformation yields observations: usually one, possibly none or
  * several. Each connection's reply queue is bounded, and when it is full the [[replies]] sink waits
  * for the connection to read, which backpressures the ingest that every connection shares: a Quine
  * host that writes its scores slowly slows the ingest, and so every host's sends, rather than losing
  * scores. (All of an ingest's connections come from one Quine cluster, whose standing-query outputs
  * absorb that backpressure.) A reply for a connection that has detached, or detaches while the sink
  * waits, is dropped.
  *
  * A connection's outbound frames also carry rescores of the ingest's model ([[offerRescore]]):
  * unsolicited, in the same format as a scored reply, and sent on whichever connection has room. They
  * have their own smaller queue per connection, so a burst of rescores never crowds out replies; a
  * rescore no connection has room for is dropped. Rescores sent and dropped are counted
  * ([[rescoreCounts]]).
  *
  * Once the ingest terminates ([[onTermination]]), every connection's replies complete, including
  * those of a connection that attaches after that, and the merged records end, so every connection
  * closes.
  *
  * Create one with [[QuineSource.apply]].
  *
  * @param hubSink   where every connection's records are merged
  * @param hubSource the merged records, consumed by the ingest
  * @param hubSwitch ends the merge of records, on termination
  */
final class QuineSource private (
  meter: IngestMeter,
  hubSink: Sink[QuineFrame, NotUsed],
  hubSource: Source[QuineFrame, NotUsed],
  hubSwitch: KillSwitch,
)(implicit mat: Materializer)
    extends DecodedSource(meter) {

  override type Decoded = Json
  override type Frame = QuineFrame
  override val foldable: DataFoldableFrom[Json] = DataFoldableFrom.jsonDataFoldable
  override val foldableFrame: DataFoldableFrom[QuineFrame] = DataFoldableFrom.jsonDataFoldable.contramap(_.value)

  override def content(input: QuineFrame): Array[Byte] = input.value.noSpaces.getBytes(StandardCharsets.UTF_8)

  /** Stream of decoded values. Metered in [[attach]], as each record arrives. */
  override def stream: Source[(() => Try[Json], QuineFrame), ShutdownSwitch] =
    sources.withKillSwitches(hubSource.map(frame => (() => Success(frame.value), frame)))

  private val nextConnectionId = new AtomicLong()
  private val connections = new ConcurrentHashMap[Long, QuineSource.Queues]()

  /** Set by [[onTermination]]. A connection's queues are completed by whichever of termination and
    * their own registration removes them from [[connections]] once this is set, so exactly once.
    */
  private val terminated = new AtomicBoolean(false)

  /** Rescores sent to some connection, rescores dropped, and where the next rescore's search for a
    * connection with room starts.
    */
  private val rescoresSent = new AtomicLong()
  private val rescoresDropped = new AtomicLong()
  private val nextRescoreConnection = new AtomicLong()

  /** Queue `reply` for connection `connectionId`, completing once the queue has taken it: at once
    * while it has room, otherwise once the connection has read enough to make room. Completes without
    * queueing it if the connection isn't attached, or detaches first, or (for a direct reply) has too
    * many replies already waiting for room.
    */
  private def offer(connectionId: Long, reply: Json): Future[Unit] =
    Option(connections.get(connectionId)).fold(Future.unit) { queues =>
      Future
        .delegate(queues.replies.offer(reply))(ExecutionContext.parasitic)
        .transform(_ => Success(()))(ExecutionContext.parasitic)
    }

  override def replies: Option[Sink[(Json, QuineFrame), NotUsed]] =
    Some(
      Flow[(Json, QuineFrame)]
        .mapAsync(1) { case (reply, frame) => offer(frame.connectionId, reply) }
        .to(Sink.ignore),
    )

  /** Send `rescore` on one of the attached connections, trying them in turn (starting from a different
    * one each time) until one has room in its rescore queue.
    *
    * @return whether a connection took it. Counted as sent if so; not counted otherwise, since the
    *         caller may still offer it to another source (see [[rescoreDropped]]).
    */
  def offerRescore(rescore: Json): Boolean = {
    val attached = connections.values().asScala.toVector
    val start = nextRescoreConnection.getAndIncrement()
    val taken = attached.indices.exists { i =>
      attached(Math.floorMod(start + i, attached.size.toLong).toInt).rescores
        .offer(rescore) == QueueOfferResult.Enqueued
    }
    if (taken) rescoresSent.incrementAndGet(): Unit
    taken
  }

  /** Count a rescore meant for this source that no connection had room for. */
  def rescoreDropped(): Unit = rescoresDropped.incrementAndGet(): Unit

  /** Rescores sent on this source's connections, and dropped, since it started. */
  def rescoreCounts: QuineSource.RescoreCounts = QuineSource.RescoreCounts(rescoresSent.get(), rescoresDropped.get())

  /** Attach a new connection. Frames are delivered once the returned outbound source is running.
    *
    * @param replyBufferSize   replies held for this connection before the ingest waits for it to read
    * @param rescoreBufferSize rescores held for this connection before it is passed over for them
    * @return a sink for the connection's inbound records, and the source of its outbound frames
    *         (replies and rescores). The connection detaches when its outbound source terminates.
    */
  def attach(replyBufferSize: Int, rescoreBufferSize: Int): QuineSource.Connection = {
    val connectionId = nextConnectionId.incrementAndGet()
    val inbound = Flow[Json]
      .map { record =>
        meter.mark(record.noSpaces.length)
        QuineFrame(connectionId, record)
      }
      .to(hubSink)
    // The queues are registered when the connection's outbound stream starts, so the connection's
    // buffers are exactly the queues (no further buffer from pre-materializing them). The outbound
    // stream ends once both queues have completed.
    val outbound = Source
      .queue[Json](replyBufferSize, OverflowStrategy.backpressure, QuineSource.MaxWaitingReplies)
      .mergeMat(Source.queue[Json](rescoreBufferSize))(Keep.both)
      .mapMaterializedValue { case (replies, rescores) =>
        val queues = QuineSource.Queues(replies, rescores)
        connections.put(connectionId, queues)
        // Attached after the ingest terminated: there will be nothing to send.
        if (terminated.get() && connections.remove(connectionId, queues)) queues.complete()
        NotUsed
      }
      .watchTermination() { (_, done) =>
        done.onComplete(_ => connections.remove(connectionId))(mat.executionContext)
        NotUsed
      }
    QuineSource.Connection(connectionId, inbound, outbound)
  }

  /** Send a reply to one connection directly, bypassing the ingest — for records that never enter it,
    * such as a frame that isn't valid JSON. Never waits: it is called from the connection's own
    * inbound stream, which must keep flowing.
    */
  def replyDirectly(connectionId: Long, reply: Json): Unit = {
    val _ = offer(connectionId, reply)
  }

  override def onTermination(): Unit = {
    terminated.set(true)
    hubSwitch.shutdown()
    connections.keySet().forEach { connectionId =>
      val queues = connections.remove(connectionId)
      if (queues != null) queues.complete()
    }
  }
}

object QuineSource {

  /** Replies that may wait at once for room in one connection's reply queue: the one the [[replies]]
    * sink waits on, and the rest for [[QuineSource.replyDirectly]], whose replies are dropped beyond
    * that.
    */
  private val MaxWaitingReplies: Int = 16

  /** A new source, with its hub for merging connections' records running until it terminates. */
  def apply(meter: IngestMeter)(implicit mat: Materializer): QuineSource = {
    // A BroadcastHub rather than a single-subscriber publisher, so that a restart of the ingest
    // stream can subscribe to the merged records again.
    val ((hubSink, hubSwitch), hubSource) = MergeHub
      .source[QuineFrame](perProducerBufferSize = 16)
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(BroadcastHub.sink[QuineFrame](bufferSize = 16))(Keep.both)
      .run()
    new QuineSource(meter, hubSink, hubSource, hubSwitch)
  }

  /** One connection's outbound queues: replies to its records, and rescores. */
  final private case class Queues(replies: SourceQueueWithComplete[Json], rescores: BoundedSourceQueue[Json]) {

    /** Complete both queues, which the connection may already have closed. */
    def complete(): Unit = {
      val _ = (Try(replies.complete()), Try(rescores.complete()))
    }
  }

  /** Rescores a source has sent to Quine, and dropped because no connection had room. */
  final case class RescoreCounts(sent: Long, dropped: Long)

  /** One attached connection.
    *
    * @param inbound  records sent by this connection
    * @param outbound frames for this connection: replies to its records, and rescores
    */
  final case class Connection(id: Long, inbound: Sink[Json, NotUsed], outbound: Source[Json, NotUsed])
}

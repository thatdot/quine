package com.thatdot.quine.util

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.{ActorSystem, Cancellable}

/** Identifies a gauge within the streaming topology. */
final case class GaugeKey(
  pipelineType: String, // "ingest" or "sq-output"
  namespace: String,
  streamName: String,
  stagePosition: String, // "source", "pre-graph-write", "post-graph-write", etc.
)

/** Aggregate backpressure assessment from a ring buffer of samples.
  *
  * 0-1 of 5 samples backpressured → Flowing
  * 2-3 of 5 samples backpressured → Constrained (intermittent bottleneck)
  * 4-5 of 5 samples backpressured → Backpressured (consistent bottleneck)
  */
sealed trait BackpressureLevel
object BackpressureLevel {
  case object Flowing extends BackpressureLevel
  case object Constrained extends BackpressureLevel // This is a made-up middle ground. Not strictly meaningful.
  case object Backpressured extends BackpressureLevel
  def fromCount(backpressuredCount: Int): BackpressureLevel =
    if (backpressuredCount <= 1) Flowing
    else if (backpressuredCount <= 3) Constrained
    else Backpressured
}

/** Thread-safe registry for dynamic backpressure gauge states.
  *
  * Gauges register themselves when their stream materializes and deregister
  * when the stream stops. A background scheduler samples all gauges every 500ms
  * into per-gauge ring buffers (5 samples). The snapshot API returns an aggregate
  * assessment based on the recent history rather than a single point-in-time read.
  */
class PressureGaugeRegistry {
  private val RingSize = 5

  private val gauges = new ConcurrentHashMap[GaugeKey, PressureGauge.State]()

  /** Per-gauge ring buffer of recent samples (0 = flowing, 1 = backpressured, -1 = disabled).
    *
    * == Threading model ==
    *
    * '''Writer:''' A single background scheduler thread calls [[sampleAll]] every 500ms.
    * It is the only thread that writes to these arrays or increments [[writeIndex]].
    *
    * '''Readers:''' API handler threads call [[snapshot]] to read the arrays.
    *
    * == Why plain arrays, not AtomicIntegerArray ==
    *
    * We considered `AtomicIntegerArray` but chose plain `Array[Int]` because:
    *
    *   - The JVM guarantees that 32-bit int writes are atomic (no torn reads), so a reader
    *     will always see a valid 0, 1, or -1 — never a partial write.
    *
    *   - Without a volatile/atomic store, a reader thread ''may'' see a stale value (a sample
    *     from the previous tick rather than the current one). This is acceptable: the ring
    *     buffer aggregates 5 samples over 2.5 seconds, so reading one slot that is 500ms
    *     stale is indistinguishable from the inherent measurement granularity.
    *
    *   - `AtomicIntegerArray` issues a `StoreLoad` fence on every `set()`, which is the
    *     most expensive memory barrier on x86/ARM. With N gauges sampled every 500ms,
    *     this would add N fence operations per tick — unnecessary overhead for data that
    *     is consumed approximately, not transactionally.
    *
    *   - `writeIndex` is similarly non-volatile. The worst case is a reader sees a stale
    *     index, which just means it reads from one tick ago — again, within noise for a
    *     2.5-second rolling window.
    *
    * If the sampling interval were reduced to microseconds or the data were used for
    * correctness-critical decisions (rather than UI display), atomics would be warranted.
    */
  private val ringBuffers = new ConcurrentHashMap[GaugeKey, Array[Int]]()
  private var writeIndex: Int = 0

  def register(key: GaugeKey, state: PressureGauge.State): Unit = {
    val _ = gauges.put(key, state)
    val _ = ringBuffers.putIfAbsent(key, new Array[Int](RingSize))
  }

  def deregister(key: GaugeKey): Unit = {
    val _ = gauges.remove(key)
    val _ = ringBuffers.remove(key)
  }

  /** Remove the gauges for a stream and all of its hierarchical sub-streams.
    *
    * Names form a '/'-delimited hierarchy: a standing query "q" owns its outputs
    * "q/out1" and "q/out2", each of which owns per-stage gauges. Matching is on whole
    * path segments — a gauge matches when its stream name equals `streamName` exactly
    * or begins with `streamName + "/"` — never on a raw character prefix. This ensures
    * deregistering ingest "data" does not also drop the unrelated sibling "data1", while
    * deregistering standing query "q" still removes every "q/..." output.
    */
  def deregisterByPrefix(pipelineType: String, namespace: String, streamName: String): Unit =
    gauges
      .keySet()
      .asScala
      .filter { k =>
        k.pipelineType == pipelineType && k.namespace == namespace &&
        (k.streamName == streamName || k.streamName.startsWith(streamName + "/"))
      }
      .foreach { k =>
        val _ = gauges.remove(k)
        val _ = ringBuffers.remove(k)
      }

  /** Called by the background scheduler every 500ms. Reads each gauge and writes
    * the current state into the next ring buffer slot.
    */
  private[util] def sampleAll(): Unit = {
    val idx = writeIndex % RingSize
    gauges.forEach { (key, state) =>
      val ring = ringBuffers.get(key)
      if (ring != null) {
        ring(idx) = if (state.isUnderPressure) 1 else 0
      }
    }
    writeIndex += 1
  }

  /** Start the background sampling scheduler. Returns a cancellable handle. */
  def startSampling(system: ActorSystem): Cancellable =
    system.scheduler.scheduleWithFixedDelay(500.millis, 500.millis)(() => sampleAll())(ExecutionContext.parasitic)

  /** Snapshot all registered gauges, returning the aggregate backpressure level
    * computed from the ring buffer of recent samples.
    */
  def snapshot(): Map[GaugeKey, BackpressureLevel] =
    ringBuffers.asScala.map { case (key, ring) =>
      var backpressuredCount = 0
      var i = 0
      while (i < RingSize) {
        if (ring(i) == 1) backpressuredCount += 1
        i += 1
      }
      val level = BackpressureLevel.fromCount(backpressuredCount)
      key -> level
    }.toMap
}

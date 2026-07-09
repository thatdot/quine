package com.thatdot.quine.webapp.components.landing

import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Computes instantaneous rates from monotonic counter deltas between successive API polls.
  *
  * Server-side rates use Dropwizard's 1-minute EWMA which decays slowly (~60s to reach zero).
  * By comparing counters between two consecutive polls (~5s apart), we get rates that respond
  * within a single poll interval.
  *
  * On the first poll (no previous data), or when conditions are unreliable (counter reset,
  * browser tab backgrounded), we fall back to the server-provided EWMA rates.
  *
  * Design note: counter-delta rates are inherently summable across cluster members, which
  * makes this approach compatible with future cluster-wide aggregation.
  */
object RateComputer {

  // Key types for rate tracking
  type IngestKey = (String, String) // (namespace, name)
  type OutputKey = (String, String, String) // (namespace, sqName, outputName)
  type QueueKey = (String, String) // (namespace, sqName)

  private case class Counters(totalCount: Long, timestampMs: Double)
  private case class QueueCounters(produced: Long, consumed: Long, timestampMs: Double)

  // Session-scoped state — reset on page reload, persists across tab switches
  private var prevIngests: Map[IngestKey, Counters] = Map.empty
  private var prevOutputs: Map[OutputKey, Counters] = Map.empty
  private var prevQueues: Map[QueueKey, QueueCounters] = Map.empty

  /** Snapshot enriched with client-computed rates. */
  case class EnrichedSnapshot(
    snap: V2BackpressureSnapshot,
    ingestRates: Map[IngestKey, Double],
    outputRates: Map[OutputKey, Double],
    queueProductionRates: Map[QueueKey, Double],
    queueConsumptionRates: Map[QueueKey, Double],
  ) {
    def ingestRate(p: V2IngestSnapshot): Double =
      ingestRates.getOrElse((p.namespace, p.name), p.rate)

    def sqOutputRate(ns: String, sqName: String, output: V2SqOutput): Double =
      outputRates.getOrElse((ns, sqName, output.name), output.rate)

    def queueProductionRate(ns: String, sqName: String, fallback: Double): Double =
      queueProductionRates.getOrElse((ns, sqName), fallback)

    def queueConsumptionRate(ns: String, sqName: String, fallback: Double): Double =
      queueConsumptionRates.getOrElse((ns, sqName), fallback)
  }

  /** Compute client-side rates by comparing this snapshot's counters to the previous one.
    * Must be called exactly once per poll tick (it updates internal state).
    */
  def computeRates(snap: V2BackpressureSnapshot): EnrichedSnapshot = {
    val nowMs = snap.timestamp
    val newIngests = Map.newBuilder[IngestKey, Counters]
    val newOutputs = Map.newBuilder[OutputKey, Counters]
    val newQueues = Map.newBuilder[QueueKey, QueueCounters]
    val ingestRates = Map.newBuilder[IngestKey, Double]
    val outputRates = Map.newBuilder[OutputKey, Double]
    val queueProdRates = Map.newBuilder[QueueKey, Double]
    val queueConsRates = Map.newBuilder[QueueKey, Double]

    // Per-ingest rates
    snap.ingests.foreach { p =>
      val key = (p.namespace, p.name)
      newIngests += key -> Counters(p.totalCount, nowMs)
      prevIngests.get(key).foreach { prev =>
        val dtSec = (nowMs - prev.timestampMs) / 1000.0
        if (dtSec > 0.5 && dtSec < 30.0 && p.totalCount >= prev.totalCount)
          ingestRates += key -> (p.totalCount - prev.totalCount).toDouble / dtSec
      }
    }

    // Per-SQ queue and output rates
    snap.standingQueries.foreach { sq =>
      val qKey = (sq.namespace, sq.name)
      val qi = sq.queue
      newQueues += qKey -> QueueCounters(qi.totalProduced, qi.totalConsumed, nowMs)
      prevQueues.get(qKey).foreach { prev =>
        val dtSec = (nowMs - prev.timestampMs) / 1000.0
        if (dtSec > 0.5 && dtSec < 30.0 && qi.totalProduced >= prev.produced && qi.totalConsumed >= prev.consumed) {
          queueProdRates += qKey -> (qi.totalProduced - prev.produced).toDouble / dtSec
          queueConsRates += qKey -> (qi.totalConsumed - prev.consumed).toDouble / dtSec
        }
      }

      sq.outputs.foreach { o =>
        val oKey = (sq.namespace, sq.name, o.name)
        newOutputs += oKey -> Counters(o.totalCount, nowMs)
        prevOutputs.get(oKey).foreach { prev =>
          val dtSec = (nowMs - prev.timestampMs) / 1000.0
          if (dtSec > 0.5 && dtSec < 30.0 && o.totalCount >= prev.totalCount)
            outputRates += oKey -> (o.totalCount - prev.totalCount).toDouble / dtSec
        }
      }
    }

    // Update state for next tick
    prevIngests = newIngests.result()
    prevOutputs = newOutputs.result()
    prevQueues = newQueues.result()

    EnrichedSnapshot(
      snap,
      ingestRates.result(),
      outputRates.result(),
      queueProdRates.result(),
      queueConsRates.result(),
    )
  }
}

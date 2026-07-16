package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.Signal
import com.raquo.airstream.state.Var

import com.thatdot.quine.webapp.dataservice.BackpressureService.Scope
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** Retains per-host backpressure snapshots and resolves them into a [[BackpressureView]].
  *
  * == What it holds, and why ==
  *
  * The server reports current state, one snapshot per cluster member per poll: per-stage levels
  * (already smoothed by a 2.5-second gauge on the server) and cumulative counters. Levels are used
  * as-is — the diagram shows the most recent state. Rates are the one thing the client must derive,
  * by differencing a counter across the two most recent snapshots; that is the only reason more than
  * the latest snapshot is kept. There is no lookback window.
  *
  * == Keyed by process, not by member ==
  *
  * History is keyed on [[V2HostInfo.hostKey]] — address, port, and '''pid'''. A cluster member
  * survives failover and is refilled by a fresh process whose cumulative counters begin again at
  * zero. Keying on `memberIdx` would diff the new process's counters against the dead one's and
  * report the reset as a throughput spike; keying on the process instead makes the handover a key
  * with no history yet, which falls back to the server's EWMA for one poll and then recovers.
  *
  * == Recording is idempotent ==
  *
  * [[record]] ignores a snapshot no newer than the last one held for that host. An Airstream signal
  * may re-run its mapping function on a value it has already seen (a restarted subscription replays
  * the current value), and the previous design — a global `RateComputer` with mutable `prev` maps,
  * driven from inside a rendering component — corrupted its own state when that happened. Making
  * the write idempotent removes the hazard rather than documenting it.
  */
final class BackpressureStore {
  import BackpressureStore._

  /** Per-host history, oldest first. Pruned by *time*: a departed member's last samples are kept for
    * [[RetentionMs]] so it stays visible in the picker as "not reporting" rather than vanishing.
    */
  private var history: Map[String, Vector[V2BackpressureSnapshot]] = Map.empty

  private val membersVar: Var[Seq[Int]] = Var(Seq.empty)

  /** Cluster members observed, ascending. Empty on OSS. */
  val members: Signal[Seq[Int]] = membersVar.signal

  /** The same members, read synchronously — for a caller that must validate a scope at the moment a
    * command is dispatched rather than react to [[members]].
    */
  def currentMembers: Seq[Int] = membersVar.now()

  /** Absorb one poll: one snapshot per reachable member. */
  def record(snaps: Seq[V2BackpressureSnapshot]): Unit =
    if (snaps.nonEmpty) {
      val appended = snaps.foldLeft(history) { (acc, snap) =>
        val key = snap.host.hostKey
        val prior = acc.getOrElse(key, Vector.empty)
        // Idempotent: a replayed or out-of-order snapshot adds nothing.
        if (prior.lastOption.exists(_.timestamp >= snap.timestamp)) acc
        else acc.updated(key, prior :+ snap)
      }

      // Prune against the newest measurement rather than the browser clock: the timestamps are the
      // server's, and a skewed browser clock must not be able to evict live history.
      val newest = snaps.map(_.timestamp).max
      val cutoff = newest - RetentionMs
      history = appended.view
        .mapValues(_.filter(_.timestamp >= cutoff))
        .filter { case (_, samples) => samples.nonEmpty }
        .toMap

      // Members come from retained history, not from this poll alone. A member that stops answering
      // must stay in the picker — tinted as not reporting — rather than silently vanishing from it:
      // "member 3 is down" is the single most important thing the picker can say, and it cannot say
      // it about a member it has dropped. Retention ages a long-departed member out on its own.
      val observed = liveHosts.flatMap(_._1.memberIdx).distinct.sorted
      if (observed != membersVar.now()) membersVar.set(observed)
    }

  /** Resolve the retained history into the view for `scope`.
    *
    * Pure with respect to the network: recomputing over a different scope reads history already held
    * and never refetches.
    *
    * `None` only when nothing has been recorded, or when `scope` names a member that has not
    * reported — callers validate the scope before calling (see `OssDataService`).
    */
  def view(scope: Scope): Option[BackpressureView] = {
    val inScope: Seq[(HostRef, Vector[V2BackpressureSnapshot])] = hostsFor(scope)
    if (inScope.isEmpty) None
    else {
      // Staleness is judged against the newest sample *any* host reported, not the newest in scope.
      // Judging within scope would make a single-member scope grade itself against itself, so a
      // member that dropped out an hour ago would still be its own latest poll and read as reporting
      // — precisely the case the member picker needs to color as down.
      val latestPollTs = newestTimestamp.getOrElse(inScope.flatMap(_._2.map(_.timestamp)).max)
      val reporting = inScope.collect {
        case (host, samples) if samples.lastOption.exists(_.timestamp >= latestPollTs - StaleHostMs) => host
      }

      // Union the graph lists the in-scope hosts last reported — the server sends every graph on a
      // host, including idle ones, so this carries graphs that no ingest or standing query mentions.
      // Unioned rather than taken from one host so a graph seen by any reporting member survives.
      val namespaces: Seq[String] =
        inScope.flatMap { case (_, samples) => samples.lastOption.toSeq.flatMap(_.namespaces) }.distinct.sorted

      Some(
        BackpressureView(
          scope = scope,
          hostsInScope = inScope.map(_._1),
          hostsReporting = reporting,
          namespaces = namespaces,
          globalValve = aggregateValve(inScope),
          ingests = ingestViews(inScope),
          standingQueries = standingQueryViews(inScope),
          persistor = aggregatePersistor(inScope),
        ),
      )
    }
  }

  /** Every member's headline state — for the member picker, which must tint members that are not in
    * the selected scope and so cannot read them off the emitted [[BackpressureView]].
    *
    * Each member's status is folded from that member's own view, rather than from a separate walk
    * over the raw snapshots. That is the point: a chip is a control that selects a view, and if the
    * chip and the view it selects disagreed about whether the member is backpressured, the user
    * would be right to trust neither. Sharing the code path makes disagreement unrepresentable.
    */
  def statusByMember: Map[Int, MemberStatus] =
    currentMembers.flatMap { idx =>
      view(Scope.Member(idx)).map { v =>
        idx -> MemberStatus(worst = v.worstLevel, isReporting = v.hostsReporting.nonEmpty)
      }
    }.toMap

  /** The newest measurement held for any host — the reference point for judging staleness. */
  private def newestTimestamp: Option[Double] =
    history.valuesIterator.flatMap(_.lastOption.map(_.timestamp)).maxOption

  /** Every host worth reading, each with its full retained history — one per member, plus any
    * unpositioned host (OSS). A member is matched on the host's most recent `memberIdx`, so a process
    * that has since been repositioned is attributed to where it is now.
    *
    * Exactly one process holds a member at a time. After a failover the departed process is still in
    * `history` — keyed by pid, so it survives its replacement's arrival — and it still claims the
    * member it died holding. It is a ghost, and counting it would sum a dead member's frozen counters
    * into its successor's and keep the member tinted by whatever state the old process died in, for as
    * long as retention holds it. The process with the newest sample wins the member; the ghost is
    * dropped until retention evicts it.
    */
  private def liveHosts: Seq[(HostRef, Vector[V2BackpressureSnapshot])] = {
    val all: Seq[(HostRef, Vector[V2BackpressureSnapshot])] = history.toSeq.flatMap { case (key, samples) =>
      samples.lastOption.map(latest => (HostRef(key, latest.host.memberIdx), samples))
    }
    val (positioned, unpositioned) = all.partition { case (host, _) => host.memberIdx.isDefined }
    val liveAtEachMember = positioned
      .groupBy { case (host, _) => host.memberIdx }
      .values
      .map(_.maxBy { case (_, samples) => samples.last.timestamp })
      .toSeq
    (liveAtEachMember ++ unpositioned)
      .sortBy { case (host, _) => (host.memberIdx.getOrElse(Int.MaxValue), host.key) }
  }

  /** The hosts `scope` selects. */
  private def hostsFor(scope: Scope): Seq[(HostRef, Vector[V2BackpressureSnapshot])] =
    liveHosts.filter { case (host, _) =>
      scope match {
        case Scope.Cluster => true
        case Scope.Member(idx) => host.memberIdx.contains(idx)
      }
    }

  // ── Ingests: collapsed by name across whichever members run them ──

  private def ingestViews(inScope: Seq[(HostRef, Vector[V2BackpressureSnapshot])]): Seq[IngestView] = {
    val keys: Seq[(String, String)] =
      inScope.flatMap { case (_, samples) => samples.last.ingests.map(i => (i.namespace, i.name)) }.distinct

    keys.sorted.map { case (ns, name) =>
      // Each host's own series for this ingest — but only a host whose LATEST snapshot still runs it
      // contributes. Older retained samples are kept for rate differencing, yet a host that has since
      // reported a snapshot without this ingest has stopped or deleted it: counting its presence
      // anywhere in history would keep a deleted ingest in the cluster aggregate — its rate summed in,
      // its member in the tooltip — for the whole retention window. Gating on `samples.last` drops it
      // on the next poll instead. A host that has merely gone silent still carries the ingest in its
      // last snapshot, so its last-known contribution is kept (and flagged via `hostsReporting`).
      val perHost: Seq[(HostRef, Vector[(Double, V2IngestSnapshot)])] = inScope.flatMap { case (host, samples) =>
        if (!samples.last.ingests.exists(i => i.namespace == ns && i.name == name)) None
        else
          Some(host -> samples.flatMap { s =>
            s.ingests.find(i => i.namespace == ns && i.name == name).map(s.timestamp -> _)
          })
      }

      // Rates are summed across members — but each member's rate is differenced from that member's OWN
      // counters first. Summing raw counters across members and then differencing would turn a
      // membership change into a phantom throughput spike, since a member joining or leaving is a step
      // change in the total that has nothing to do with throughput.
      val rate: Double = perHost.map { case (_, series) =>
        latestRate(series.map { case (t, i) => (t, i.totalCount) }).getOrElse(series.last._2.rate)
      }.sum
      val totalCount: Long = perHost.map { case (_, series) => series.last._2.totalCount }.sum

      // A stage's level is the latest snapshot's state, read only where the ingest is RUNNING: a
      // stopped stream's gauge is deregistered or sitting un-pulled at its initial "backpressured"
      // value, so a paused ingest would otherwise read as backpressured. Status, not the gauge, is the
      // authority. Reduced to the worst member, attribution retained.
      def levelAgg(f: V2IngestStages => Option[String]): Aggregated[PressureLevel] =
        worstLevelAcross(perHost.flatMap { case (host, series) =>
          val latest = series.last._2
          if (latest.status == "RUNNING") f(latest.stages).map(s => host -> PressureLevel.fromWire(s)) else None
        })

      // Config fields are member-uniform; take the lowest member's (perHost follows the sorted scope).
      val repr = perHost.head._2.last._2
      // The rate is summed across members, so the limit must be too, or the card compares a
      // cluster-wide throughput against a single member's cap ("2/s of 1/s"). Each member throttles
      // its own instance independently, so the cluster's effective cap is the sum of the per-member
      // caps. If any member is unthrottled the aggregate has no clean cap, so it reads as unlimited.
      val rateLimit: Option[Int] =
        if (perHost.forall(_._2.last._2.rateLimit.isDefined))
          Some(perHost.map(_._2.last._2.rateLimit.get).sum)
        else None
      IngestView(
        name = name,
        namespace = ns,
        sourceType = repr.sourceType,
        status = repr.status,
        rateLimit = rateLimit,
        rate = rate,
        totalCount = totalCount,
        members = perHost.flatMap(_._1.memberIdx).sorted,
        source = levelAgg(st => Some(st.source)),
        preGraphWrite = levelAgg(st => Some(st.preGraphWrite)),
        postGraphWrite =
          if (perHost.exists(_._2.last._2.stages.postGraphWrite.isDefined)) Some(levelAgg(_.postGraphWrite))
          else None,
      )
    }
  }

  // ── Standing queries: replicated onto every member — grouped by name, aggregated with attribution ──

  private def standingQueryViews(inScope: Seq[(HostRef, Vector[V2BackpressureSnapshot])]): Seq[StandingQueryView] = {
    val keys: Seq[(String, String)] =
      inScope.flatMap { case (_, samples) => samples.last.standingQueries.map(sq => (sq.namespace, sq.name)) }.distinct

    keys.sorted.map { case (ns, name) =>
      // Each host's own series for this standing query — gated on the host's LATEST snapshot still
      // running it, so a deleted standing query drops from the aggregate on the next poll rather than
      // lingering out of stale retained samples. See the ingest collapse above for the full rationale.
      val perHost: Seq[(HostRef, Vector[(Double, V2StandingQuery)])] = inScope.flatMap { case (host, samples) =>
        if (!samples.last.standingQueries.exists(sq => sq.namespace == ns && sq.name == name)) None
        else
          Some(host -> samples.flatMap { s =>
            s.standingQueries.find(sq => sq.namespace == ns && sq.name == name).map(s.timestamp -> _)
          })
      }

      val queues: Seq[(HostRef, QueueView)] = perHost.map { case (host, series) =>
        val latest = series.last._2.queue
        host -> QueueView(
          bufferCount = latest.bufferCount,
          backpressureThreshold = latest.backpressureThreshold,
          maxSize = latest.maxSize,
          thresholdRatio = latest.thresholdRatio,
          capacityRatio = latest.capacityRatio,
          totalProduced = latest.totalProduced,
          totalCancellations = latest.totalCancellations,
          totalDropped = latest.totalDropped,
          totalConsumed = latest.totalConsumed,
          productionRate = latestRate(series.map { case (t, sq) => (t, sq.queue.totalProduced) })
            .getOrElse(latest.productionRate),
          consumptionRate = latestRate(series.map { case (t, sq) => (t, sq.queue.totalConsumed) })
            .getOrElse(latest.consumptionRate),
        )
      }

      StandingQueryView(
        name = name,
        namespace = ns,
        queue = Aggregated(combineQueues(queues.map(_._2)), queues),
        outputs = outputViews(perHost),
      )
    }
  }

  private def outputViews(perHost: Seq[(HostRef, Vector[(Double, V2StandingQuery)])]): Seq[SqOutputView] = {
    val names: Seq[String] = perHost.flatMap { case (_, series) => series.last._2.outputs.map(_.name) }.distinct

    names.sorted.map { name =>
      // Gated on the output being in the host's LATEST standing-query snapshot, so a removed output
      // drops promptly rather than lingering out of stale retained samples (as with ingests above).
      val perHostOut: Seq[(HostRef, Vector[(Double, V2SqOutput)])] = perHost.flatMap { case (host, series) =>
        if (!series.last._2.outputs.exists(_.name == name)) None
        else Some(host -> series.flatMap { case (t, sq) => sq.outputs.find(_.name == name).map(t -> _) })
      }

      val rate: Double = perHostOut.map { case (_, out) =>
        latestRate(out.map { case (t, o) => (t, o.totalCount) }).getOrElse(out.last._2.rate)
      }.sum
      val totalCount: Long = perHostOut.map { case (_, out) => out.last._2.totalCount }.sum

      val hasEnrichment: Boolean = perHostOut.exists { case (_, out) => out.last._2.hasEnrichment }
      val enrichment: Option[Aggregated[PressureLevel]] =
        if (!hasEnrichment) None
        else
          Some(worstLevelAcross(perHostOut.flatMap { case (host, out) =>
            out.last._2.enrichmentState.map(s => host -> PressureLevel.fromWire(s))
          }))

      // Destinations join across hosts on `index`, not on `type`: an output may carry two destinations
      // of the same type, and only the index distinguishes them.
      val destKeys: Seq[(Int, String)] = perHostOut
        .flatMap { case (_, out) => out.last._2.destinations.map(d => (d.index, d.`type`)) }
        .distinct
        .sortBy { case (idx, _) => idx }

      SqOutputView(
        name = name,
        rate = rate,
        totalCount = totalCount,
        hasEnrichment = hasEnrichment,
        enrichment = enrichment,
        destinations = destKeys.map { case (idx, tpe) =>
          DestinationView(
            idx,
            tpe,
            worstLevelAcross(perHostOut.flatMap { case (host, out) =>
              out.last._2.destinations.find(_.index == idx).map(d => host -> PressureLevel.fromWire(d.state))
            }),
          )
        },
      )
    }
  }

  // ── Whole-host state ──

  private def aggregateValve(inScope: Seq[(HostRef, Vector[V2BackpressureSnapshot])]): Aggregated[ValveView] = {
    val per: Seq[(HostRef, ValveView)] = inScope.map { case (host, samples) =>
      val latest = samples.last.globalValve
      host -> ValveView(
        isOpen = latest.isOpen,
        closedCount = latest.closedCount,
        oneMinuteClosures = latest.oneMinuteClosures,
      )
    }
    val vs = per.map(_._2)
    Aggregated(
      ValveView(
        // Any closed valve closes the cluster's story: ingest is being held somewhere.
        isOpen = vs.forall(_.isOpen),
        closedCount = vs.map(_.closedCount).sum,
        oneMinuteClosures = vs.map(_.oneMinuteClosures).sum,
      ),
      per,
    )
  }

  private def aggregatePersistor(
    inScope: Seq[(HostRef, Vector[V2BackpressureSnapshot])],
  ): Aggregated[PersistorView] = {
    val per: Seq[(HostRef, PersistorView)] = inScope.map { case (host, samples) =>
      val latest = samples.last.persistor
      host -> PersistorView(
        `type` = latest.`type`,
        writeLatencyMs = latest.writeLatencyMs,
        readLatencyMs = latest.readLatencyMs,
      )
    }
    val ps = per.map(_._2)
    Aggregated(
      PersistorView(
        // The store type is cluster-uniform, so any host's answer is the cluster's.
        `type` = ps.headOption.map(_.`type`).getOrElse("unknown"),
        // Worst member, not the mean: an idle member reports 0ms because it is doing no persisting,
        // and averaging it in would mask a genuinely slow persistor on the member doing the work.
        writeLatencyMs = maxOr0(ps.map(_.writeLatencyMs)),
        readLatencyMs = maxOr0(ps.map(_.readLatencyMs)),
      ),
      per,
    )
  }

  /** Instantaneous queue fields come from the worst member — and '''all''' of them from that same
    * member, so buffer count, budgets, and fill ratios stay mutually consistent in a tooltip.
    * Cumulative counters and rates are meaningful cluster-wide sums. Attribution is not lost by this
    * reduction: the per-host queues ride alongside in [[Aggregated.perHost]].
    */
  private def combineQueues(queues: Seq[QueueView]): QueueView =
    queues
      .reduceOption { (a, b) =>
        val worst = if (a.thresholdRatio >= b.thresholdRatio) a else b
        QueueView(
          bufferCount = worst.bufferCount,
          backpressureThreshold = worst.backpressureThreshold,
          maxSize = worst.maxSize,
          thresholdRatio = worst.thresholdRatio,
          capacityRatio = worst.capacityRatio,
          totalProduced = a.totalProduced + b.totalProduced,
          totalCancellations = a.totalCancellations + b.totalCancellations,
          totalDropped = a.totalDropped + b.totalDropped,
          totalConsumed = a.totalConsumed + b.totalConsumed,
          productionRate = a.productionRate + b.productionRate,
          consumptionRate = a.consumptionRate + b.consumptionRate,
        )
      }
      .getOrElse(EmptyQueue)
}

object BackpressureStore {

  /** History kept per host. Only the two most recent samples feed a rate; the rest of the retention
    * exists so a member that stops reporting stays visible in the picker (tinted "not reporting") for
    * this long before it ages out.
    */
  val RetentionMs: Double = 5 * 60 * 1000.0

  /** A host whose latest sample is older than this, relative to the newest sample any host reported,
    * counts as not reporting: it dropped out, or missed the server's cluster broadcast timeout. Two
    * poll intervals, so one slow poll is not mistaken for a departure.
    */
  private val StaleHostMs: Double = 12 * 1000.0

  /** Below this span between the two most recent samples, a counter delta is dominated by measurement
    * jitter rather than throughput.
    */
  private val MinIntervalSec: Double = 0.5

  private val EmptyQueue: QueueView = QueueView(0, 0, 0, 0.0, 0.0, 0L, 0L, 0L, 0L, 0.0, 0.0)

  private def maxOr0(xs: Seq[Double]): Double = if (xs.isEmpty) 0.0 else xs.max

  /** Reduce one point's per-member levels to the worst, retaining attribution. Empty (no member
    * reported this point) reads as [[PressureLevel.Flowing]].
    */
  private def worstLevelAcross(per: Seq[(HostRef, PressureLevel)]): Aggregated[PressureLevel] =
    Aggregated(if (per.isEmpty) PressureLevel.Flowing else per.map(_._2).max, per)

  /** The rate from the two most recent cumulative readings of one series on one host:
    * `(last - prev) / elapsed`. This is the most responsive rate the data supports — one poll
    * interval — with no averaging over a longer window.
    *
    * `None` when there is only one sample, when the two are less than [[MinIntervalSec]] apart, or
    * when the counter went backwards — which within a single `hostKey` should be impossible, since a
    * restarted process is a new key, but is guarded anyway rather than reported as a negative rate.
    * Callers fall back to the server's EWMA.
    */
  private def latestRate(series: Seq[(Double, Long)]): Option[Double] = {
    val lastTwo = series.takeRight(2)
    if (lastTwo.sizeIs < 2) None
    else {
      val (prevT, prevC) = lastTwo.head
      val (lastT, lastC) = lastTwo.last
      val elapsedSec = (lastT - prevT) / 1000.0
      if (elapsedSec >= MinIntervalSec && lastC >= prevC) Some((lastC - prevC).toDouble / elapsedSec)
      else None
    }
  }
}

package com.thatdot.quine.webapp.dataservice

import com.thatdot.quine.webapp.dataservice.BackpressureService.Scope

/** Backpressure severity. Ordered, so combining across cluster members is `max` — the worst member
  * wins. Summing or averaging would let one saturated member of N sink below the diagram's
  * thresholds and hide the very bottleneck the page exists to surface.
  */
sealed abstract class PressureLevel(val severity: Int)
object PressureLevel {
  case object Flowing extends PressureLevel(0)
  case object Constrained extends PressureLevel(1)
  case object Backpressured extends PressureLevel(2)

  /** Unrecognized states read as [[Flowing]] rather than failing the decode: a snapshot with one
    * unfamiliar stage is still worth rendering, and the server owns this vocabulary.
    */
  def fromWire(s: String): PressureLevel = s match {
    case "BACKPRESSURED" => Backpressured
    case "CONSTRAINED" => Constrained
    case _ => Flowing
  }

  implicit val ordering: Ordering[PressureLevel] = Ordering.by(_.severity)
}

/** A host that contributed measurements, as the view refers to it.
  *
  * `memberIdx` is the user-facing *member* — what the scope picker selects and what a tooltip names.
  * `key` is the *process* identity, and the two are deliberately different: a member survives
  * failover and is refilled by a fresh process whose counters restart at zero, so counter history
  * must be keyed on the process, never the member.
  */
final case class HostRef(key: String, memberIdx: Option[Int]) {
  def label: String = memberIdx.fold("this host")(i => s"member $i")
}

/** A value reduced across cluster members that keeps its attribution.
  *
  * `combined` is the law-appropriate reduction (worst for levels, sum for counters); `perHost` is
  * always retained, so a tooltip can name *which* members are responsible rather than reporting only
  * that some unnamed member is at fault. When the scope is a single member, `perHost` has one entry
  * and `combined` equals it — the cluster and single-member views are the same rendering path, not
  * two.
  */
final case class Aggregated[A](combined: A, perHost: Seq[(HostRef, A)]) {

  /** Cluster members whose own value satisfies `p`, ascending. Empty in OSS, which has no members —
    * callers should fall back to `combined` rather than reporting "no members".
    */
  def membersWhere(p: A => Boolean): Seq[Int] =
    perHost.collect { case (h, a) if p(a) => h.memberIdx }.flatten.sorted

  def hostsWhere(p: A => Boolean): Seq[HostRef] =
    perHost.collect { case (h, a) if p(a) => h }
}

final case class ValveView(
  isOpen: Boolean,
  closedCount: Int,
  oneMinuteClosures: Int,
)

final case class PersistorView(
  `type`: String,
  writeLatencyMs: Double,
  readLatencyMs: Double,
)

/** One ingest stream, collapsed across whichever members run it.
  *
  * An ingest with the same name may run on more than one member; they are combined into one view —
  * the rate summed across members, each per-stage level reduced to the worst member (with attribution
  * retained), and `members` naming the members that contribute. Today an ingest runs on a single
  * member, so `members` usually has one entry; the shape does not assume that.
  */
final case class IngestView(
  name: String,
  namespace: String,
  sourceType: String,
  status: String,
  rateLimit: Option[Int],
  /** Summed across the members that run this ingest — each member's rate differenced from its own
    * counters first (see [[BackpressureStore]]).
    */
  rate: Double,
  totalCount: Long,
  /** Members running this ingest, ascending. Empty on OSS, which has no members. */
  members: Seq[Int],
  source: Aggregated[PressureLevel],
  preGraphWrite: Aggregated[PressureLevel],
  postGraphWrite: Option[Aggregated[PressureLevel]],
) {
  def isRunning: Boolean = status == "RUNNING"
}

final case class QueueView(
  bufferCount: Int,
  backpressureThreshold: Int,
  maxSize: Int,
  thresholdRatio: Double,
  capacityRatio: Double,
  totalProduced: Long,
  totalCancellations: Long,
  totalDropped: Long,
  totalConsumed: Long,
  productionRate: Double,
  consumptionRate: Double,
) {

  /** A standing query's queue is judged by how close it is to the threshold at which it closes the
    * global valve, not by a gauge — the queue is a depth, not a stream stage.
    *
    * This lives on the view rather than in the diagram because two consumers must agree on it: the
    * diagram, which colors the queue meter, and [[BackpressureStore.statusByMember]], which tints
    * that member's chip. A chip contradicting the diagram it selects is worse than either being
    * wrong, so there is one definition.
    */
  def level: PressureLevel =
    if (thresholdRatio >= 1.0) PressureLevel.Backpressured
    else if (thresholdRatio >= 0.4) PressureLevel.Constrained
    else PressureLevel.Flowing
}

/** One destination of one output. `index` (not `type`) is the identity: an output may have two
  * destinations of the same type, and only the index tells them apart — on this host and, more to
  * the point, across members.
  */
final case class DestinationView(
  index: Int,
  `type`: String,
  state: Aggregated[PressureLevel],
)

final case class SqOutputView(
  name: String,
  rate: Double,
  totalCount: Long,
  hasEnrichment: Boolean,
  enrichment: Option[Aggregated[PressureLevel]],
  destinations: Seq[DestinationView],
)

final case class StandingQueryView(
  name: String,
  namespace: String,
  queue: Aggregated[QueueView],
  outputs: Seq[SqOutputView],
)

/** The one shape the rendering layer consumes, already resolved for the chosen scope.
  *
  * Fixed regardless of that choice: a single member and the whole cluster differ in what is in
  * `hostsInScope`, never in the type. Consumers never see the raw per-host snapshot list and never
  * aggregate — that is all settled here.
  *
  * Every level is the most recent snapshot's state; every rate is differenced from the two most
  * recent snapshots. There is no lookback window — the view is what the system is doing now.
  */
final case class BackpressureView(
  scope: Scope,
  hostsInScope: Seq[HostRef],
  /** Hosts present in the most recent poll. A host in scope but not reporting has dropped out (or
    * missed the server's broadcast timeout), and its share of every summed rate is missing.
    */
  hostsReporting: Seq[HostRef],
  /** Every graph in scope, ascending — including idle ones that run no ingests or standing queries
    * and so contribute nothing to `ingests`/`standingQueries`. The graph list the diagram offers to
    * filter is drawn from this, not inferred from the pipelines that happen to be running.
    */
  namespaces: Seq[String],
  globalValve: Aggregated[ValveView],
  ingests: Seq[IngestView],
  standingQueries: Seq[StandingQueryView],
  persistor: Aggregated[PersistorView],
) {

  /** True when some host in scope did not report in the latest poll: summed rates undercount. */
  def hasMissingHosts: Boolean = hostsReporting.size < hostsInScope.size

  /** True when this view reduces more than one host into each figure it reports.
    *
    * Not the same question as `scope == Cluster`: OSS and a single-node deployment resolve
    * `Scope.Cluster` over exactly one host, and nothing there is aggregated. Callers that must
    * suppress a figure *because* it was reduced — the bottleneck indicator, which names one stage
    * but cannot say which member it is on — must ask this, not the scope.
    */
  def isAggregate: Boolean = hostsInScope.sizeIs > 1

  /** The worst level anything in this view is at, right now.
    *
    * Resolved from `combined`, so over a single-member scope this is exactly that member's own worst
    * — which is what makes it safe to tint a member's chip with the value computed from that
    * member's view. See [[BackpressureStore.statusByMember]].
    */
  def worstLevel: PressureLevel = {
    val levels: Seq[PressureLevel] =
      ingests.flatMap(i => Seq(i.source.combined, i.preGraphWrite.combined) ++ i.postGraphWrite.map(_.combined)) ++
      standingQueries.flatMap { sq =>
        sq.queue.combined.level +: sq.outputs.flatMap { o =>
          o.enrichment.map(_.combined).toSeq ++ o.destinations.map(_.state.combined)
        }
      }
    if (levels.isEmpty) PressureLevel.Flowing else levels.max
  }
}

/** One cluster member's headline state, for the member picker — which must show every member at
  * once, and so cannot be read off a [[BackpressureView]] (that is resolved to the *selected* scope,
  * and knows nothing about the members it excluded).
  *
  * `isReporting` is deliberately separate from `worst` rather than folded in as a fourth severity: a
  * member that has dropped out is not "very backpressured", it is *unmeasured*, and its last known
  * `worst` is stale rather than wrong. The picker renders not-reporting as its own color.
  */
final case class MemberStatus(worst: PressureLevel, isReporting: Boolean)

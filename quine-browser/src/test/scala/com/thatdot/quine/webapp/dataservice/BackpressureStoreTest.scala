package com.thatdot.quine.webapp.dataservice

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

import com.thatdot.quine.webapp.dataservice.BackpressureService.Scope
import com.thatdot.quine.webapp.v2api.V2ApiTypes._

/** The cluster model the store is responsible for: reduce across members with attribution retained,
  * rate from the two most recent snapshots, level from the latest snapshot. No lookback window.
  */
class BackpressureStoreTest extends AnyFunSuite with Matchers with OptionValues with LoneElement {

  private val T0 = 1_000_000.0

  private def host(port: Int, memberIdx: Option[Int], pid: Long = 1L): V2HostInfo =
    V2HostInfo(version = "test", address = "10.0.0.1", port = port, pid = pid, memberIdx = memberIdx)

  private def dest(index: Int, state: String): V2Destination =
    V2Destination(index = index, `type` = "Kafka", state = state)

  private def output(name: String, totalCount: Long, destinations: Seq[V2Destination]): V2SqOutput =
    V2SqOutput(
      name = name,
      rate = 0.0, // server EWMA; the store prefers its own two-snapshot rate
      totalCount = totalCount,
      hasEnrichment = false,
      enrichmentState = None,
      destinations = destinations,
    )

  private def queue(produced: Long, consumed: Long, buffered: Int): V2SqQueue =
    V2SqQueue(
      bufferCount = buffered,
      backpressureThreshold = 100,
      maxSize = 1000,
      thresholdRatio = buffered.toDouble / 100,
      capacityRatio = buffered.toDouble / 1000,
      totalProduced = produced,
      totalCancellations = 0L,
      totalDropped = 0L,
      totalConsumed = consumed,
      productionRate = 0.0,
      consumptionRate = 0.0,
    )

  private def ingest(
    name: String,
    totalCount: Long,
    status: String = "RUNNING",
    source: String = "FLOWING",
    namespace: String = "default",
    rateLimit: Option[Int] = None,
  ) =
    V2IngestSnapshot(
      name = name,
      namespace = namespace,
      sourceType = "NumberIterator",
      status = status,
      rateLimit = rateLimit,
      rate = 0.0,
      totalCount = totalCount,
      stages = V2IngestStages(source = source, preGraphWrite = "FLOWING", postGraphWrite = None),
    )

  private def snap(
    h: V2HostInfo,
    ts: Double,
    ingests: Seq[V2IngestSnapshot] = Seq.empty,
    sqs: Seq[V2StandingQuery] = Seq.empty,
    valveOpen: Boolean = true,
    namespaces: Seq[String] = Seq("default"),
  ): V2BackpressureSnapshot =
    V2BackpressureSnapshot(
      timestamp = ts,
      host = h,
      namespaces = namespaces,
      globalValve = V2GlobalValve(isOpen = valveOpen, closedCount = if (valveOpen) 0 else 1, oneMinuteClosures = 0),
      ingests = ingests,
      standingQueries = sqs,
      persistor = V2Persistor(`type` = "rocksdb", writeLatencyMs = 1.0, readLatencyMs = 1.0),
    )

  private def sq(
    name: String,
    q: V2SqQueue,
    outs: Seq[V2SqOutput],
    namespace: String = "default",
  ): V2StandingQuery =
    V2StandingQuery(name = name, namespace = namespace, queue = q, outputs = outs)

  private val h0 = host(8080, Some(0))
  private val h1 = host(8081, Some(1))
  private val h2 = host(8082, Some(2))

  test("a backpressured destination is attributed to the members actually responsible") {
    val store = new BackpressureStore
    // Same standing query on all three members — member 1 is the only one struggling.
    def poll(ts: Double): Seq[V2BackpressureSnapshot] = Seq(
      snap(h0, ts, sqs = Seq(sq("q", queue(10, 10, 0), Seq(output("out", 10, Seq(dest(0, "FLOWING"))))))),
      snap(h1, ts, sqs = Seq(sq("q", queue(10, 10, 0), Seq(output("out", 10, Seq(dest(0, "BACKPRESSURED"))))))),
      snap(h2, ts, sqs = Seq(sq("q", queue(10, 10, 0), Seq(output("out", 10, Seq(dest(0, "FLOWING"))))))),
    )
    store.record(poll(T0))
    store.record(poll(T0 + 5000))

    val d = store.view(Scope.Cluster).value.standingQueries.loneElement.outputs.loneElement.destinations.loneElement

    // Worst member wins the color — one saturated member must not be averaged away.
    d.state.combined shouldBe PressureLevel.Backpressured
    // ...and we can say WHICH member, which is the entire point.
    d.state.membersWhere(_ != PressureLevel.Flowing) shouldBe Seq(1)
  }

  test("rates are differenced per host and only then summed across the cluster") {
    val store = new BackpressureStore
    // Two members, each ingesting its own stream. 100 events per host over 10s = 10/s per host.
    store.record(
      Seq(
        snap(h0, T0, ingests = Seq(ingest("a", 0))),
        snap(h1, T0, ingests = Seq(ingest("b", 0))),
      ),
    )
    store.record(
      Seq(
        snap(h0, T0 + 10000, ingests = Seq(ingest("a", 100))),
        snap(h1, T0 + 10000, ingests = Seq(ingest("b", 100))),
      ),
    )

    val view = store.view(Scope.Cluster).value
    view.ingests.map(_.name).sorted shouldBe Seq("a", "b")
    view.ingests.foreach(_.rate shouldBe 10.0)
    // Each ingest names the member it runs on.
    view.ingests.find(_.name == "a").value.members shouldBe Seq(0)
    view.ingests.find(_.name == "b").value.members shouldBe Seq(1)
  }

  test("rate comes from the two most recent snapshots, not an average over more") {
    val store = new BackpressureStore
    // A burst, then a lull: 100 events in the first 5s, then only 10 in the next 5s.
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0)))))
    store.record(Seq(snap(h0, T0 + 5000, ingests = Seq(ingest("a", 100)))))
    store.record(Seq(snap(h0, T0 + 10000, ingests = Seq(ingest("a", 110)))))

    // The two most recent samples are the lull: 10 events over 5s = 2/s. Averaging over all three
    // would report 11/s — the responsiveness the two-snapshot rate is chosen for.
    store.view(Scope.Cluster).value.ingests.loneElement.rate shouldBe 2.0
  }

  test("an ingest of the same name on several members collapses into one, with the rate summed") {
    val store = new BackpressureStore
    // The SAME ingest on both members: member 0 at 10/s, member 1 at 20/s.
    store.record(
      Seq(
        snap(h0, T0, ingests = Seq(ingest("shared", 0))),
        snap(h1, T0, ingests = Seq(ingest("shared", 0))),
      ),
    )
    store.record(
      Seq(
        snap(h0, T0 + 10000, ingests = Seq(ingest("shared", 100))),
        snap(h1, T0 + 10000, ingests = Seq(ingest("shared", 200))),
      ),
    )

    val ingestView = store.view(Scope.Cluster).value.ingests.loneElement
    ingestView.name shouldBe "shared"
    ingestView.rate shouldBe 30.0 // 10 + 20
    ingestView.totalCount shouldBe 300L
    ingestView.members shouldBe Seq(0, 1) // both members named, for the tooltip
  }

  test("deleting a collapsed ingest on one member drops it from the aggregate on the next poll") {
    val store = new BackpressureStore
    // The same ingest on both members, each running at 20/s.
    store.record(
      Seq(
        snap(h0, T0, ingests = Seq(ingest("shared", 0))),
        snap(h1, T0, ingests = Seq(ingest("shared", 0))),
      ),
    )
    store.record(
      Seq(
        snap(h0, T0 + 10000, ingests = Seq(ingest("shared", 200))),
        snap(h1, T0 + 10000, ingests = Seq(ingest("shared", 200))),
      ),
    )
    store.view(Scope.Cluster).value.ingests.loneElement.rate shouldBe 40.0 // 20 + 20, both members

    // Member 1 deletes the ingest: its next snapshot no longer carries it, while member 0 keeps
    // running at 20/s. The older samples that still mention it on member 1 are retained only for rate
    // differencing and must NOT keep it in the aggregate — regression for a deleted ingest lingering
    // at its last-seen rate, with its member still named, for the whole retention window.
    store.record(
      Seq(
        snap(h0, T0 + 20000, ingests = Seq(ingest("shared", 400))),
        snap(h1, T0 + 20000, ingests = Seq.empty),
      ),
    )

    val ingestView = store.view(Scope.Cluster).value.ingests.loneElement
    ingestView.rate shouldBe 20.0 // member 0 alone, not the stale 40
    ingestView.members shouldBe Seq(0) // member 1 is gone from the tooltip
    // The member-scoped views stay correct throughout.
    store.view(Scope.Member(0)).value.ingests.loneElement.rate shouldBe 20.0
    store.view(Scope.Member(1)).value.ingests shouldBe empty
  }

  test("a collapsed ingest's rate limit is summed too, so it is on the same basis as the rate") {
    val store = new BackpressureStore
    // The same ingest, capped at 1/s, on both members — each running at its cap.
    def poll(ts: Double, count: Long): Seq[V2BackpressureSnapshot] = Seq(
      snap(h0, ts, ingests = Seq(ingest("shared", count, rateLimit = Some(1)))),
      snap(h1, ts, ingests = Seq(ingest("shared", count, rateLimit = Some(1)))),
    )
    store.record(poll(T0, 0))
    store.record(poll(T0 + 1000, 1)) // 1 event in 1s on each member

    val ingestView = store.view(Scope.Cluster).value.ingests.loneElement
    ingestView.rate shouldBe 2.0 // summed across both members
    // The cap is summed too: two members each capped at 1/s cap the cluster at 2/s. Without this the
    // card would read "2/s of 1/s" — a cluster throughput against a single member's limit.
    ingestView.rateLimit shouldBe Some(2)
  }

  test("a collapsed ingest reads as unlimited if any member is unthrottled") {
    val store = new BackpressureStore
    def poll(ts: Double, count: Long): Seq[V2BackpressureSnapshot] = Seq(
      snap(h0, ts, ingests = Seq(ingest("shared", count, rateLimit = Some(1)))),
      snap(h1, ts, ingests = Seq(ingest("shared", count, rateLimit = None))), // no cap on this member
    )
    store.record(poll(T0, 0))
    store.record(poll(T0 + 1000, 1))

    // A partial cap is no cluster-wide cap, so no "of y/s" is shown at all.
    store.view(Scope.Cluster).value.ingests.loneElement.rateLimit shouldBe None
  }

  test("a member joining does not manufacture a throughput spike") {
    val store = new BackpressureStore
    // One member ingesting steadily at 10/s.
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0)))))
    store.record(Seq(snap(h0, T0 + 10000, ingests = Seq(ingest("a", 100)))))

    val before = store.view(Scope.Cluster).value
    before.ingests.loneElement.rate shouldBe 10.0

    // A second member joins, already carrying a large cumulative count from before it was visible.
    // Summing raw counters across hosts and *then* differencing would read that as a huge spike;
    // differencing each host against its own history cannot.
    store.record(
      Seq(
        snap(h0, T0 + 20000, ingests = Seq(ingest("a", 200))),
        snap(h1, T0 + 20000, ingests = Seq(ingest("b", 1_000_000))),
      ),
    )

    val after = store.view(Scope.Cluster).value
    // "a" keeps its true rate; the newcomer has no history yet and falls back to the server EWMA (0.0),
    // rather than reporting a million-event spike.
    after.ingests.find(_.name == "a").value.rate shouldBe 10.0
    after.ingests.find(_.name == "b").value.rate shouldBe 0.0
  }

  test("standing-query counters are summed across the members that run it") {
    val store = new BackpressureStore
    def poll(ts: Double, produced: Long): Seq[V2BackpressureSnapshot] = Seq(
      snap(
        h0,
        ts,
        sqs = Seq(sq("q", queue(produced, produced, 0), Seq(output("out", produced, Seq(dest(0, "FLOWING")))))),
      ),
      snap(
        h1,
        ts,
        sqs = Seq(sq("q", queue(produced, produced, 0), Seq(output("out", produced, Seq(dest(0, "FLOWING")))))),
      ),
    )
    store.record(poll(T0, 0))
    store.record(poll(T0 + 10000, 100))

    val q = store.view(Scope.Cluster).value.standingQueries.loneElement
    // The same query runs on both members, so its cumulative counters are a cluster-wide sum...
    q.queue.combined.totalProduced shouldBe 200L
    // ...and so is its rate: 10/s on each of two members.
    q.queue.combined.productionRate shouldBe 20.0
    q.outputs.loneElement.rate shouldBe 20.0
    q.outputs.loneElement.totalCount shouldBe 200L
  }

  test("the worst member's queue supplies every instantaneous field, keeping them mutually consistent") {
    val store = new BackpressureStore
    store.record(
      Seq(
        snap(h0, T0, sqs = Seq(sq("q", queue(0, 0, buffered = 5), Seq.empty))),
        snap(h1, T0, sqs = Seq(sq("q", queue(0, 0, buffered = 90), Seq.empty))),
      ),
    )

    val q = store.view(Scope.Cluster).value.standingQueries.loneElement.queue
    // Buffer count and its ratios all come from the same (worst) member — a tooltip showing 90
    // buffered alongside a 5% fill ratio would be incoherent.
    q.combined.bufferCount shouldBe 90
    q.combined.thresholdRatio shouldBe 0.9
    q.membersWhere(_.bufferCount >= 90) shouldBe Seq(1)
  }

  test("scoping to one member reduces over just that member") {
    val store = new BackpressureStore
    store.record(
      Seq(
        snap(h0, T0, ingests = Seq(ingest("a", 0))),
        snap(h1, T0, ingests = Seq(ingest("b", 0))),
      ),
    )
    store.record(
      Seq(
        snap(h0, T0 + 10000, ingests = Seq(ingest("a", 100))),
        snap(h1, T0 + 10000, ingests = Seq(ingest("b", 500))),
      ),
    )

    val only1 = store.view(Scope.Member(1)).value
    only1.hostsInScope.map(_.memberIdx) shouldBe Seq(Some(1))
    only1.ingests.loneElement.name shouldBe "b"
    only1.ingests.loneElement.rate shouldBe 50.0
  }

  test("recording is idempotent, so a replayed poll cannot corrupt the rates") {
    val store = new BackpressureStore
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0)))))
    store.record(Seq(snap(h0, T0 + 10000, ingests = Seq(ingest("a", 100)))))

    val expected = store.view(Scope.Cluster).value.ingests.loneElement.rate

    // Airstream re-runs a mapping function when a signal restarts; the same snapshot arrives twice.
    store.record(Seq(snap(h0, T0 + 10000, ingests = Seq(ingest("a", 100)))))
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0))))) // and out of order, for good measure

    store.view(Scope.Cluster).value.ingests.loneElement.rate shouldBe expected
  }

  test("a paused ingest does not read as backpressured") {
    val store = new BackpressureStore
    // A stopped stream's source gauge is never pulled, so it sits at its initial "backpressured"
    // value. Reporting that as a bottleneck would be a lie: the ingest is not stuck, it is stopped.
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 5, status = "PAUSED", source = "BACKPRESSURED")))))
    store.record(
      Seq(snap(h0, T0 + 5000, ingests = Seq(ingest("a", 5, status = "PAUSED", source = "BACKPRESSURED")))),
    )

    val p = store.view(Scope.Cluster).value.ingests.loneElement
    p.status shouldBe "PAUSED"
    // No RUNNING member contributes a level, so the reduction is empty and reads as Flowing.
    p.source.combined shouldBe PressureLevel.Flowing
    p.source.perHost shouldBe empty
  }

  test("a member that stops reporting is visible as missing rather than silently dropped") {
    val store = new BackpressureStore
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0))), snap(h1, T0, ingests = Seq(ingest("b", 0)))))
    // Only h0 answers the next poll — h1 missed the server's broadcast timeout, or left.
    store.record(Seq(snap(h0, T0 + 30000, ingests = Seq(ingest("a", 100)))))

    val view = store.view(Scope.Cluster).value
    view.hostsInScope.map(_.memberIdx) should contain allOf (Some(0), Some(1))
    view.hostsReporting.map(_.memberIdx) shouldBe Seq(Some(0))
    view.hasMissingHosts shouldBe true // summed rates undercount, and the UI can say so
  }

  // ── Member statuses: what the member picker tints its chips with ──

  test("a member's status is the worst thing on that member, and does not leak across members") {
    val store = new BackpressureStore
    def poll(ts: Double): Seq[V2BackpressureSnapshot] = Seq(
      snap(h0, ts, sqs = Seq(sq("q", queue(10, 10, 0), Seq(output("out", 10, Seq(dest(0, "FLOWING"))))))),
      snap(h1, ts, sqs = Seq(sq("q", queue(10, 10, 0), Seq(output("out", 10, Seq(dest(0, "BACKPRESSURED"))))))),
    )
    store.record(poll(T0))
    store.record(poll(T0 + 5000))

    val statuses = store.statusByMember
    // The cluster aggregate is backpressured — but only one member is, and the picker must say so.
    // Tinting both chips would send the user to the wrong member.
    statuses(0).worst shouldBe PressureLevel.Flowing
    statuses(1).worst shouldBe PressureLevel.Backpressured
    statuses.values.foreach(_.isReporting shouldBe true)
  }

  test("a member's status agrees with the view its chip selects") {
    val store = new BackpressureStore
    // Nothing is backpressured; the only pressure anywhere is a queue at 90% of its threshold, which
    // is Constrained *only* because QueueView.level says so. The chip and the queue meter read that
    // from the same definition, so a chip cannot contradict the diagram it selects.
    store.record(Seq(snap(h0, T0, sqs = Seq(sq("q", queue(0, 0, buffered = 90), Seq.empty)))))

    val member0 = store.view(Scope.Member(0)).value
    member0.standingQueries.loneElement.queue.combined.level shouldBe PressureLevel.Constrained
    store.statusByMember(0).worst shouldBe member0.worstLevel
    store.statusByMember(0).worst shouldBe PressureLevel.Constrained
  }

  test("a member that stops reporting is marked down, not left at its last healthy status") {
    val store = new BackpressureStore
    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0))), snap(h1, T0, ingests = Seq(ingest("b", 0)))))
    // h1 goes away. Its last known state was healthy, and it stays healthy-looking — but "we have
    // not heard from it in 30s" is the thing the picker has to show, and staleness is judged against
    // the newest sample ANY member reported. Judged within h1's own scope it would be its own latest
    // poll, and so would read as reporting forever.
    store.record(Seq(snap(h0, T0 + 30000, ingests = Seq(ingest("a", 100)))))

    val statuses = store.statusByMember
    statuses(0).isReporting shouldBe true
    statuses(1).isReporting shouldBe false
    statuses(1).worst shouldBe PressureLevel.Flowing // last known — stale, not wrong
  }

  test("a member refilled by a fresh process is not double-counted with the process it replaced") {
    val store = new BackpressureStore
    val dying = host(8081, memberIdx = Some(1), pid = 1L)
    val reborn = host(8081, memberIdx = Some(1), pid = 2L) // same member, new process

    store.record(Seq(snap(h0, T0, ingests = Seq(ingest("a", 0))), snap(dying, T0, ingests = Seq(ingest("b", 0)))))
    store.record(
      Seq(
        snap(h0, T0 + 10000, ingests = Seq(ingest("a", 100))),
        // Member 1 died backpressured, and its history is retained under its old pid.
        snap(dying, T0 + 10000, ingests = Seq(ingest("b", 500, source = "BACKPRESSURED"))),
      ),
    )
    // Failover: a fresh process takes member 1, counters restarting at zero, healthy.
    store.record(
      Seq(
        snap(h0, T0 + 20000, ingests = Seq(ingest("a", 200))),
        snap(reborn, T0 + 20000, ingests = Seq(ingest("b", 0))),
      ),
    )
    store.record(
      Seq(
        snap(h0, T0 + 30000, ingests = Seq(ingest("a", 300))),
        snap(reborn, T0 + 30000, ingests = Seq(ingest("b", 100))),
      ),
    )

    // The member is held by one process, so the picker offers it once...
    store.currentMembers shouldBe Seq(0, 1)
    // ...and it is tinted by the process that holds it now, not by the state its predecessor died in.
    store.statusByMember(1).worst shouldBe PressureLevel.Flowing
    store.statusByMember(1).isReporting shouldBe true

    // The dead process contributes nothing to the cluster: one "b", not two, and its 500 frozen
    // events are not summed into its successor's 100.
    val view = store.view(Scope.Cluster).value
    view.hostsInScope should have size 2
    view.ingests.map(_.name).sorted shouldBe Seq("a", "b")
    view.ingests.find(_.name == "b").value.totalCount shouldBe 100L
  }

  test("a name is not an identity: same-named streams in different graphs stay apart") {
    val store = new BackpressureStore
    // Two graphs, each with an ingest called "orders" and a standing query called "q". Only the
    // billing graph's is struggling. The diagram keys its bottleneck, its clip-path ids and its
    // ribbon matching on (namespace, name) precisely because this is representable.
    def poll(ts: Double): Seq[V2BackpressureSnapshot] = Seq(
      snap(
        h0,
        ts,
        ingests = Seq(
          ingest("orders", 10, namespace = "default"),
          ingest("orders", 20, namespace = "billing", source = "BACKPRESSURED"),
        ),
        sqs = Seq(
          sq("q", queue(0, 0, buffered = 0), Seq(output("out", 5, Seq(dest(0, "FLOWING")))), namespace = "default"),
          sq("q", queue(0, 0, buffered = 90), Seq(output("out", 7, Seq(dest(0, "FLOWING")))), namespace = "billing"),
        ),
      ),
    )
    store.record(poll(T0))
    store.record(poll(T0 + 5000))

    val view = store.view(Scope.Cluster).value

    // Two separate ingests, not one collapsed onto the other.
    view.ingests should have size 2
    view.ingests.find(_.namespace == "default").value.source.combined shouldBe PressureLevel.Flowing
    view.ingests.find(_.namespace == "billing").value.source.combined shouldBe PressureLevel.Backpressured

    // ...and two separate standing queries, each with its own queue.
    view.standingQueries should have size 2
    view.standingQueries.find(_.namespace == "default").value.queue.combined.level shouldBe PressureLevel.Flowing
    view.standingQueries.find(_.namespace == "billing").value.queue.combined.level shouldBe PressureLevel.Constrained
  }
}

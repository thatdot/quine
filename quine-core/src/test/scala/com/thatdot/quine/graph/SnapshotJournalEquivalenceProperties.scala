package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.LiteralMessage.{DistinctIdIndexState, DistinctIdSubscriberState}
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  HalfEdge,
  PropertyComparisonFunctions,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceConfig, PrimePersistor}
import com.thatdot.quine.util.TestLogging._

/** A node rebuilt from its journal is the node a snapshot would have rebuilt, over generated workloads.
  *
  * `SnapshotJournalEquivalenceTest` states this for one hand-built workload. This states it for workloads drawn
  * from every operation the DistinctId bookkeeping responds to: property and edge writes and removals, queries
  * registered while nodes are awake and cancelled while they are awake, and sleeps at any point. The same
  * schedule runs on three graphs that differ only in when they snapshot, and every node they restore has to agree.
  *
  * Every case builds its own three graphs and shuts them down. Sharing graphs across cases was tried twice and
  * produced counterexamples that could not be reproduced: a case inherits whatever an earlier one left behind,
  * such as registered queries or a wedged shard, and a shrunk schedule then describes a situation that does not
  * exist on a fresh graph. A reduction is only worth reading if the graph it ran on was new.
  *
  * Abstract over the persistor so the same property runs on a real store: journal order, snapshot keys and
  * read-back all belong to the persistor, and the in-memory one exercises none of its edges.
  */
abstract class SnapshotJournalEquivalenceProperties(
  persistorLabel: String,
  makePersistor: (PersistenceConfig, ActorSystem) => PrimePersistor,
) extends AnyFunSuite
    with Matchers
    with ScalaCheckPropertyChecks {

  import SnapshotJournalEquivalenceProperties._

  implicit val timeout: Timeout = Timeout(30.seconds)
  val namespace: NamespaceId = defaultNamespaceId
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  private class Rig(val label: String, persistenceConfig: PersistenceConfig, caseNo: Int) {
    val graph: GraphService = {
      val g = Await.result(
        GraphService(
          s"equivalence-$persistorLabel-$label-$caseNo",
          effectOrder = EventEffectOrder.PersistorFirst,
          persistorMaker = system => makePersistor(persistenceConfig, system),
          idProvider = idProvider,
          declineSleepWhenWriteWithinMillis = 0L,
        ),
        timeout.duration,
      )
      g.requiredGraphIsReady()
      g
    }
    // Which query currently stands for each pattern key on this graph, so a cancel can find it.
    val registered: mutable.Map[String, StandingQueryId] = mutable.Map.empty

    /** Every standing query this rig has ever registered, labelled by the pattern it was registered for.
      *
      * Standing query ids are random and minted per rig, so they can never be compared between rigs. The pattern
      * key can: both rigs run the same schedule and so register the same keys. Cumulative rather than following
      * `registered`, because a cancelled query's id can still appear in state that has not been swept yet.
      */
    val queryLabels: mutable.Map[StandingQueryId, String] = mutable.Map.empty
    // Every result each pattern's query has emitted on this graph, across registrations.
    val results: mutable.Map[String, ConcurrentLinkedQueue[StandingQueryResult]] = mutable.Map.empty
    def shutdown(): Unit = Await.result(graph.shutdown(), timeout.duration)
  }

  /** The snapshot policies every schedule is run under, built one at a time. The first is the reference the others
    * are compared against.
    */
  private val rigSpecs: List[(String, PersistenceConfig)] = List(
    "always" -> PersistenceConfig(snapshotAfterEvents = 0),
    // Low enough that a schedule of at most twelve operations over three nodes can cross it, so this rig is the one
    // where a node restores from a snapshot and then a journal tail. At sixteen it never snapshotted and was the
    // "never" rig under another name.
    "threshold" -> PersistenceConfig(snapshotAfterEvents = 4),
    "never" -> PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
  )

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  /** Patterns are keyed so the same key names the same content-hashed DGN on every graph and across cases. */
  private def pattern(p: Pattern): SingleBranch = p match {
    case OneHop(prop) => SingleBranch(hasProperty(prop), nextBranches = Nil)
    case TwoHop(prop, childProp) => hop(prop, pattern(OneHop(childProp)))
    case ThreeHop(prop, childProp, grandchildProp) => hop(prop, pattern(TwoHop(childProp, grandchildProp)))
  }

  private def hop(prop: String, child: SingleBranch): SingleBranch =
    SingleBranch(
      hasProperty(prop),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, child)),
    )

  private def register(rig: Rig, p: Pattern): Unit = if (!rig.registered.contains(p.key)) {
    val sqId = StandingQueryId.fresh()
    val dgnPackage = pattern(p).toDomainGraphNodePackage
    Await.result(
      rig.graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    val sqns = rig.graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    // Consumed as they arrive: results with no consumer back up the query's hub, and a full hub closes the
    // graph's ingest valve, after which every write and wake stalls.
    val captured = rig.results.getOrElseUpdate(p.key, new ConcurrentLinkedQueue[StandingQueryResult]())
    val drain: org.apache.pekko.stream.scaladsl.Sink[StandingQueryResult, org.apache.pekko.stream.UniqueKillSwitch] =
      org.apache.pekko.stream.scaladsl
        .Flow[StandingQueryResult]
        .viaMat(org.apache.pekko.stream.KillSwitches.single)(org.apache.pekko.stream.scaladsl.Keep.right)
        .map { r =>
          captured.add(r)
          MasterStream.SqResultsExecToken(s"equivalence-${p.key}")
        }
        .to(Sink.ignore)
    sqns.createStandingQuery(
      name = p.key,
      pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
        dgnId = dgnPackage.dgnId,
        formatReturnAsStr = false,
        aliasReturnAs = Symbol("id"),
        includeCancellation = false,
        origin = PatternOrigin.DirectDgb,
      ),
      outputs = Map("drain" -> drain),
      sqId = sqId,
    )
    // Awake nodes take the awake-node path; the rest meet the query at their next wake.
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    rig.registered(p.key) = sqId
    rig.queryLabels(sqId) = p.key
  }

  private def cancel(rig: Rig, p: Pattern): Unit = rig.registered.remove(p.key).foreach { sqId =>
    val sqns = rig.graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, timeout.duration))
    // Awake nodes drop the query on this path; the rest find it gone at their next wake.
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
  }

  private def awake(g: GraphService, qid: QuineId): Boolean =
    Await.result(
      g.relayAsk(
        g.shardFromNode(qid).quineRef,
        ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
      ).flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(g.materializer))(
        ExecutionContext.parasitic,
      ).map(_.contains(qid))(ExecutionContext.parasitic),
      timeout.duration,
    )

  private def sleepsCompleted(g: GraphService): Long =
    g.metrics.metricRegistry.getCounters.asScala.collect {
      case (name, counter) if name.endsWith("sleep-counters.slept-success") => counter.getCount
    }.sum

  /** Counted rather than observed, as in `DistinctIdReplayProperties`: a message in flight wakes the node straight
    * back, and that is a sleep that happened. A node the schedule has not touched is not awake and is left alone.
    */
  private def sleepNode(g: GraphService, qid: QuineId): Unit = if (awake(g, qid)) {
    val before = sleepsCompleted(g)
    Await.result(g.requestNodeSleep(namespace, qid), timeout.duration)
    // `requestNodeSleep` completing says the request was accepted; the counter moving is the sleep itself.
    // The sleep counter is an in-memory read, so this can be re-tested far more often than an ask can.
    val sleepDeadline = System.nanoTime() + deadlockTimeout.toNanos
    while (sleepsCompleted(g) == before && System.nanoTime() < sleepDeadline) Thread.sleep(5)
    if (sleepsCompleted(g) == before) fail(s"node never slept when asked; the schedule would be a lie")
  }

  /** The single duration in this suite, and a safety valve rather than a tuning knob: it exists so that a case
    * which can never reach its condition fails instead of hanging. Nothing about pass or fail depends on it.
    */
  private val deadlockTimeout: FiniteDuration = 60.seconds

  /** How often to re-test a condition. Not a wait: the loop ends when the condition holds. It is this coarse
    * because the conditions here are `logState` asks to the very actors being waited on -- re-testing every couple
    * of milliseconds starves them and makes the wait longer, not shorter.
    */
  private val recheckInterval: FiniteDuration = 25.milliseconds

  private def pollUntil(condition: => Boolean): Boolean = {
    val deadline = System.nanoTime() + deadlockTimeout.toNanos
    var held = condition
    while (!held && System.nanoTime() < deadline) {
      Thread.sleep(recheckInterval.toMillis)
      held = condition
    }
    held
  }

  /** Block until `actual` reaches `expected`, then hand back what was read so the caller's matcher reports it.
    *
    * Only for a value that is *owed* -- a report count a schedule guarantees. Not for an equality between two
    * independently-evolving graphs: when such an equality is genuinely false, waiting for it costs the whole
    * deadlock timeout before the assertion gets to say so, which turns every real difference into a stall.
    */
  private def awaitingValue[A](expected: => A)(actual: => A): A = {
    val _ = pollUntil(actual == expected)
    actual
  }

  /** Total work a graph has recorded doing: every persisted event, snapshot, sleep and wake ticks one of these.
    *
    * Monotonic, so two equal readings mean nothing happened in between. Read from the metrics registry, which
    * costs no message and cannot wake a sleeping node -- the reason this is not done by polling the nodes is that
    * reading a node is what restores it, and the restore is what this suite is about.
    */
  private def activity(g: GraphService): Long = {
    val registry = g.metrics.metricRegistry
    registry.getCounters.asScala.values.map(_.getCount).sum +
    registry.getTimers.asScala.values.map(_.getCount).sum
  }

  /** Block until `g` stops doing work.
    *
    * This is an observation about the program, not about the clock: the loop ends when the counter stops moving,
    * and a slower machine simply takes more iterations to get there. What it cannot distinguish is "finished" from
    * "paused long enough to look finished", which is why this suite does not run its tests concurrently -- under
    * contention that distinction is exactly what breaks.
    */
  private def settle(g: GraphService): Unit = {
    val deadline = System.nanoTime() + deadlockTimeout.toNanos
    var quietPolls = 0
    var last = activity(g)
    while (quietPolls < 3 && System.nanoTime() < deadline) {
      Thread.sleep(recheckInterval.toMillis)
      val now = activity(g)
      if (now == last) quietPolls += 1 else { quietPolls = 0; last = now }
    }
  }

  /** On a timed-out wait, print every thread that is inside our code or the actor system, then rethrow. */
  private def dumpingThreadsOnTimeout[A](body: => A): A =
    try body
    catch {
      case e: java.util.concurrent.TimeoutException =>
        System.err.println(
          s"DIAG TIMEOUT ${e.getMessage} awaiting at\n    " +
          e.getStackTrace.filter(_.getClassName.contains("thatdot")).take(6).mkString("\n    "),
        )
        Thread.getAllStackTraces.asScala.foreach { case (t, frames) =>
          val ours = frames.filter(f => f.getClassName.contains("thatdot") || f.getClassName.contains("pekko"))
          if (ours.nonEmpty)
            System.err.println(
              s"DIAG THREAD ${t.getName} [${t.getState}]\n    " + ours.take(14).mkString("\n    "),
            )
        }
        throw e
    }

  private def applyOp(rig: Rig, nodes: IndexedSeq[QuineId], op: Op, i: Int): Unit = {
    val ops = rig.graph.literalOps(namespace)
    op match {
      case SetProp(n, key) => Await.result(ops.setProp(nodes(n), key, QuineValue.Str(s"v$i")), timeout.duration)
      case RemoveProp(n, key) => Await.result(ops.removeProp(nodes(n), key), timeout.duration)
      case AddEdge(from, to) => Await.result(ops.addEdge(nodes(from), nodes(to), "to"), timeout.duration)
      case RemoveEdge(from, to) => Await.result(ops.removeEdge(nodes(from), nodes(to), "to"), timeout.duration)
      case Register(p) => register(rig, p)
      case Cancel(p) => cancel(rig, p)
      case Sleep(n) => sleepNode(rig.graph, nodes(n))
      // Propagation is waited on by condition where it matters, so this is a no-op the generator may still emit.
      case Settle => settle(rig.graph)
    }
  }

  /** The parts of a restored node that must not depend on how it was restored. Excluded by definition:
    * `latestUpdateMillisAfterSnapshot`, the journal itself, and the structures rebuilt from scratch at every wake.
    */
  private type Observable = (
    Map[Symbol, String],
    Set[HalfEdge],
    Set[(Long, Option[QuineId], Option[String], Set[String], Option[Boolean])],
    Set[(Long, QuineId, Set[String], Option[Boolean])],
    Long,
  )

  private def observe(rig: Rig, qid: QuineId): Observable = {
    val g = rig.graph
    val state = Await.result(g.literalOps(namespace).logState(qid), timeout.duration)
    // Both lists carry the query sets that decide whether either end of a subscription is still wanted, so they
    // are compared too: a restore that brought the answers back with the wrong attribution would otherwise pass.
    //
    // Standing query ids are random and minted per rig, so they can never be compared directly. They are labelled
    // by the pattern each was registered for, which every rig running the same schedule shares. An earlier attempt
    // ranked them by sorting their uuids, which looked canonical and was not: the uuids differ per rig, so the
    // ordering -- and therefore the rank -- differed too, and two identical states compared unequal.
    val subs = state.sqStateResults.subscribers
    val idx = state.sqStateResults.subscriptions
    def label(q: StandingQueryId): String = rig.queryLabels.getOrElse(q, s"unlabelled-${q.uuid}")
    def normalizeSubs(
      rs: List[DistinctIdSubscriberState],
    ): Set[(Long, Option[QuineId], Option[String], Set[String], Option[Boolean])] =
      rs.map(r =>
        (r.dgnId, r.subscriberNode, r.subscriberQuery.map(label), r.forQueries.map(label).toSet, r.lastResult),
      ).toSet
    def normalizeIndex(rs: List[DistinctIdIndexState]): Set[(Long, QuineId, Set[String], Option[Boolean])] =
      rs.map(r => (r.dgnId, r.peer, r.forQueries.map(label).toSet, r.answer)).toSet
    (
      state.properties,
      state.edges,
      normalizeSubs(subs),
      normalizeIndex(idx),
      state.graphNodeHashCode,
    )
  }

  private val caseNos = Iterator.from(1)

  /** Run one schedule on each rig, restore every node, and require what came back to be what was there before.
    *
    * Two claims, kept apart, because they fail for different reasons and only one of them is about restoring:
    *
    *   - every rig restores each node to the state that node held while awake. This is the property, and it holds
    *     per rig, whatever that rig's snapshot policy is.
    *   - the rigs agree with each other. This additionally requires three independently scheduled graphs to reach
    *     the same live state, which is not a claim about restoring at all. Asserting it without the first would
    *     report a divergence the schedule produced as though replay had lost something.
    *
    * ==Why the schedule settles between operations==
    *
    * A subscription propagates hop by hop, and a schedule can build a cycle, so the work one operation starts can
    * still be in flight when the next begins. Left that way, where each graph is caught depends on how the machine
    * happened to schedule three actor systems, and the graphs reach different live states from the same schedule.
    * Completing each step before starting the next makes the final state a function of the schedule, which is what
    * makes a reported counterexample reproducible.
    *
    * Only one graph is alive at a time, for the same reason: `settle` cannot tell "finished" from "paused", and
    * three actor systems competing is what turns that into a wrong answer. Each rig is observed and shut down
    * before the next is built, so the observations are compared rather than the graphs.
    *
    * `alsoRequire` sees what the always-snapshot graph restored, for a schedule whose point is what that must hold.
    * `afterRestore` drives each restored graph further, with the rig's label for its clues, for a schedule whose
    * point is what the restored graph goes on to do.
    */
  private def assertEquivalentAfter(
    schedule: List[Op],
    alsoRequire: IndexedSeq[Observable] => Any = _ => (),
    afterRestore: (String, GraphService, IndexedSeq[QuineId], String => Int) => Any = (_, _, _, _) => (),
    compareRigs: Boolean = true,
  ): Unit = {
    val nodes: IndexedSeq[QuineId] = (1 to NodeCount).map(n => idProvider.customIdToQid(n.toLong))
    val caseNo = caseNos.next()
    val awake = mutable.Map.empty[String, IndexedSeq[Observable]]
    val restored = mutable.Map.empty[String, IndexedSeq[Observable]]
    dumpingThreadsOnTimeout {
      rigSpecs.foreach { case (label, persistenceConfig) =>
        val rig = new Rig(label, persistenceConfig, caseNo)
        try {
          schedule.zipWithIndex.foreach { case (op, i) =>
            applyOp(rig, nodes, op, i)
            // Only where there is something to wait for. Sleeping already waits for the sleep, and `Settle` is one;
            // anything else can leave work in flight that the next operation must not race.
            op match {
              case Sleep(_) | Settle => ()
              case SetProp(_, _) | RemoveProp(_, _) | AddEdge(_, _) | RemoveEdge(_, _) | Register(_) | Cancel(_) =>
                settle(rig.graph)
            }
          }
          // One cycle before the reference reading, so it is taken from a node that already has everything it is
          // ever going to be given. `propagateStandingQueries` walks the nodes the store knows about, so a node with
          // nothing stored -- one whose only operation was a write that changed nothing -- is not told about a query
          // registered while it was awake, and first hears of it at its next wake. Reading before that cycle would
          // show a node missing a subscription and then "gaining" one on restore, which is the graph catching up,
          // not a restore inventing state, and is not what this is testing.
          //
          // The same goes for a node asleep at registration: `propagateStandingQueries(None)` does not wake it, so
          // it too first hears of the query at this wake. If it matches the query's root it asks its peers as part
          // of waking, and it answers a read before they reply. So the wake is done by a read whose result is
          // discarded, and the reference reading is taken once the graph is quiet; taken at the wake itself it
          // would catch the question out, with the answer landing before the restored reading. Which rig that
          // bit was decided by how much the waking node wrote before it got to the read, so the always-snapshot
          // rig passed the schedules the others failed.
          nodes.foreach(sleepNode(rig.graph, _))
          nodes.foreach(observe(rig, _))
          settle(rig.graph)
          awake(label) = nodes.map(observe(rig, _))
          // Everything restored at once, then read back; a read wakes a sleeping node, which is the restore.
          nodes.foreach(sleepNode(rig.graph, _))
          restored(label) = nodes.map(observe(rig, _))
          settle(rig.graph)
          val _ = afterRestore(s"$persistorLabel/$label", rig.graph, nodes, key => reportsOf(rig, key))
        } finally rig.shutdown()
      }
    }
    rigSpecs.foreach { case (label, _) =>
      nodes.indices.foreach { n =>
        withClue(s"[$persistorLabel/$label] node $n did not come back as it was, after schedule $schedule: ")(
          restored(label)(n) shouldBe awake(label)(n),
        )
      }
    }
    val alwaysLabel = rigSpecs.head._1
    if (compareRigs) rigSpecs.tail.foreach { case (label, _) =>
      nodes.indices.foreach { n =>
        withClue(s"[$persistorLabel/$label] node $n differs from $alwaysLabel, after schedule $schedule: ")(
          restored(label)(n) shouldBe restored(alwaysLabel)(n),
        )
      }
    }
    val _ = alsoRequire(restored(alwaysLabel))
  }

  /** Results the pattern's query has emitted on this graph, once the stream has gone quiet. */
  private def reportsOf(rig: Rig, patternKey: String): Int =
    rig.results
      .get(patternKey)
      .fold(0)(_.asScala.count {
        case StandingQueryResult(StandingQueryResult.Meta(true), _) => true
        case _ => false
      })

  private val regionDgnId: Long = SingleBranch(hasProperty("region"), nextBranches = Nil).toDomainGraphNodePackage.dgnId

  /** `holder`'s answers from `peer`, as (child DGN, answer). */
  private def heldBy(restored: IndexedSeq[Observable], holder: Int, peer: Int): Set[(Long, Option[Boolean])] = {
    val peerQid = idProvider.customIdToQid((peer + 1).toLong)
    restored(holder)._4.collect { case (dgnId, `peerQid`, _, answer) => (dgnId, answer) }
  }

  /** Node 0's answers from node 2, as (child DGN, answer). */
  private def heldFromNode2(restored: IndexedSeq[Observable]): Set[(Long, Option[Boolean])] = heldBy(restored, 0, 2)

  // The answer that filled node 0's index result for node 2 is in the journal, and One("region") keeps the child
  // pattern registered after the cancel, so a cancel that reached only memory would come back on replay and
  // nothing at wake would remove it.
  test(
    s"[$persistorLabel] a cancelled two-hop query's answer does not come back on replay while its child stays registered",
  ) {
    assertEquivalentAfter(
      List(
        SetProp(0, "kind"),
        SetProp(2, "region"),
        AddEdge(0, 2),
        Register(TwoHop("kind", "region")),
        Settle,
        Cancel(TwoHop("kind", "region")),
        Register(OneHop("region")),
        Settle,
        Sleep(0),
      ),
    )
  }

  // Node 0 sleeps before the cancel and hears of it only at wake, when the registry has forgotten the pattern. The
  // answer it holds from node 2 records the query it was for, and that query has stopped, so the answer goes.
  test(
    s"[$persistorLabel] a cancelled two-hop query's answer does not survive a wake the cancellation happened before",
  ) {
    assertEquivalentAfter(
      List(
        SetProp(0, "kind"),
        SetProp(2, "region"),
        AddEdge(0, 2),
        Register(TwoHop("kind", "region")),
        Settle,
        Sleep(0),
        Cancel(TwoHop("kind", "region")),
        Register(OneHop("region")),
        Settle,
      ),
      restored => withClue("node 0 still holds node 2's answer about region: ")(heldFromNode2(restored) shouldBe empty),
    )
  }

  // Two patterns share the child `region`. The second finds the first's answer already in the result and takes it,
  // which is journaled again with both queries. Cancel the first: the result is the second's, and a node rebuilt from
  // its journal must still hold the answer for it.
  test(s"[$persistorLabel] an answer a second query took over from a cancelled one is still held after replay") {
    assertEquivalentAfter(
      List(
        SetProp(0, "kind"),
        SetProp(0, "other"),
        SetProp(2, "region"),
        AddEdge(0, 2),
        Register(TwoHop("kind", "region")),
        Settle,
        Register(TwoHop("other", "region")),
        Settle,
        Cancel(TwoHop("kind", "region")),
        Settle,
        Sleep(0),
      ),
      restored =>
        withClue("node 0 holds node 2's answer about region for the surviving query: ")(
          heldFromNode2(restored) shouldBe Set((regionDgnId, Some(true))),
        ),
    )
  }

  /** A node with no properties and no edges, whose only state is the subscription a query left on it. Nothing in
    * `RemoveProp` here has any effect -- the node has no such property -- so the subscription and the answer it
    * reported are the whole of what has to survive.
    */
  test(s"[$persistorLabel] a node whose only state is a subscription comes back holding it") {
    assertEquivalentAfter(List(RemoveProp(2, "kind"), Register(TwoHop("kind", "region"))))
  }

  test(s"[$persistorLabel] a node asleep when a two-hop query is registered restores the same either way") {
    assertEquivalentAfter(
      List(SetProp(2, "kind"), AddEdge(0, 2), Sleep(0), Register(TwoHop("kind", "region"))),
    )
  }

  // Unlike the case above, the sleeping node matches the query's root, so at the wake that first tells it of the
  // query it has to ask node 1 about the child pattern. The reference reading must be taken after node 1 has
  // answered, or it holds the question out where the restored reading holds the answer. The generated schedules
  // found this shape; it is pinned here so it is checked every run.
  test(s"[$persistorLabel] a node asleep at registration that must ask a peer at its wake is read once answered") {
    assertEquivalentAfter(
      List(AddEdge(0, 1), SetProp(0, "other"), Sleep(0), Register(ThreeHop("other", "kind", "region"))),
    )
  }

  test(s"[$persistorLabel] a node restored from its journal matches the node a snapshot would have restored") {
    val gen = Gen.chooseNum(1, 12).flatMap(n => Gen.listOfN(n, genOp))
    forAll(gen, minSuccessful(30))((schedule: List[Op]) => assertEquivalentAfter(schedule))
  }

  // A three-hop query's child subtree is the same content-hashed DGN as a two-hop query's root, so the two share a
  // part at different depths: node 1 evaluates `kind -> region` once, as a peer for node 0 under the three-hop and
  // as a root under the two-hop, holding one result for node 2 on behalf of both. Cancelling either query must leave
  // that result, and node 1's subscription for the other, in place, whichever was registered first and whichever
  // nodes were asleep when the cancellation happened. The survivor proves it by reporting its root again when the
  // answer falls and rises after the restore.
  private val threeHop = ThreeHop("other", "kind", "region")
  private val twoHop = TwoHop("kind", "region")
  private val twoHopDgnId: Long = pattern(twoHop).toDomainGraphNodePackage.dgnId

  for {
    threeHopFirst <- List(true, false)
    cancelled <- List(threeHop, twoHop)
    asleepAtCancel <- List(Set.empty[Int], Set(0), Set(1), Set(2), Set(0, 1), Set(0, 2), Set(1, 2), Set(0, 1, 2))
  } {
    val survivor = if (cancelled == threeHop) twoHop else threeHop
    val survivorRoot = if (survivor == threeHop) 0 else 1
    val order = if (threeHopFirst) List(threeHop, twoHop) else List(twoHop, threeHop)
    val cancelledFirst = order.head == cancelled
    test(
      s"[$persistorLabel] cancelling ${cancelled.key}, registered ${if (cancelledFirst) "first" else "second"}, with " +
      s"${if (asleepAtCancel.isEmpty) "no node" else s"nodes ${asleepAtCancel.mkString(",")}"} asleep, leaves the " +
      s"shared subtree to ${survivor.key}",
    ) {
      val schedule =
        List(SetProp(0, "other"), SetProp(1, "kind"), SetProp(2, "region"), AddEdge(0, 1), AddEdge(1, 2)) ++
        order.flatMap(p => List(Register(p), Settle)) ++
        asleepAtCancel.toList.sorted.map(Sleep(_)) ++
        List(Cancel(cancelled), Settle)
      assertEquivalentAfter(
        schedule,
        restored => {
          withClue(s"node 1 holds node 2's answer about region for the survivor: ")(
            heldBy(restored, 1, 2) shouldBe Set((regionDgnId, Some(true))),
          )
          val node0Holds = if (survivor == threeHop) Set((twoHopDgnId, Some(true))) else Set.empty
          withClue(s"node 0 holds node 1's answer about the shared subtree only while the three-hop runs: ")(
            heldBy(restored, 0, 1) shouldBe node0Holds,
          )
        },
        (label, graph, nodes, reports) => {
          val ops = graph.literalOps(namespace)
          withClue(s"[$label] the survivor reported its root once before the restore: ")(
            awaitingValue(1)(reports(survivor.key)) shouldBe 1,
          )
          withClue(s"[$label] the cancelled query reported its root once and never again: ")(
            awaitingValue(1)(reports(cancelled.key)) shouldBe 1,
          )
          Await.result(ops.removeProp(nodes(2), "region"), timeout.duration)
          Await.result(ops.setProp(nodes(2), "region", QuineValue.Str("again")), timeout.duration)
          withClue(s"[$label] the survivor reports root $survivorRoot again after the shared answer rises: ")(
            awaitingValue(2)(reports(survivor.key)) shouldBe 2,
          )
          withClue(s"[$label] the cancelled query stays silent: ")(awaitingValue(1)(reports(cancelled.key)) shouldBe 1)
        },
      )
    }
  }

  // Two three-hop queries with different roots share the middle part, and both roots are on node 0. Node 0 asks
  // node 1 about `kind -> region` once, for the first query; the second is answered from what node 0 already
  // holds, so node 1 learns of it only through node 0. Cancelling the first query must leave node 1's result for
  // node 2 in place, or node 2's next change arrives for a result that is gone and the second query never learns.
  private val otherThreeHop = ThreeHop("else", "kind", "region")

  for (cancelFirstRegistered <- List(threeHop, otherThreeHop)) {
    val survivor = if (cancelFirstRegistered == threeHop) otherThreeHop else threeHop
    test(
      s"[$persistorLabel] two three-hop queries rooted on one node share the middle part: cancelling " +
      s"${cancelFirstRegistered.key}, registered first, leaves node 1's answer to ${survivor.key}",
    ) {
      assertEquivalentAfter(
        List(SetProp(0, "other"), SetProp(0, "else"), SetProp(1, "kind"), SetProp(2, "region")) ++
        List(AddEdge(0, 1), AddEdge(1, 2)) ++
        List(Register(cancelFirstRegistered), Settle, Register(survivor), Settle) ++
        List(Cancel(cancelFirstRegistered), Settle, Sleep(1)),
        restored =>
          withClue("node 1 holds node 2's answer about region for the survivor: ")(
            heldBy(restored, 1, 2) shouldBe Set((regionDgnId, Some(true))),
          ),
        (label, graph, nodes, reports) => {
          val ops = graph.literalOps(namespace)
          withClue(s"[$label] the survivor reported node 0 once before the restore: ")(
            awaitingValue(1)(reports(survivor.key)) shouldBe 1,
          )
          Await.result(ops.removeProp(nodes(2), "region"), timeout.duration)
          Await.result(ops.setProp(nodes(2), "region", QuineValue.Str("again")), timeout.duration)
          withClue(s"[$label] the survivor reports node 0 again after region on node 2 falls and rises: ")(
            awaitingValue(2)(reports(survivor.key)) shouldBe 2,
          )
        },
      )
    }
  }

  // A three-hop query is cancelled while every node sleeps, so no node tells the next one down. Node 2 wakes while
  // nothing registers `region` and drops its subscription for node 1, silently. The same query is registered again
  // before node 1 wakes, so node 1's subscriber from node 0 is judged alive by registration, node 1's answer from
  // node 2 is kept, and the new query's root is answered from it. Node 2 no longer maintains that answer.
  test(
    s"[$persistorLabel] a peer that wakes while the shared part is unregistered stops maintaining an answer the node above keeps",
  ) {
    val counts = mutable.ListBuffer.empty[(String, Int, Int)]
    assertEquivalentAfter(
      List(SetProp(0, "other"), SetProp(1, "kind"), SetProp(2, "region"), AddEdge(0, 1), AddEdge(1, 2)) ++
      List(Register(threeHop), Settle, Sleep(0), Sleep(1), Sleep(2), Cancel(threeHop), Settle) ++
      // Node 0 is woken here so its subscription for the new query, and its ask of node 1, settle before the
      // harness compares the rigs.
      List(SetProp(2, "poke"), Settle, Register(threeHop), Settle, SetProp(0, "poke"), Settle),
      _ => (),
      (label, graph, nodes, reports) => {
        val ops = graph.literalOps(namespace)
        val before = reports(threeHop.key)
        Await.result(ops.removeProp(nodes(2), "region"), timeout.duration)
        Await.result(ops.setProp(nodes(2), "region", QuineValue.Str("again")), timeout.duration)
        counts += ((label, before, awaitingValue(before + 1)(reports(threeHop.key))))
      },
      // The rigs are not compared: node 2's drop is durable on the always-snapshot rig and replayed away on the
      // others, so they differ by design of the drop, and what this case is about is each rig's own reports.
      compareRigs = false,
    )
    withClue(
      s"(rig, reports before the toggle, after): ${counts.toList}; each registration reports once, then the toggle: ",
    ) {
      counts.foreach { case (_, before, after) => (before, after) shouldBe ((2, 3)) }
    }
  }

  // The registration gap again, with the answer changing inside it: node 2 wakes while nothing registers `region`,
  // drops its subscription for node 1, and then stops being a `region`. Node 1 still holds node 2's earlier answer
  // when the same query is registered again and node 0 asks it. The new query must not be told that answer.
  test(
    s"[$persistorLabel] a query registered again is not answered from an answer a peer stopped maintaining in the gap",
  ) {
    val counts = mutable.ListBuffer.empty[(String, Int)]
    assertEquivalentAfter(
      List(SetProp(0, "other"), SetProp(1, "kind"), SetProp(2, "region"), AddEdge(0, 1), AddEdge(1, 2)) ++
      List(Register(threeHop), Settle, Sleep(1), Sleep(2), Cancel(threeHop), Settle) ++
      List(SetProp(2, "poke"), Settle, RemoveProp(2, "region"), Settle) ++
      List(Register(threeHop), Settle, SetProp(0, "poke"), Settle),
      _ => (),
      (label, _, _, reports) => counts += ((label, reports(threeHop.key))),
      compareRigs = false,
    )
    withClue(s"(rig, positive reports): ${counts.toList}; only the first registration ever matched: ") {
      counts.foreach { case (_, positives) => positives shouldBe 1 }
    }
  }

  test(s"[$persistorLabel] both queries sharing a subtree cancelled, in either order, leave nothing behind") {
    List(List(threeHop, twoHop), List(twoHop, threeHop)).foreach { cancelOrder =>
      assertEquivalentAfter(
        List(SetProp(0, "other"), SetProp(1, "kind"), SetProp(2, "region"), AddEdge(0, 1), AddEdge(1, 2)) ++
        List(Register(threeHop), Settle, Register(twoHop), Settle) ++
        cancelOrder.flatMap(p => List(Cancel(p), Settle)) ++
        List(Sleep(1)),
        restored => {
          withClue("node 1 holds nothing from node 2: ")(heldBy(restored, 1, 2) shouldBe empty)
          withClue("node 0 holds nothing from node 1: ")(heldBy(restored, 0, 1) shouldBe empty)
          withClue("node 1 has no subscribers left: ")(restored(1)._3 shouldBe empty)
        },
      )
    }
  }
}

object SnapshotJournalEquivalenceProperties {
  val NodeCount = 3

  /** `key` names the pattern, not just its property, so a one-hop and a two-hop on `kind` are two queries. */
  sealed trait Pattern { def key: String }
  final case class OneHop(prop: String) extends Pattern { val key: String = s"one:$prop" }
  final case class TwoHop(prop: String, childProp: String) extends Pattern { val key: String = s"two:$prop:$childProp" }
  final case class ThreeHop(prop: String, childProp: String, grandchildProp: String) extends Pattern {
    val key: String = s"three:$prop:$childProp:$grandchildProp"
  }

  sealed trait Op
  final case class SetProp(node: Int, key: String) extends Op
  final case class RemoveProp(node: Int, key: String) extends Op
  final case class AddEdge(from: Int, to: Int) extends Op
  final case class RemoveEdge(from: Int, to: Int) extends Op
  final case class Register(pattern: Pattern) extends Op
  final case class Cancel(pattern: Pattern) extends Op
  final case class Sleep(node: Int) extends Op
  // Lets in-flight work land before the next op; not generated, used where a schedule depends on the order.
  case object Settle extends Op

  // A small alphabet, so keys collide and the content-hashed DGNs they name are shared, as in production. The
  // three patterns on `kind` and `region` nest: the three-hop's child subtree is the two-hop's root and the
  // two-hop's child is the one-hop, so registering and cancelling them shares parts at different depths.
  private val keys: Gen[String] = Gen.oneOf("kind", "region", "other")
  private val node: Gen[Int] = Gen.chooseNum(0, NodeCount - 1)
  private val pattern: Gen[Pattern] = Gen.oneOf(
    Gen.const(OneHop("kind")),
    Gen.const(OneHop("region")),
    Gen.const(TwoHop("kind", "region")),
    Gen.const(ThreeHop("other", "kind", "region")),
    Gen.const(OneHop("other")),
  )
  private val edge: Gen[(Int, Int)] = for { a <- node; b <- node if a != b } yield (a, b)

  val genOp: Gen[Op] = Gen.frequency(
    4 -> (for { n <- node; k <- keys } yield SetProp(n, k)),
    1 -> (for { n <- node; k <- keys } yield RemoveProp(n, k)),
    2 -> edge.map { case (from, to) => AddEdge(from, to) },
    1 -> edge.map { case (from, to) => RemoveEdge(from, to) },
    2 -> pattern.map(Register(_)),
    1 -> pattern.map(Cancel(_)),
    3 -> node.map(Sleep(_)),
  )
}

/** The in-memory persistor: fast, and the one the rest of the suite runs on. */
class SnapshotJournalEquivalenceInMemoryProperties
    extends SnapshotJournalEquivalenceProperties(
      "inmemory",
      (pc, system) =>
        new com.thatdot.quine.persistor.StatelessPrimePersistor(
          pc,
          None,
          (c, ns) => new com.thatdot.quine.persistor.InMemoryPersistor(persistenceConfig = c, namespace = ns),
        )(org.apache.pekko.stream.Materializer.matFromSystem(system), logConfig),
    )

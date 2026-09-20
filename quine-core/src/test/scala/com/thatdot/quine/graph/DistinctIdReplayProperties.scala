package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  PropertyComparisonFunctions,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** What a DistinctId standing query reports must not depend on when its nodes happened to sleep.
  *
  * A node that goes to sleep and comes back is the same node; a subscriber that has already been
  * told an answer must not be told it again just because the node was evicted in between. Sleeping
  * is a memory-management decision, and memory management is not something a standing query result
  * stream should be able to observe.
  *
  * Stated over generated schedules rather than hand-picked ones, because every replay defect found
  * so far has been a node reaching a conclusion from state the fold had not finished rebuilding,
  * and which schedules expose that is exactly what nobody can predict in advance.
  *
  * The expectation is the number of times the answer rises to true, counted by folding the same
  * schedule over a model of the pattern. That is what makes removals worth generating: an earlier
  * version of this suite could only write and could only ever expect one report, so the half of
  * `latestAnswer` that records a node having answered *false* was never carried across a sleep.
  *
  * Each case also picks which snapshot policy to run against, which is what tells a failure apart
  * from a bad expectation. `always` snapshots on every sleep and so barely replays; if a case fails
  * there it fails against live behaviour and the model below is wrong. A failure confined to
  * `threshold` or `never` is a replay defect.
  */
class DistinctIdReplayProperties
    extends AnyFunSuite
    with BeforeAndAfterAll
    with Matchers
    with ScalaCheckPropertyChecks {

  import DistinctIdReplayProperties._

  implicit val timeout: Timeout = Timeout(30.seconds)
  val namespace: NamespaceId = defaultNamespaceId
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  // Constrained on a property the node must actually hold. `SingleBranch.empty` matches any
  // node, which makes the "no local match" branch unreachable and the property vacuous.
  private val oneHop: SingleBranch = SingleBranch(hasProperty("kind"), nextBranches = Nil)

  // The shape that duplicated in the cluster: a root whose answer also depends on what a
  // neighbour tells it, so the journal carries subscription results as well as property writes.
  private val twoHop: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )

  /** One graph per snapshot policy, each with both patterns registered.
    *
    * Results are kept for the whole run rather than cleared between cases, and counted by root id.
    * Clearing was how the previous version bounded a case, and it silently discarded any result
    * that arrived after the case had been judged -- exactly the duplicate being looked for.
    */
  private class Rig(val label: String, persistenceConfig: PersistenceConfig) {
    val oneHopResults = new ConcurrentLinkedQueue[StandingQueryResult]()
    val twoHopResults = new ConcurrentLinkedQueue[StandingQueryResult]()

    val graph: GraphService = {
      def persistorMaker(system: ActorSystem): PrimePersistor =
        new StatelessPrimePersistor(
          persistenceConfig,
          None,
          (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
        )(Materializer.matFromSystem(system), logConfig)

      val g = Await.result(
        GraphService(
          s"distinctid-replay-properties-$label",
          effectOrder = EventEffectOrder.PersistorFirst,
          persistorMaker = persistorMaker,
          idProvider = idProvider,
          declineSleepWhenWriteWithinMillis = 0L,
        ),
        timeout.duration,
      )
      g.requiredGraphIsReady()
      registerDistinctIdQuery(g, s"one-hop-$label", oneHop, oneHopResults)
      registerDistinctIdQuery(g, s"two-hop-$label", twoHop, twoHopResults)
      g
    }
  }

  private def registerDistinctIdQuery(
    g: GraphService,
    name: String,
    branch: SingleBranch,
    results: ConcurrentLinkedQueue[StandingQueryResult],
    sqId: StandingQueryId = StandingQueryId.fresh(),
  ): Unit = {
    val dgnPackage = branch.toDomainGraphNodePackage
    Await.result(
      g.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    val outputs: Map[String, Sink[StandingQueryResult, UniqueKillSwitch]] = Map(
      "capture" -> Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r => results.offer(r); SqResultsExecToken(s"distinctid-replay-properties-$name") }
        .to(Sink.ignore),
    )
    g.standingQueries(namespace)
      .getOrElse(fail("default namespace should exist"))
      .createStandingQuery(
        name = name,
        pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
          dgnId = dgnPackage.dgnId,
          // The native id rather than its rendering, so `valueToQid` round-trips it back to the
          // node a result is about. Counting by root id is what makes stale results impossible to
          // misattribute between cases.
          formatReturnAsStr = false,
          aliasReturnAs = Symbol("id"),
          includeCancellation = false,
          origin = PatternOrigin.DirectDgb,
        ),
        outputs = outputs,
        sqId = sqId,
      )
    ()
  }

  private val rigs: List[Rig] = List(
    new Rig("always", PersistenceConfig(snapshotAfterEvents = 0)),
    new Rig("threshold", PersistenceConfig(snapshotAfterEvents = 16)),
    new Rig("never", PersistenceConfig(snapshotAfterEvents = Int.MaxValue)),
  )

  implicit val ec: ExecutionContextExecutor = rigs.head.graph.system.dispatcher

  override def afterAll(): Unit = rigs.foreach(r => Await.result(r.graph.shutdown(), timeout.duration))

  private val ids = Iterator.from(1)
  private def freshNode(): QuineId = idProvider.customIdToQid(ids.next().toLong)

  private def sleepsCompleted(g: GraphService): Long =
    g.metrics.metricRegistry.getCounters.asScala.collect {
      case (name, counter) if name.endsWith("sleep-counters.slept-success") => counter.getCount
    }.sum

  private def awake(g: GraphService, qid: QuineId): Boolean =
    Await.result(
      g.relayAsk(
        g.shardFromNode(qid).quineRef,
        ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
      ).flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(g.materializer))
        .map(_.contains(qid)),
      timeout.duration,
    )

  /** `requestNodeSleep` asks; it does not guarantee. A schedule whose sleeps never happened would
    * make every case in this property a no-sleep case, and it would pass on nothing.
    *
    * Counted rather than observed: a message already in flight to the node wakes it straight back
    * up, which is a sleep that happened, not one that was refused. A node the schedule has not
    * touched yet is not awake and has nothing to sleep, so it is left alone.
    */
  private def sleepNode(g: GraphService, qid: QuineId): Unit = if (awake(g, qid)) {
    val before = sleepsCompleted(g)
    Await.result(g.requestNodeSleep(namespace, qid), timeout.duration)
    var waited = 0
    while (sleepsCompleted(g) == before && waited < 1000) { Thread.sleep(5); waited += 1 }
    if (sleepsCompleted(g) == before) fail(s"node never slept after being asked; the schedule would be a lie")
  }

  /** Wait for the result stream to fall silent rather than for a fixed span.
    *
    * A fixed wait decides in advance how slow the slowest machine is allowed to be, and answers
    * "no duplicate yet" when it is wrong. This answers "no duplicate since the stream stopped
    * moving", and only gives up if the stream never stops.
    */
  private def awaitQuiet(q: ConcurrentLinkedQueue[StandingQueryResult]): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    var quietPolls = 0
    var lastSize = q.size()
    while (quietPolls < 4 && System.nanoTime() < deadline) {
      Thread.sleep(20)
      val size = q.size()
      if (size == lastSize) quietPolls += 1
      else { quietPolls = 0; lastSize = size }
    }
  }

  private def reportsFor(q: ConcurrentLinkedQueue[StandingQueryResult], qid: QuineId): Int = {
    awaitQuiet(q)
    q.asScala.count {
      case StandingQueryResult(StandingQueryResult.Meta(true), data) =>
        data.get("id").flatMap(idProvider.valueToQid).contains(qid)
      case _ => false
    }
  }

  private def applyOp(rig: Rig, root: QuineId, neighbour: QuineId, op: Op, i: Int): Unit = {
    val ops = rig.graph.literalOps(namespace)
    // Distinct values per step: a `SET` to the value already stored is dropped before it becomes
    // an event, and a schedule whose writes never reached the journal would replay nothing.
    op match {
      case SetKind => Await.result(ops.setProp(root, "kind", QuineValue.Str(s"k$i")), timeout.duration)
      case ClearKind => Await.result(ops.removeProp(root, "kind"), timeout.duration)
      case SetRegion => Await.result(ops.setProp(neighbour, "region", QuineValue.Str(s"r$i")), timeout.duration)
      case ClearRegion => Await.result(ops.removeProp(neighbour, "region"), timeout.duration)
      case AddEdge => Await.result(ops.addEdge(root, neighbour, "to"), timeout.duration)
      case RemoveEdge => Await.result(ops.removeEdge(root, neighbour, "to"), timeout.duration)
      case SleepRoot => sleepNode(rig.graph, root)
      case SleepNeighbour => sleepNode(rig.graph, neighbour)
    }
  }

  private val rigGen: Gen[Rig] = Gen.oneOf(rigs)

  test("a one-hop DistinctId match is reported once per rising edge, whatever the sleep schedule") {
    // Small and bounded: each case drives a real graph, so this trades case count for fidelity.
    val gen = for {
      rig <- rigGen
      ops <- Gen.chooseNum(1, 12).flatMap(n => Gen.listOfN(n, Gen.oneOf(SetKind, ClearKind, SleepRoot)))
    } yield (rig, ops)

    forAll(gen, minSuccessful(90)) { case (rig: Rig, schedule: List[Op]) =>
      val root = freshNode()
      schedule.zipWithIndex.foreach { case (op, i) => applyOp(rig, root, root, op, i) }
      val owed = owedReports(schedule, _.kind)
      withClue(s"[${rig.label}] schedule $schedule: ") {
        reportsFor(rig.oneHopResults, root) shouldBe owed
      }
    }
  }

  test("a two-hop DistinctId match is never reported more often than its answer rises") {
    val anyOp = Gen.oneOf(SetKind, ClearKind, SetRegion, ClearRegion, AddEdge, RemoveEdge, SleepRoot, SleepNeighbour)
    // Half the cases are drawn so that the pattern is satisfiable at some point. Left to chance,
    // most short schedules never assemble all three of kind, region and edge, and assert only that
    // a node which never matched was never reported -- true of a graph that reports nothing at all.
    val satisfiable = for {
      extra <- Gen.chooseNum(0, 7).flatMap(n => Gen.listOfN(n, anyOp))
      all <- Gen.pick(extra.size + 3, List(SetKind, SetRegion, AddEdge) ++ extra)
    } yield all.toList
    val gen = for {
      rig <- rigGen
      ops <- Gen.frequency(
        1 -> Gen.chooseNum(1, 10).flatMap(n => Gen.listOfN(n, anyOp)),
        1 -> satisfiable,
      )
    } yield (rig, ops)

    forAll(gen, minSuccessful(90)) { case (rig: Rig, schedule: List[Op]) =>
      val root = freshNode()
      val neighbour = freshNode()
      schedule.zipWithIndex.foreach { case (op, i) => applyOp(rig, root, neighbour, op, i) }
      val matched = (m: Model) => m.kind && m.region && m.edge
      val rises = owedReports(schedule, matched)
      val seen = reportsFor(rig.twoHopResults, root)
      withClue(s"[${rig.label}] schedule $schedule: ") {
        // Bounded rather than pinned, because a match the schedule un-makes before the root has
        // heard back from its neighbour is never reported at all -- and that is true of the
        // always-snapshot graph too, so it is how the engine behaves and not a replay defect.
        // The upper bound is the assertion that matters: a rise can be reported once, and a second
        // report against the same rise is the bookkeeping loss this suite exists for.
        seen should be <= rises
        if (matched(schedule.foldLeft(Model(false, false, false))(step))) seen should be >= 1
      }
    }
  }

  /** Where the property has to settle for a bound, these pin the count. Each phase is given time
    * to propagate, so every transition below is one the root has finished hearing about.
    */
  /** Total work a graph has recorded doing: every persisted event, snapshot, sleep and wake ticks one of these.
    * Monotonic, so two equal readings mean nothing happened in between, and reading it costs no message and
    * cannot wake a sleeping node.
    */
  private def activity(g: GraphService): Long = {
    val registry = g.metrics.metricRegistry
    registry.getCounters.asScala.values.map(_.getCount).sum +
    registry.getTimers.asScala.values.map(_.getCount).sum
  }

  /** Wait for a graph to stop doing work, rather than for a fixed span.
    *
    * An observation about the program: the loop ends when the counter stops moving, so a slower machine takes
    * more iterations rather than giving a wrong answer. What it cannot tell apart is "finished" from "paused long
    * enough to look finished", which is why this suite does not run its tests concurrently.
    */
  private def settleGraph(g: GraphService): Unit = {
    val deadline = System.nanoTime() + 30.seconds.toNanos
    var quietPolls = 0
    var last = activity(g)
    while (quietPolls < 3 && System.nanoTime() < deadline) {
      Thread.sleep(20)
      val now = activity(g)
      if (now == last) quietPolls += 1 else { quietPolls = 0; last = now }
    }
  }

  /** All three rigs at once, so the thirteen call sites need no rig threaded through them. Three metric reads per
    * poll is cheaper than the one-second pause this replaced.
    */
  private def settle(): Unit = rigs.foreach(r => settleGraph(r.graph))

  test("a two-hop answer that falls and rises again is reported both times") {
    rigs.foreach { rig =>
      val root = freshNode()
      val neighbour = freshNode()
      val ops = rig.graph.literalOps(namespace)
      Await.result(ops.setProp(root, "kind", QuineValue.Str("k1")), timeout.duration)
      Await.result(ops.setProp(neighbour, "region", QuineValue.Str("r1")), timeout.duration)
      Await.result(ops.addEdge(root, neighbour, "to"), timeout.duration)
      settle()
      withClue(s"[${rig.label}] first match: ")(reportsFor(rig.twoHopResults, root) shouldBe 1)

      Await.result(ops.removeProp(root, "kind"), timeout.duration)
      settle()
      withClue(s"[${rig.label}] a fall is silent without cancellations: ")(
        reportsFor(rig.twoHopResults, root) shouldBe 1,
      )

      Await.result(ops.setProp(root, "kind", QuineValue.Str("k2")), timeout.duration)
      settle()
      // The node last told its subscribers `false`. Rebuilt from a journal that ends in the
      // removal, it has to come back knowing that, or this rise looks like a repeat and is
      // suppressed.
      withClue(s"[${rig.label}] the second rise is a new answer, not a repeat: ")(
        reportsFor(rig.twoHopResults, root) shouldBe 2,
      )
    }
  }

  test("a two-hop match already reported is not reported again after both nodes sleep") {
    rigs.foreach { rig =>
      val root = freshNode()
      val neighbour = freshNode()
      val ops = rig.graph.literalOps(namespace)
      Await.result(ops.setProp(root, "kind", QuineValue.Str("k1")), timeout.duration)
      Await.result(ops.setProp(neighbour, "region", QuineValue.Str("r1")), timeout.duration)
      Await.result(ops.addEdge(root, neighbour, "to"), timeout.duration)
      settle()
      withClue(s"[${rig.label}] first match: ")(reportsFor(rig.twoHopResults, root) shouldBe 1)

      sleepNode(rig.graph, root)
      sleepNode(rig.graph, neighbour)
      // Rewriting the watched property recomputes an answer that has not changed. That is the
      // moment a node which forgot what it last reported reports it again, and it is the shape
      // that duplicated in the cluster.
      Await.result(ops.setProp(root, "kind", QuineValue.Str("k2")), timeout.duration)
      settle()
      withClue(s"[${rig.label}] an unchanged answer must not be reported a second time: ")(
        reportsFor(rig.twoHopResults, root) shouldBe 1,
      )
    }
  }

  test("a DistinctId match on an already-awake node is reported once, whatever the sleep schedule") {
    // The rigs register their queries before any node exists, so every node meets them at wake, in
    // `NodeActor`'s constructor. A query registered while a node is awake reaches it through
    // `updateDistinctIdStandingQueriesOnNode` instead, and that is a different path into the node's state.
    val gen = for {
      rig <- rigGen
      writesBefore <- Gen.choose(1, 4)
      writesAfter <- Gen.choose(0, 4)
      // -1 sleeps right after the query lands, before any further write
      sleepAfter <- Gen.someOf(-1 until writesAfter)
    } yield (rig, writesBefore, writesAfter, sleepAfter.toSet)

    forAll(gen, minSuccessful(30)) { case (rig: Rig, writesBefore: Int, writesAfter: Int, sleepAfter: Set[Int]) =>
      val root = freshNode()
      val ops = rig.graph.literalOps(namespace)
      val sqns = rig.graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
      // A property of its own per case, so nodes left over from earlier cases match none of this one's query.
      val key = s"late${ids.next()}"
      val results = new ConcurrentLinkedQueue[StandingQueryResult]()
      val sqId = StandingQueryId.fresh()

      for (i <- 0 until writesBefore)
        Await.result(ops.setProp(root, key, QuineValue.Str(s"l$i")), timeout.duration)
      registerDistinctIdQuery(
        rig.graph,
        s"late-$key",
        SingleBranch(hasProperty(key), nextBranches = Nil),
        results,
        sqId,
      )
      // Awake nodes only: the node under test is awake, and this is the path a running graph takes.
      Await.result(sqns.propagateStandingQueries(None), timeout.duration)
      settle()

      if (sleepAfter.contains(-1)) sleepNode(rig.graph, root)
      for (i <- 0 until writesAfter) {
        Await.result(ops.setProp(root, key, QuineValue.Str(s"l${writesBefore + i}")), timeout.duration)
        if (sleepAfter.contains(i)) sleepNode(rig.graph, root)
      }
      // A schedule ending on a sleep would otherwise leave the node asleep, and it is the wake that re-reports.
      Await.result(ops.getProps(root), timeout.duration)

      val seen = reportsFor(results, root)
      sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, timeout.duration))

      // Total over shrunk inputs: a node never written never gains the property and owes nothing.
      val owed = if (writesBefore + writesAfter >= 1) 1 else 0
      withClue(
        s"[${rig.label}] $writesBefore write(s), then the query, then $writesAfter write(s), " +
        s"sleeping after ${sleepAfter.toList.sorted}: ",
      ) {
        seen shouldBe owed
      }
    }
  }

  /** What this node is subscribed to on other nodes, read back from `domainNodeIndex`. */
  private def subscriptionsOf(g: GraphService, qid: QuineId): List[messaging.LiteralMessage.DistinctIdIndexState] =
    Await.result(g.literalOps(namespace).logState(qid), timeout.duration).sqStateResults.subscriptions

  /** Register a two-hop query on a fresh root once its match already exists, cancel it while the root is awake, and
    * read back what the root still holds of its neighbour's answer: right after the cancellation, and again after a
    * sleep and wake. The cancellation reaches the root through `updateDistinctIdStandingQueriesOnNode`, the same
    * path the registration takes, and by then `cancelStandingQuery` has already unregistered the pattern.
    */
  private def heldAfterCancellation(
    rig: Rig,
    childKey: String,
  ): (
    Long,
    List[messaging.LiteralMessage.DistinctIdIndexState],
    List[messaging.LiteralMessage.DistinctIdIndexState],
  ) = {
    // Adding an edge subscribes the root to the neighbour for every registered pattern with such an edge, matched
    // or not, so the root holds answers for the rigs' patterns too. Only this query's child is under test.
    val childDgnId: Long = SingleBranch(hasProperty(childKey), nextBranches = Nil).toDomainGraphNodePackage.dgnId
    val root = freshNode()
    val neighbour = freshNode()
    val ops = rig.graph.literalOps(namespace)
    val sqns = rig.graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    val key = s"late${ids.next()}"
    val results = new ConcurrentLinkedQueue[StandingQueryResult]()
    val sqId = StandingQueryId.fresh()
    val branch = SingleBranch(
      hasProperty(key),
      nextBranches = List(
        DomainEdge(
          GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
          DependsUpon,
          SingleBranch(hasProperty(childKey), nextBranches = Nil),
        ),
      ),
    )

    Await.result(ops.setProp(root, key, QuineValue.Str("k")), timeout.duration)
    Await.result(ops.setProp(neighbour, childKey, QuineValue.Str("c")), timeout.duration)
    Await.result(ops.addEdge(root, neighbour, "to"), timeout.duration)
    registerDistinctIdQuery(rig.graph, s"late-$key", branch, results, sqId)
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    settle()
    withClue(s"[${rig.label}] the match is reported before the cancellation: ")(reportsFor(results, root) shouldBe 1)
    withClue(s"[${rig.label}] the root holds its neighbour's answer: ")(
      subscriptionsOf(rig.graph, root).map(_.dgnId) should contain(childDgnId),
    )

    sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, timeout.duration))
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    settle()
    val afterCancellation = subscriptionsOf(rig.graph, root)

    sleepNode(rig.graph, root)
    Await.result(ops.getProps(root), timeout.duration)
    settle()
    (childDgnId, afterCancellation, subscriptionsOf(rig.graph, root))
  }

  test("cancelling a query whose child pattern nothing else uses leaves nothing behind, before or after a wake") {
    // A child key of its own means the root subscribed to the neighbour on this query's behalf and nothing else
    // needs that answer once the query is gone. Both patterns leave the registry with the cancellation, so the
    // cleanup cannot look them up there.
    rigs.foreach { rig =>
      val (child, afterCancellation, afterWake) = heldAfterCancellation(rig, childKey = s"child${ids.next()}")
      withClue(s"[${rig.label}] the cancellation cleared the root while it was awake: ")(
        afterCancellation.map(_.dgnId) should not contain child,
      )
      withClue(s"[${rig.label}] a cancelled query must not come back with the journal: ")(
        afterWake.map(_.dgnId) should not contain child,
      )
    }
  }

  test("cancelling a query whose child pattern a live query shares keeps the shared answer") {
    // `region` is the child of every rig's two-hop pattern, and adding the edge already subscribed the root to the
    // neighbour on that pattern's behalf. This query found the answer cached and never subscribed itself, so the
    // entry belongs to the live query and has to survive both the cancellation and the wake-time sweep.
    rigs.foreach { rig =>
      val (child, afterCancellation, afterWake) = heldAfterCancellation(rig, childKey = "region")
      withClue(s"[${rig.label}] the shared answer is kept for the query still using it: ")(
        afterCancellation.map(_.dgnId) should contain(child),
      )
      withClue(s"[${rig.label}] and survives a wake: ")(afterWake.map(_.dgnId) should contain(child))
    }
  }

  /** Two queries whose patterns share a child, one cancelled: the survivor is owed a report for every rise of its
    * answer after that, whichever query subscribed to the neighbour first, whether the cancellation reached the
    * root through the propagate endpoint or was found at a wake, and whichever of the two nodes slept meanwhile.
    * The neighbour's view of who cares about the shared child is what the cancellation tests.
    */
  private def survivorKeepsReporting(
    rig: Rig,
    survivorFirst: Boolean,
    propagate: Boolean,
    sleeps: Set[String],
  ): Assertion = {
    val root = freshNode()
    val neighbour = freshNode()
    val ops = rig.graph.literalOps(namespace)
    val sqns = rig.graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    val childKey = s"child${ids.next()}"
    def query(rootKey: String): SingleBranch = SingleBranch(
      hasProperty(rootKey),
      nextBranches = List(
        DomainEdge(
          GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
          DependsUpon,
          SingleBranch(hasProperty(childKey), nextBranches = Nil),
        ),
      ),
    )
    val cancelledKey = s"late${ids.next()}"
    val survivorKey = s"late${ids.next()}"
    val cancelledResults = new ConcurrentLinkedQueue[StandingQueryResult]()
    val survivorResults = new ConcurrentLinkedQueue[StandingQueryResult]()
    val cancelledSq = StandingQueryId.fresh()
    val survivorSq = StandingQueryId.fresh()

    Await.result(ops.setProp(root, cancelledKey, QuineValue.Str("a")), timeout.duration)
    Await.result(ops.setProp(root, survivorKey, QuineValue.Str("b")), timeout.duration)
    Await.result(ops.setProp(neighbour, childKey, QuineValue.Str("c")), timeout.duration)
    Await.result(ops.addEdge(root, neighbour, "to"), timeout.duration)

    def register(key: String, results: ConcurrentLinkedQueue[StandingQueryResult], sqId: StandingQueryId): Unit = {
      registerDistinctIdQuery(rig.graph, s"shared-$key", query(key), results, sqId)
      Await.result(sqns.propagateStandingQueries(None), timeout.duration)
      settle()
    }
    if (survivorFirst) {
      register(survivorKey, survivorResults, survivorSq)
      register(cancelledKey, cancelledResults, cancelledSq)
    } else {
      register(cancelledKey, cancelledResults, cancelledSq)
      register(survivorKey, survivorResults, survivorSq)
    }
    withClue("both report the match before the cancellation: ") {
      reportsFor(survivorResults, root) shouldBe 1
      reportsFor(cancelledResults, root) shouldBe 1
    }

    sqns.cancelStandingQuery(cancelledSq).foreach(f => Await.result(f, timeout.duration))
    if (propagate) Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    if (sleeps.contains("root")) sleepNode(rig.graph, root)
    if (sleeps.contains("neighbour")) sleepNode(rig.graph, neighbour)
    Await.result(ops.getProps(neighbour), timeout.duration)
    Await.result(ops.getProps(root), timeout.duration)
    settle()

    // The survivor's answer falls and rises again at the neighbour. The rise is a report it is owed, and it only
    // arrives if the neighbour still tells this root about the shared child.
    Await.result(ops.removeProp(neighbour, childKey), timeout.duration)
    settle()
    Await.result(ops.setProp(neighbour, childKey, QuineValue.Str("c2")), timeout.duration)
    withClue("the survivor reports the rise after the other query is gone: ")(
      reportsFor(survivorResults, root) shouldBe 2,
    )
    withClue("the cancelled query reports nothing more: ")(reportsFor(cancelledResults, root) shouldBe 1)
  }

  test("cancelling one of two queries that share a child leaves the other reporting, in every order") {
    for {
      rig <- rigs
      survivorFirst <- List(true, false)
      propagate <- List(true, false)
      sleeps <- List(Set.empty[String], Set("root"), Set("neighbour"), Set("root", "neighbour"))
    } withClue(
      s"[${rig.label}] survivor registered ${if (survivorFirst) "first" else "second"}, " +
      s"cancelled ${if (propagate) "with" else "without"} propagate, " +
      s"then slept: ${if (sleeps.isEmpty) "neither" else sleeps.toList.sorted.mkString(" and ")}: ",
    )(survivorKeepsReporting(rig, survivorFirst, propagate, sleeps))
  }
}

private object DistinctIdReplayProperties {

  sealed trait Op
  case object SetKind extends Op
  case object ClearKind extends Op
  case object SetRegion extends Op
  case object ClearRegion extends Op
  case object AddEdge extends Op
  case object RemoveEdge extends Op
  case object SleepRoot extends Op
  case object SleepNeighbour extends Op

  /** What the pattern can see, as the schedule leaves it. */
  final case class Model(kind: Boolean, region: Boolean, edge: Boolean)

  def step(m: Model, op: Op): Model = op match {
    case SetKind => m.copy(kind = true)
    case ClearKind => m.copy(kind = false)
    case SetRegion => m.copy(region = true)
    case ClearRegion => m.copy(region = false)
    case AddEdge => m.copy(edge = true)
    case RemoveEdge => m.copy(edge = false)
    case SleepRoot | SleepNeighbour => m
  }

  /** How many times the answer rises to true, which is how many positive reports are owed.
    *
    * Cancellations are not requested, so a fall to false is silent; the next rise is a new report
    * and not a duplicate. A node that never matches is owed nothing.
    */
  def owedReports(ops: List[Op], matches: Model => Boolean): Int = {
    val (_, count) = ops.foldLeft((Model(false, false, false), 0)) { case ((m, n), op) =>
      val next = step(m, op)
      (next, if (matches(next) && !matches(m)) n + 1 else n)
    }
    count
  }
}

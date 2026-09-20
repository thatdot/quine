package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
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

/** What a node does with an answer it computed for a pattern that was unregistered and registered again.
  *
  * A DistinctId answer is memoized by pattern, not by query: patterns are content-addressed, so a second query
  * asking about the same pattern is answered from what the node already holds. That is the point of the design,
  * and it is sound while the answer is kept current. `receiveDomainNodeSubscription` relies on it: an arriving
  * subscription for a pattern the node already has an answer for is replied to from that answer, with no
  * evaluation and nothing asked downstream.
  *
  * Cancelling a query breaks the premise without removing the answer. The cancellation reaches no sleeping node
  * (`NodeActorMailbox.shouldIgnoreWhenSleeping` drops it), so a root asleep at the cancellation never tells the
  * node below it to stop. That node keeps the subscription -- a node subscriber is judged dead only by whether
  * its pattern is registered, and cancelling the query that registered the pattern is not something it is told
  * about -- while the pattern is gone from the registry, and `reevaluateDomainNode` answers a local change to an
  * unregistered pattern by dropping the watch rather than by reporting. The answer stops being maintained and
  * stays where it is.
  *
  * Register a query with the same pattern and the pattern is in the registry again, so the wake-time judgment
  * that would have dropped the subscription keeps it, and the stale answer is what the new query is told.
  *
  * Node A holds `kind` and an edge to node B, which holds `region`; the pattern is "a `kind` with an edge to a
  * `region`". Nothing here turns on how a node is restored: B, which holds the stale answer, never sleeps. The
  * three configurations differ only in when a snapshot is written, and are run to show that.
  */
class CancelledPatternAnswerReuseTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  private val nodeA: QuineId = idProvider.customIdToQid(1L)
  private val nodeB: QuineId = idProvider.customIdToQid(2L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  /** A `kind` with an edge to a `region`. Content-addressed, so every registration of it names the same DGN ids. */
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

  private def makeGraph(name: String, persistenceConfig: PersistenceConfig): GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        persistenceConfig,
        None,
        (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
      )(Materializer.matFromSystem(system), logConfig)

    val graph = Await.result(
      GraphService(
        name,
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      timeout.duration,
    )
    graph.requiredGraphIsReady()
    graph
  }

  /** Every positive match a registration has reported. Results are drained as they arrive: a query whose results
    * back up closes the graph's ingest valve, after which every write and wake stalls.
    */
  private class Registration(val sqId: StandingQueryId) {
    private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()
    val drain: Sink[StandingQueryResult, UniqueKillSwitch] =
      Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r =>
          captured.add(r)
          MasterStream.SqResultsExecToken("cancelled-pattern-answer-reuse")
        }
        .to(Sink.ignore)
    def positiveMatches: Int = captured.asScala.count(_.meta.isPositiveMatch)
  }

  private def register(graph: GraphService, name: String): Registration = {
    val registration = new Registration(StandingQueryId.fresh())
    val dgnPackage = twoHop.toDomainGraphNodePackage
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, registration.sqId, skipPersistor = true),
      timeout.duration,
    )
    val sqns = graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    sqns.createStandingQuery(
      name = name,
      pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
        dgnId = dgnPackage.dgnId,
        formatReturnAsStr = false,
        aliasReturnAs = Symbol("id"),
        includeCancellation = false,
        origin = PatternOrigin.DirectDgb,
      ),
      outputs = Map("drain" -> registration.drain),
      sqId = registration.sqId,
    )
    // Awake nodes take the awake-node path; the rest meet the query at their next wake.
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
    registration
  }

  private def cancel(graph: GraphService, registration: Registration): Unit = {
    val sqns = graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    sqns.cancelStandingQuery(registration.sqId).foreach(f => Await.result(f, timeout.duration))
    // Awake nodes drop the query on this path; the rest find it gone at their next wake.
    Await.result(sqns.propagateStandingQueries(None), timeout.duration)
  }

  private def awake(graph: GraphService, qid: QuineId): Boolean =
    Await.result(
      graph
        .relayAsk(
          graph.shardFromNode(qid).quineRef,
          ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
        )
        .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))(
          ExecutionContext.parasitic,
        )
        .map(_.contains(qid))(ExecutionContext.parasitic),
      timeout.duration,
    )

  private def sleepsCompleted(graph: GraphService): Long =
    graph.metrics.metricRegistry.getCounters.asScala.collect {
      case (name, counter) if name.endsWith("sleep-counters.slept-success") => counter.getCount
    }.sum

  /** Counted rather than observed: a message in flight wakes the node straight back, and that is a sleep that
    * happened. A node that is not awake is left alone.
    */
  private def sleepNode(graph: GraphService, qid: QuineId): Unit = if (awake(graph, qid)) {
    val before = sleepsCompleted(graph)
    Await.result(graph.requestNodeSleep(namespace, qid), timeout.duration)
    var waited = 0
    while (sleepsCompleted(graph) == before && waited < 100) { Thread.sleep(50); waited += 1 }
    if (sleepsCompleted(graph) == before) fail("node never slept after being asked; the schedule would be a lie")
  }

  private def settle(): Unit = Thread.sleep(500)

  /** The answers node B has reported to its subscribers, by pattern. */
  private def reportedByB(graph: GraphService): Set[(Long, QuineId, Option[Boolean])] =
    Await
      .result(graph.literalOps(namespace).logState(nodeB), timeout.duration)
      .sqStateResults
      .subscribers
      .flatMap(r => r.subscriberNode.map((r.dgnId, _, r.lastResult)))
      .toSet

  private val regionDgnId: Long =
    SingleBranch(hasProperty("region"), nextBranches = Nil).toDomainGraphNodePackage.dgnId

  private val configurations: List[(String, PersistenceConfig)] = List(
    "always" -> PersistenceConfig(snapshotAfterEvents = 0),
    "threshold" -> PersistenceConfig(snapshotAfterEvents = 4),
    "never" -> PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
  )

  /** Everything up to the moment the second query is registered, with the root either asleep or awake when the
    * first query is cancelled. Returns the second registration and the graph, for the caller to drive and assert.
    */
  private def upToSecondRegistration(graph: GraphService, rootAsleepAtCancel: Boolean): (Registration, Int) = {
    val ops = graph.literalOps(namespace)
    Await.result(ops.setProp(nodeA, "kind", QuineValue.Str("a")), timeout.duration)
    Await.result(ops.setProp(nodeB, "region", QuineValue.Str("b")), timeout.duration)
    Await.result(ops.addEdge(nodeA, nodeB, "to"), timeout.duration)

    val first = register(graph, "first")
    settle()
    val matchesUnderFirst = first.positiveMatches

    // Node B stays awake for the whole schedule, so nothing below depends on how a node is restored.
    if (rootAsleepAtCancel) sleepNode(graph, nodeA)
    cancel(graph, first)
    settle()

    // The pattern no longer matches: B is not a `region` any more. B is awake and processes this itself.
    Await.result(ops.removeProp(nodeB, "region"), timeout.duration)
    settle()

    val second = register(graph, "second")
    settle()
    (second, matchesUnderFirst)
  }

  private def withGraph(name: String, persistenceConfig: PersistenceConfig)(body: GraphService => Any): Unit = {
    val graph = makeGraph(name, persistenceConfig)
    try { val _ = body(graph) }
    finally Await.result(graph.shutdown(), timeout.duration)
  }

  configurations.foreach { case (label, persistenceConfig) =>
    test(
      s"[$label] a query registered on a pattern whose last query was cancelled is not answered from the answer " +
      s"that pattern was left holding",
    ) {
      withGraph(s"cancelled-pattern-answer-reuse-asleep-$label", persistenceConfig) { graph =>
        val (second, matchesUnderFirst) = upToSecondRegistration(graph, rootAsleepAtCancel = true)

        withClue("the first query should have matched, or this schedule tests nothing: ")(
          matchesUnderFirst shouldBe 1,
        )
        // The cancellation reaches B awake, and no running query is named on A's subscription, so B drops it
        // there: the answer B was left holding has no subscriber to be told to.
        withClue("node B dropped its subscriber when the cancellation reached it: ")(
          reportedByB(graph) should not contain ((regionDgnId, nodeA, Some(true))),
        )

        // Waking the root is what makes it subscribe for the second query, which is what asks B.
        val _ = Await.result(graph.literalOps(namespace).logState(nodeA), timeout.duration)
        settle()

        withClue(
          "node B has no `region`, so nothing satisfies the pattern and the second query has nothing to report: ",
        )(second.positiveMatches shouldBe 0)
      }
    }

    // The same schedule, differing only in that the root is awake when the cancellation happens and so tears its
    // subscription to B down. B recomputes for the second query instead of replying from what it held. This pins
    // the difference to the sleep: a re-registration on its own reports nothing.
    test(s"[$label] the same pattern registered again reports nothing when the root was awake to be cancelled") {
      withGraph(s"cancelled-pattern-answer-reuse-awake-$label", persistenceConfig) { graph =>
        val (second, matchesUnderFirst) = upToSecondRegistration(graph, rootAsleepAtCancel = false)

        withClue("the first query should have matched, or this schedule tests nothing: ")(
          matchesUnderFirst shouldBe 1,
        )
        withClue("the cancellation reached node B, so it is not still reporting a match to the root: ")(
          reportedByB(graph) should not contain ((regionDgnId, nodeA, Some(true))),
        )

        val _ = Await.result(graph.literalOps(namespace).logState(nodeA), timeout.duration)
        settle()

        withClue("node B has no `region`, so the second query has nothing to report: ")(
          second.positiveMatches shouldBe 0,
        )
      }
    }
  }
}

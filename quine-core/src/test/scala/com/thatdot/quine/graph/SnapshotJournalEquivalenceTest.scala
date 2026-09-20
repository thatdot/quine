package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

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
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** Every decision to skip a snapshot rests on one property: a node rebuilt from its journal is the
  * same node it would have been had a snapshot been written. Nothing else asserts that, so a piece
  * of state that lives only inside snapshots -- as `DistinctIdSubscription.latestAnswer` once
  * did -- can be dropped silently by any change to when snapshots are taken.
  *
  * The same workload runs against three graphs that differ only in that decision, and the nodes
  * they restore have to agree.
  */
class SnapshotJournalEquivalenceTest extends AsyncFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val namespace: NamespaceId = defaultNamespaceId

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
    // Two hops on purpose. `SqStateResults.subscribers` reports only subscribers that are themselves
    // nodes, so a pattern registered as a bare standing query surfaces no `latestAnswer` at all
    // and the comparison below would be blind to it. Two hops give `chainB` a node subscriber
    // (`chainA`) and a node it is subscribed to (`chainC`), so both halves of its bookkeeping are
    // populated -- the only shape where a node has to replay an incoming subscription and an
    // incoming result and end up agreeing with itself about what it already reported.
    val sqId = StandingQueryId.fresh()
    val anyP1 = DomainNodeEquiv(
      None,
      Map(Symbol("p1") -> ((PropertyComparisonFunctions.Wildcard, None))),
      Set.empty,
    )
    val anyNode = DomainNodeEquiv(None, Map.empty, Set.empty)
    def pointsAt(to: SingleBranch): DomainEdge =
      DomainEdge(GenericEdge(Symbol("points-at"), EdgeDirection.Outgoing), DependsUpon, to)
    val dgnPackage = SingleBranch(
      anyP1,
      nextBranches = List(
        pointsAt(
          SingleBranch(
            anyNode,
            nextBranches = List(pointsAt(SingleBranch(anyP1, nextBranches = Nil))),
          ),
        ),
      ),
    ).toDomainGraphNodePackage
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      timeout.duration,
    )
    graph
      .standingQueries(namespace)
      .getOrElse(fail("default namespace should exist"))
      .createStandingQuery(
        name = "distinct-id-any-node",
        pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
          dgnId = dgnPackage.dgnId,
          formatReturnAsStr = true,
          aliasReturnAs = Symbol("id"),
          includeCancellation = false,
          origin = PatternOrigin.DirectDgb,
        ),
        // This query runs for the life of the graph. `cancelStandingQuery` completes only once
        // `terminateOutputQueue` has, and a query with no sink never drains the results it buffered,
        // so cancelling this one after it has matched would not return.
        outputs = Map.empty,
        sqId = sqId,
      )
    graph
  }

  /** Snapshots on every sleep. */
  private val alwaysSnapshot: GraphService =
    makeGraph("equivalence-always", PersistenceConfig(snapshotAfterEvents = 0))

  /** Snapshots only once a node has journaled enough to be worth it, so some nodes skip. */
  private val thresholded: GraphService =
    makeGraph("equivalence-threshold", PersistenceConfig(snapshotAfterEvents = 16))

  /** Never snapshots, so every node is rebuilt from its journal alone. */
  private val neverSnapshot: GraphService =
    makeGraph("equivalence-never", PersistenceConfig(snapshotAfterEvents = Int.MaxValue))

  private val graphs: List[(String, GraphService)] =
    List("always" -> alwaysSnapshot, "threshold" -> thresholded, "never" -> neverSnapshot)

  implicit val ec: ExecutionContextExecutor = alwaysSnapshot.system.dispatcher

  override def afterAll(): Unit =
    graphs.foreach { case (_, g) => Await.result(g.shutdown(), timeout.duration) }

  private val quiet = idProvider.customIdToQid(1L) // stays under the threshold
  private val busy = idProvider.customIdToQid(2L) // crosses it comfortably
  // chainA -> chainB -> chainC. `chainB` is the case that matters: it is subscribed to by `chainA`
  // and subscribed to `chainC`, so it both receives a result and reports one.
  private val chainA = idProvider.customIdToQid(3L)
  private val chainB = idProvider.customIdToQid(4L)
  private val chainC = idProvider.customIdToQid(5L)

  private def workload(graph: GraphService): Future[Unit] = {
    val ops = graph.literalOps(namespace)
    def setProps(qid: QuineId, count: Int, offset: Int = 0): Future[Unit] =
      (1 to count).foldLeft(Future.unit) { (prior, i) =>
        prior.flatMap(_ => ops.setProp(qid, s"p${i + offset}", QuineValue.Integer(i.toLong)))
      }
    for {
      _ <- setProps(quiet, 3)
      _ <- setProps(busy, 20)
      _ <- setProps(chainA, 5)
      _ <- setProps(chainC, 2)
      _ <- ops.addEdge(chainA, chainB, "points-at", isDirected = true)
      _ <- ops.addEdge(chainB, chainC, "points-at", isDirected = true)
      // A second round after the first sleep, so the restored node is written to again.
      _ <- sleepAll(graph)
      _ <- setProps(quiet, 2, offset = 100)
      _ <- setProps(busy, 5, offset = 100)
    } yield ()
  }

  private def sleepAndAwait(graph: GraphService, qid: QuineId): Future[Unit] = {
    def stillAwake(): Future[Boolean] =
      graph
        .relayAsk(
          graph.shardFromNode(qid).quineRef,
          ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
        )
        .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))
        .map(_.contains(qid))

    graph.requestNodeSleep(namespace, qid).flatMap { _ =>
      Patterns
        .retry(
          () => stillAwake().filter(_ == false),
          attempts = 100,
          delay = 50.millis,
          graph.system.scheduler,
          graph.system.dispatcher,
        )
        .map(_ => ())
    }
  }

  private def sleepAll(graph: GraphService): Future[Unit] =
    List(quiet, busy, chainA, chainB, chainC).foldLeft(Future.unit)((prior, q) =>
      prior.flatMap(_ => sleepAndAwait(graph, q)),
    )

  /** The parts of a restored node that must not depend on how it was restored.
    *
    * `latestUpdateMillisAfterSnapshot` and the journal itself are deliberately excluded: the first
    * differs by definition between these configurations, and event times differ between separate
    * runs of the same workload.
    */
  private type Observable = (
    Map[Symbol, String],
    Set[HalfEdge],
    Set[(Long, Option[QuineId], Option[String], Set[String], Option[Boolean])],
    Set[(Long, QuineId, Set[String], Option[Boolean])],
    Long,
  )

  private def observe(graph: GraphService, qid: QuineId): Future[Observable] =
    graph.literalOps(namespace).logState(qid).map { state =>
      // Standing query ids are minted per graph, so two rigs running the same schedule never share them. What is
      // comparable is the *shape*: which subscriber holds which queries alongside which others. Ranking the ids
      // within a node's own observation preserves that and drops the identity that cannot match.
      val subs = state.sqStateResults.subscribers
      val idx = state.sqStateResults.subscriptions
      // Labelled by the pattern the query was registered for, which every graph compared here shares. Ranking by
      // uuid would look canonical and not be: the uuids are random per graph, so the ordering -- and with it the
      // rank -- differs, and two identical states compare unequal. This suite registers one query, so that bug was
      // latent rather than visible.
      // This suite registers exactly one standing query, so every id seen here is that one and a constant label
      // is its canonical form. Registering a second would make this wrong: it would then need labelling by the
      // pattern each query was registered for, as `SnapshotJournalEquivalenceProperties` does.
      def rank(q: StandingQueryId): String = { val _ = q; "the-query" }
      def normalizeSubs(
        rs: List[DistinctIdSubscriberState],
      ): Set[(Long, Option[QuineId], Option[String], Set[String], Option[Boolean])] =
        rs.map(r =>
          (r.dgnId, r.subscriberNode, r.subscriberQuery.map(rank), r.forQueries.map(rank).toSet, r.lastResult),
        ).toSet
      def normalizeIndex(rs: List[DistinctIdIndexState]): Set[(Long, QuineId, Set[String], Option[Boolean])] =
        rs.map(r => (r.dgnId, r.peer, r.forQueries.map(rank).toSet, r.answer)).toSet
      (
        state.properties,
        state.edges,
        normalizeSubs(subs),
        normalizeIndex(idx),
        state.graphNodeHashCode,
      )
    }

  private def restoreAll(graph: GraphService): Future[Map[QuineId, Observable]] =
    sleepAll(graph).flatMap(_ =>
      Future
        .traverse(List(quiet, busy, chainA, chainB, chainC))(q => observe(graph, q).map(q -> _))
        .map(_.toMap),
    )

  test("a node restored from its journal matches the node a snapshot would have restored") {
    for {
      _ <- graphs.foldLeft(Future.unit)((prior, g) => prior.flatMap(_ => workload(g._2)))
      always <- restoreAll(alwaysSnapshot)
      threshold <- restoreAll(thresholded)
      never <- restoreAll(neverSnapshot)
    } yield {
      // Three graphs agreeing is not the same as three graphs being right: a defect that reaches
      // every configuration equally satisfies the comparison below. So pin what the workload
      // actually implies before comparing, and pin it on every graph rather than on the fixture.
      graphs.foreach { case (label, _) =>
        val observed = label match {
          case "always" => always
          case "threshold" => threshold
          case _ => never
        }
        withClue(s"$label: the workload should have written every property: ") {
          observed(quiet)._1.keySet.map(_.name) shouldBe Set("p1", "p2", "p3", "p101", "p102")
          observed(busy)._1.keySet.map(_.name) should have size 25
        }
        // chainA -> chainB -> chainC satisfies the pattern end to end, so chainB has told chainA
        // the chain matches. What is being pinned is that the answer is known: `None` is a node
        // restored without knowing what it last reported, and that is what re-reports on the next
        // write.
        withClue(s"$label: chainB should have told its subscriber the chain matches: ") {
          // Node subscribers only. Every node is a potential root, so chainB also has the query itself as a
          // subscriber for the root pattern it does not match; that `false` is correct and beside the point here.
          observed(chainB)._3.collect { case (_, Some(_), _, _, lastResult) => lastResult } shouldBe Set(Some(true))
        }
        // chainB watches chainC under more than one sub-pattern, and chainC satisfies only one of
        // them, so a `false` here is an answer and not a gap. An unanswered watch is the gap.
        withClue(s"$label: chainB should have a settled answer from every node it watches: ") {
          val heard = observed(chainB)._4.map(_._4)
          heard should contain(Some(true))
          heard should not contain None
        }
      }

      // Per node, so a failure names the one that diverged instead of printing all of them.
      List("quiet" -> quiet, "busy" -> busy, "chainA" -> chainA, "chainB" -> chainB, "chainC" -> chainC)
        .foreach { case (label, qid) =>
          withClue(s"$label: skipping the snapshot below the threshold changed it: ")(
            threshold(qid) shouldBe always(qid),
          )
          withClue(s"$label: restoring from the journal alone changed it: ")(
            never(qid) shouldBe always(qid),
          )
        }
      succeed
    }
  }
}

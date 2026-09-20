package com.thatdot.quine.graph

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer}
import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.edges.ReverseOrderedEdgeCollection
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.messaging.StandingQueryMessage.DomainNodeSubscriptionResult
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  HalfEdge,
  PropertyComparisonFunctions,
  PropertyValue,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor, PersistenceConfig, StatelessPrimePersistor}
import com.thatdot.quine.util.TestLogging._

/** A peer's answer replayed into a result nobody holds is taken up by the next pattern replayed that can use it.
  * Whether that pattern had the answer live cannot be read off the journal: the journal of a pattern that took an
  * existing answer up, and the journal of a pattern that asked afresh after the result was torn down, are the same
  * three events. What tells them apart is the peer's reply. A peer answers an ask, or a change, and never repeats
  * itself, so a reply that lands on such a result is a reply to an ask, and the pattern that took the answer up on
  * replay was in fact waiting for it live. Its last-notified value goes back to what it was before the claim and
  * the reply is reported as its first answer.
  */
class ReplayedAnswerClaimTests extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(10.seconds)

  private val node: QuineId = QuineId(Array(1.toByte))
  private val peer: QuineId = QuineId(Array(2.toByte))
  private val liveQuery: StandingQueryId = StandingQueryId(new UUID(0L, 1L))
  private val goneQuery: StandingQueryId = StandingQueryId(new UUID(0L, 2L))

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private def twoHop(rootKey: String): SingleBranch = SingleBranch(
    hasProperty(rootKey),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )
  // The live pattern and the cancelled one share the child `region` and differ at the root.
  private val livePattern: SingleBranch = twoHop("other")
  private val gonePattern: SingleBranch = twoHop("kind")
  private val liveDgn: Long = livePattern.toDomainGraphNodePackage.dgnId
  private val goneDgn: Long = gonePattern.toDomainGraphNodePackage.dgnId
  private val regionDgn: Long = SingleBranch(hasProperty("region"), nextBranches = Nil).toDomainGraphNodePackage.dgnId

  /** Every result the live query has emitted; positive ones are the reports. */
  private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()

  private def withGraph(check: GraphService => Assertion): Assertion = {
    captured.clear()
    val metricRegistry = new MetricRegistry
    val _ = SharedMetricRegistries.add(HostQuineMetrics.MetricsRegistryName, metricRegistry)
    val system = ActorSystem(
      "replayed-answer-claim",
      ConfigFactory
        .load()
        .withValue("pekko.actor.provider", ConfigValueFactory.fromAnyRef("local"))
        .withValue(
          "pekko.extensions",
          ConfigValueFactory.fromIterable(
            java.util.Arrays.asList("com.thatdot.quine.graph.messaging.NodeActorMailboxExtension"),
          ),
        ),
    )
    val primePersistor = new StatelessPrimePersistor(
      PersistenceConfig(),
      None,
      (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
    )(Materializer.matFromSystem(system), logConfig)
    Await.result(primePersistor.syncVersion(), 10.seconds)

    val graph = new GraphService(
      system,
      primePersistor,
      IdentityIdProvider,
      shardCount = 1,
      inMemorySoftNodeLimit = Some(50000),
      inMemoryHardNodeLimit = Some(75000),
      effectOrder = EventEffectOrder.PersistorFirst,
      declineSleepWhenWriteWithinMillis = 0L,
      declineSleepWhenAccessWithinMillis = 0L,
      labelsProperty = Symbol("__LABEL"),
      edgeCollectionFactory = new ReverseOrderedEdgeCollection(_),
      metrics = HostQuineMetrics(enableDebugMetrics = false, metricRegistry, omitDefaultNamespace = false),
    )

    try {
      // Only the live pattern is registered; the cancelled one's body is gone from the registry, as after a cancel.
      val dgnPackage = livePattern.toDomainGraphNodePackage
      Await.result(
        graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, liveQuery, skipPersistor = true),
        10.seconds,
      )
      graph
        .standingQueries(defaultNamespaceId)
        .getOrElse(fail("default namespace should exist"))
        .createStandingQuery(
          name = "live",
          pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
            dgnId = dgnPackage.dgnId,
            formatReturnAsStr = false,
            aliasReturnAs = Symbol("id"),
            includeCancellation = false,
            origin = PatternOrigin.DirectDgb,
          ),
          outputs = Map(
            "capture" -> Flow[StandingQueryResult]
              .viaMat(KillSwitches.single)(Keep.right)
              .map { r =>
                captured.add(r)
                MasterStream.SqResultsExecToken("replayed-answer-claim")
              }
              .to(Sink.ignore),
          ),
          sqId = liveQuery,
        )

      // Exactly what the node journaled: its properties and edge; the cancelled query's subscription; the peer's
      // answer for it; then the live query's subscription, made after the cancel tore the result down and told
      // the peer to forget, so the live pattern asked afresh. The reply to that ask is not in the journal.
      val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor"))
      Await.result(
        persistor.persistNodeChangeEvents(
          node,
          NonEmptyList.of(
            NodeEvent.WithTime(
              PropertyEvent.PropertySet(Symbol("other"), PropertyValue(QuineValue.Str("x"))),
              EventTime(1_000_000L, timestampSequence = 0L),
            ),
            NodeEvent.WithTime(
              EdgeEvent.EdgeAdded(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, peer)),
              EventTime(1_000_000L, timestampSequence = 1L),
            ),
          ),
        ),
        10.seconds,
      )
      Await.result(
        persistor.persistDomainIndexEvents(
          node,
          NonEmptyList.of(
            NodeEvent.WithTime(
              DomainIndexEvent.CreateDomainStandingQuerySubscription(goneDgn, goneQuery, Set(goneQuery)),
              EventTime(1_000_000L, timestampSequence = 2L),
            ),
            NodeEvent.WithTime(
              DomainIndexEvent.DomainNodeSubscriptionResult(peer, regionDgn, result = true),
              EventTime(1_000_000L, timestampSequence = 3L),
            ),
            // The cancel, recorded where the node applied it, naming the answer it took away. Without this row a
            // replay cannot know the answer was torn down before the live pattern asked, because a cancelled
            // query's pattern is deleted and there is nothing left to ask which answers were its.
            NodeEvent.WithTime(
              DomainIndexEvent.CancelDomainStandingQuerySubscription(goneDgn, goneQuery),
              EventTime(1_000_000L, timestampSequence = 4L),
            ),
            NodeEvent.WithTime(
              DomainIndexEvent.CreateDomainStandingQuerySubscription(liveDgn, liveQuery, Set(liveQuery)),
              EventTime(1_000_000L, timestampSequence = 5L),
            ),
          ),
        ),
        10.seconds,
      )
      check(graph)
    } finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  private def wake(graph: GraphService): Unit = {
    val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(node), 20.seconds)
  }

  /** The peer's reply, as the message it sends. */
  private def replyFromPeer(graph: GraphService, answer: Boolean): Unit =
    graph.relayTell(
      SpaceTimeQuineId(node, defaultNamespaceId, None),
      DomainNodeSubscriptionResult(peer, regionDgn, answer),
    )

  private def reports: Int = captured.asScala.count {
    case StandingQueryResult(StandingQueryResult.Meta(true), _) => true
    case _ => false
  }

  private def eventually(deadline: Long = 10.seconds.toNanos)(cond: => Boolean): Boolean = {
    val end = System.nanoTime() + deadline
    while (!cond && System.nanoTime() < end) Thread.sleep(50)
    cond
  }

  test("the reply to an ask made before the sleep is reported, though replay had already claimed the old answer") {
    withGraph { graph =>
      wake(graph)
      withClue("the live query has reported nothing before the reply: ")(reports shouldBe 0)
      replyFromPeer(graph, answer = true)
      withClue("the reply is the live query's first answer: ")(
        eventually()(reports == 1) shouldBe true,
      )
      Thread.sleep(300)
      withClue("and it is reported once: ")(reports shouldBe 1)
    }
  }

  test("a change from the peer after the claim is reported once, as it is live") {
    withGraph { graph =>
      wake(graph)
      replyFromPeer(graph, answer = false)
      Thread.sleep(300)
      withClue("a false answer is not a match: ")(reports shouldBe 0)
      replyFromPeer(graph, answer = true)
      withClue("the rise is reported: ")(eventually()(reports == 1) shouldBe true)
      Thread.sleep(300)
      reports shouldBe 1
    }
  }
}

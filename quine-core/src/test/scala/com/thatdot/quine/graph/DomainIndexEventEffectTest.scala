package com.thatdot.quine.graph

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ConcurrentNavigableMap}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.LiteralMessage.SqStateResults
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  CancelDomainNodeSubscription,
  CreateDomainNodeSubscription,
  DomainNodeSubscriptionResult,
}
import com.thatdot.quine.graph.messaging.{ShardMessage, SpaceTimeQuineId, StandingQueryMessage}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
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

/** A DistinctId journal row stands for a change, the rule property and edge events have always held to.
  *
  * `domainIndexEventHasEffect` is what enforces it, and each of its cases asks the state the event lands in
  * whether the effect is already there, by the same test the applying code uses. What that buys is stated three
  * ways here: a command that changes nothing costs no row; the same command that does change something costs
  * exactly one; and a command that changes nothing is still *served*, because a subscriber re-asking a question
  * may have lost the answer it was given, and re-asking is how it gets it back.
  *
  * The third of those is why this is not a unit test of the predicate: applying is not gated, only the journal
  * write is, and the difference between them is only visible from outside the node.
  *
  * ==Every node here is woken before it is told anything==
  *
  * [[com.thatdot.quine.graph.messaging.NodeActorMailbox.shouldIgnoreWhenSleeping]] discards a
  * `CancelDomainNodeSubscription` addressed to a node that is not awake, on the grounds that the node checks
  * for cancellations itself at its next wake. A test that sent one to a sleeping node would be measuring that
  * rule rather than the has-effect gate, and would pass whatever the gate did. So each case wakes its node
  * first, and the rule itself is pinned by a case of its own at the end.
  *
  * ==How "nothing was written" is decided==
  *
  * A negative has no arrival to wait for, so every case pairs the no-op with a command that *does* write, sends
  * the no-op first, and waits for the second command's row. One actor writes the rows in order, so the second
  * row arriving is a happens-after for the first command having been handled. The count is then exact rather
  * than eventually-exact.
  */
class DomainIndexEventEffectTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  /** The single duration here: a safety valve, so a case that can never reach its row fails instead of hanging. */
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val domainIndexEvents: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
    new ConcurrentHashMap()
  private val journals: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] =
    new ConcurrentHashMap()

  private val graph: GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        // Never snapshot: the journal is then the whole record of what each node was told.
        PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
        None,
        (pc, ns) =>
          new InMemoryPersistor(
            journals = journals,
            domainIndexEvents = domainIndexEvents,
            persistenceConfig = pc,
            namespace = ns,
          ),
      )(Materializer.matFromSystem(system), logConfig)
    val g = Await.result(
      GraphService(
        "domain-index-event-effect",
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      awaitTimeout,
    )
    g.requiredGraphIsReady()
    g
  }

  override def afterAll(): Unit = { val _ = Await.result(graph.shutdown(), awaitTimeout) }

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private val regionBranch: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val twoHop: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
  )
  private val twoHopDgn: DomainGraphNodeId = twoHop.toDomainGraphNodePackage.dgnId
  private val regionDgn: DomainGraphNodeId = regionBranch.toDomainGraphNodePackage.dgnId

  // Registered so a node can evaluate them. No standing query is created: nothing here turns on a query
  // running, and leaving them out keeps the wake-time sweeps from writing rows of their own.
  locally {
    val _ = Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(
        twoHop.toDomainGraphNodePackage,
        StandingQueryId.fresh(),
        skipPersistor = true,
      ),
      awaitTimeout,
    )
  }

  private val ids = Iterator.from(1)
  private def freshNode(): QuineId = idProvider.customIdToQid(ids.next().toLong)

  private def tell(node: QuineId, msg: StandingQueryMessage.DomainNodeSubscriptionCommand): Unit =
    graph.relayTell(SpaceTimeQuineId(node, namespace, None), msg)

  /** Read the node's state. Also what wakes it, and what says everything sent before this has been handled: a
    * node serves its mailbox in order, so a reply to this one means the earlier ones are done.
    */
  private def stateOf(node: QuineId): SqStateResults =
    Await.result(graph.literalOps(namespace).logState(node), awaitTimeout).sqStateResults

  private def wake(node: QuineId): Unit = { val _ = stateOf(node) }

  private def rowsOn(node: QuineId): List[DomainIndexEvent] =
    Option(domainIndexEvents.get(node)).fold(List.empty[DomainIndexEvent])(_.values.asScala.toList)

  /** Block until `node` has journaled `n` DistinctId rows, failing if it never does. The loop ends on the
    * condition; the duration only bounds a case that can never reach it.
    */
  private def awaitRows(node: QuineId, n: Int): List[DomainIndexEvent] = {
    val deadline = System.nanoTime() + awaitTimeout.toNanos
    while (rowsOn(node).size < n && System.nanoTime() < deadline) Thread.sleep(10)
    val rows = rowsOn(node)
    if (rows.size < n)
      fail(s"only ${rows.size} of $n expected journal rows were written on node $node; rows were $rows")
    rows
  }

  private def awaitCondition(what: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + awaitTimeout.toNanos
    while (!condition && System.nanoTime() < deadline) Thread.sleep(10)
    if (!condition) fail(s"never reached: $what")
  }

  test("a subscription for a subscriber and queries already recorded writes no journal row") {
    val node = freshNode()
    val peer = freshNode()
    val q1 = StandingQueryId.fresh()
    val q2 = StandingQueryId.fresh()
    wake(node)

    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q1)))
    val _ = awaitRows(node, 1)
    // A verbatim repeat, then a command that does change something. The second row arriving says the repeat
    // has been handled, so the total is exact.
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q1)))
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q1, q2)))
    val rows = awaitRows(node, 2)

    withClue(s"rows on the node were $rows: ")(rows.size shouldBe 2)
    withClue("the repeat is the one that should be missing: ")(
      rows shouldBe List(
        DomainIndexEvent.CreateDomainNodeSubscription(twoHopDgn, peer, Set(q1)),
        DomainIndexEvent.CreateDomainNodeSubscription(twoHopDgn, peer, Set(q1, q2)),
      ),
    )
    withClue("and the subscriber holds the union of what it named: ")(
      stateOf(node).subscribers.filter(_.subscriberNode.contains(peer)).flatMap(_.forQueries).toSet shouldBe
      Set(q1, q2),
    )
  }

  test("a subscription that changes nothing is still answered, because the subscriber may have lost the answer") {
    // The asymmetry with a property event: the write is gated, the effect is not. A node rebuilt from its
    // journal can hold a subscription and not the answer it was given, and re-asking is the only way back.
    val node = freshNode()
    val subscriber = freshNode()
    val anchor = freshNode()
    val q = StandingQueryId.fresh()
    Await.result(graph.literalOps(namespace).setProp(node, "region", QuineValue.Str("r")), awaitTimeout)
    wake(node)

    tell(node, CreateDomainNodeSubscription(regionDgn, Left(subscriber), Set(q)))
    // What is waited for is the node's own record of having reported, which is what a repeat has to reproduce.
    // The subscriber itself never asked, so it refuses the answer and holds nothing.
    awaitCondition("the node answered its new subscriber") {
      stateOf(node).subscribers.exists(r => r.subscriberNode.contains(subscriber) && r.lastResult.contains(true))
    }
    withClue("the subscriber never asked, so it records nothing: ")(
      stateOf(subscriber).subscriptions.filter(_.peer == node) shouldBe empty,
    )

    val rowsBefore = rowsOn(node).size
    // The same subscription again: nothing new is recorded, so no row -- but it is still served.
    tell(node, CreateDomainNodeSubscription(regionDgn, Left(subscriber), Set(q)))
    tell(node, CreateDomainNodeSubscription(regionDgn, Left(anchor), Set(q)))
    val rows = awaitRows(node, rowsBefore + 1)
    withClue(s"rows were $rows: the repeat must not have been journaled: ")(rows.size shouldBe rowsBefore + 1)
    withClue("and the node holds both subscribers, each having been answered: ")(
      stateOf(node).subscribers.flatMap(_.subscriberNode).toSet shouldBe Set(subscriber, anchor),
    )
  }

  test("an answer about a pattern this node does not ask about is refused and writes no row") {
    // The case that matters for more than volume: a fold records an answer even where it has not re-derived the
    // subscription that asked, so a row the live node threw away would come back as an entry it never had.
    val node = freshNode()
    val peer = freshNode()
    val q = StandingQueryId.fresh()
    wake(node)

    tell(node, DomainNodeSubscriptionResult(peer, regionDgn, result = true))
    // An anchor that does write, to give the refusal a happens-after.
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q)))
    val rows = awaitRows(node, 1)

    withClue(s"rows were $rows: ")(
      rows shouldBe List(DomainIndexEvent.CreateDomainNodeSubscription(twoHopDgn, peer, Set(q))),
    )
    withClue("and nothing was recorded about the refused answer: ")(
      stateOf(node).subscriptions.filter(_.dgnId == regionDgn) shouldBe empty,
    )
  }

  test("an answer repeating what is already recorded writes no row") {
    val root = freshNode()
    val leaf = freshNode()
    val q = StandingQueryId.fresh()
    val ops = graph.literalOps(namespace)
    Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
    Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
    Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)
    wake(root)

    // A subscription to the root makes it ask the leaf, and the leaf's answer is what gets repeated.
    tell(root, CreateDomainNodeSubscription(twoHopDgn, Left(freshNode()), Set(q)))
    awaitCondition("the root holds the leaf's answer") {
      stateOf(root).subscriptions.exists(r => r.peer == leaf && r.dgnId == regionDgn && r.answer.contains(true))
    }

    val rowsBefore = rowsOn(root).size
    tell(root, DomainNodeSubscriptionResult(leaf, regionDgn, result = true))
    // The anchor: the same answer the other way, which does change what the root holds.
    tell(root, DomainNodeSubscriptionResult(leaf, regionDgn, result = false))
    val rows = awaitRows(root, rowsBefore + 1)

    withClue(s"rows after the repeat and the change were $rows: ")(rows.size shouldBe rowsBefore + 1)
    withClue("the row that was written is the change, not the repeat: ")(
      rows.last shouldBe DomainIndexEvent.DomainNodeSubscriptionResult(leaf, regionDgn, result = false),
    )
  }

  test("cancelling for someone who is not a subscriber writes no row") {
    // Cancelling removes nothing, and the teardown that follows a removal is reached only when one happened,
    // so there is no effect for a row to stand for.
    val node = freshNode()
    val stranger = freshNode()
    val peer = freshNode()
    val q = StandingQueryId.fresh()
    wake(node)

    tell(node, CancelDomainNodeSubscription(twoHopDgn, stranger))
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q)))
    val afterCreate = awaitRows(node, 1)
    withClue(s"rows were $afterCreate: the cancellation for a stranger must not be among them: ")(
      afterCreate shouldBe List(DomainIndexEvent.CreateDomainNodeSubscription(twoHopDgn, peer, Set(q))),
    )

    // And one that does remove a subscriber writes exactly one row.
    tell(node, CancelDomainNodeSubscription(twoHopDgn, peer))
    val rows = awaitRows(node, 2)
    withClue(s"rows were $rows: ")(rows.last shouldBe DomainIndexEvent.CancelDomainNodeSubscription(twoHopDgn, peer))
    withClue("and the subscriber is gone: ")(stateOf(node).subscribers shouldBe empty)
  }

  test("a second cancellation for the same subscriber writes no further row") {
    val node = freshNode()
    val peer = freshNode()
    val q = StandingQueryId.fresh()
    wake(node)

    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q)))
    tell(node, CancelDomainNodeSubscription(twoHopDgn, peer))
    val _ = awaitRows(node, 2)

    tell(node, CancelDomainNodeSubscription(twoHopDgn, peer))
    // An anchor: subscribing again does change something.
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(q)))
    val rows = awaitRows(node, 3)
    withClue(s"rows were $rows: ")(rows.size shouldBe 3)
    withClue("the third row is the re-subscription, not a second cancellation: ")(
      rows.last shouldBe DomainIndexEvent.CreateDomainNodeSubscription(twoHopDgn, peer, Set(q)),
    )
  }

  private def sleepNode(node: QuineId): Unit = {
    Await.result(graph.requestNodeSleep(namespace, node), awaitTimeout)
    awaitCondition(s"node $node went to sleep")(!awakeNodes().contains(node))
  }

  private def awakeNodes(): Set[QuineId] =
    // Every shard, not just the one this node happens to live on: `SampleAwakeNodes` answers only for the
    // shard it is sent to, and a reading that missed a shard would silently turn "evict this node" into a
    // no-op and every claim about its restore into a claim about a node that never slept.
    Await.result(
      Future
        .traverse(graph.shards.toList) { shard =>
          graph
            .relayAsk(shard.quineRef, ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _))
            .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))(
              ExecutionContext.parasitic,
            )
        }(implicitly, ExecutionContext.parasitic)
        .map(_.foldLeft(Set.empty[QuineId])(_ union _))(ExecutionContext.parasitic),
      awaitTimeout,
    )

  test("a cancellation addressed to a node that is not awake is discarded before the node ever sees it") {
    // Not the has-effect gate but the mailbox rule above it, pinned because every case in this suite is written
    // around it: a `CancelDomainNodeSubscription` to a sleeping node is dropped, and the node is expected to
    // find the cancellation for itself at its next wake instead.
    //
    // The subscriber names no query, which is what keeps the wake-time sweep from retiring it for reasons of
    // its own: an empty set is unknown rather than dead, so the subscriber survives and the dropped
    // cancellation is what the reading afterwards is about. The dead case is the test below.
    val node = freshNode()
    val peer = freshNode()
    wake(node)
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set.empty))
    val _ = awaitRows(node, 1)
    sleepNode(node)

    tell(node, CancelDomainNodeSubscription(twoHopDgn, peer))
    // Reading wakes the node, which is what would have applied the cancellation had it survived.
    withClue("the cancellation was dropped, so the subscriber is still there: ")(
      stateOf(node).subscribers.flatMap(_.subscriberNode) should contain(peer),
    )
    withClue("and nothing was journaled for it: ")(rowsOn(node).size shouldBe 1)
  }

  test("a node subscriber whose every named query has stopped is retired at the wake that discovers it") {
    // The other half of the rule above, and what makes dropping the message safe. Nobody tells this node
    // anything: the queries the subscriber named do not run, so the subscription is retired here, and the
    // retirement is journaled at the point this node discovered it -- which is the one ordering a replay needs
    // and which the query's absence cannot supply afterwards.
    val node = freshNode()
    val peer = freshNode()
    val stopped = StandingQueryId.fresh()
    wake(node)
    tell(node, CreateDomainNodeSubscription(twoHopDgn, Left(peer), Set(stopped)))
    val _ = awaitRows(node, 1)
    sleepNode(node)

    withClue("the subscriber is gone after the wake: ")(stateOf(node).subscribers shouldBe empty)
    val rows = awaitRows(node, 2)
    withClue(s"rows were $rows: ")(
      rows.last shouldBe DomainIndexEvent.CancelDomainNodeSubscription(twoHopDgn, peer),
    )
  }
}

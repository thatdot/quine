package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.messaging.LiteralMessage.{LocallyRegisteredStandingQuery, NodeInternalState}
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.messaging.StandingQueryMessage.CreateMultipleValuesStandingQuerySubscription
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber.NodeSubscriber
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineValue}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** A `CreateMultipleValuesStandingQuerySubscription` can reach a node after its standing query was cancelled. The
  * cancel removes the query from the graph's index and tells no node, so a subscription already queued, or sent by a
  * hub that still holds the cancelled query, names a part the node can no longer resolve. The node must drop that
  * message. Recording a state for the part before finding it missing crashes the node, and the restarted actor is
  * built from the arguments it woke with: it stops for good, or it serves the properties it had at that wake.
  *
  * These tests send the stale message directly, so the race that a cancel otherwise depends on happens every time.
  * Each test uses its own property key, because part ids derive from a part's content, and a part some other test
  * left running would resolve.
  */
class StaleMultipleValuesSubscriptionTest extends AsyncFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val namespace: NamespaceId = defaultNamespaceId

  private val edgeName = Symbol("e")

  val graph: GraphService = Await.result(
    GraphService(
      "stale-multiple-values-subscription",
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = (system: ActorSystem) =>
        new StatelessPrimePersistor(
          persistenceConfig = PersistenceConfig(),
          None,
          (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
        )(Materializer.matFromSystem(system), logConfig): PrimePersistor,
      idProvider = idProvider,
    ),
    timeout.duration,
  )
  graph.requiredGraphIsReady()

  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher

  override def afterAll(): Unit = Await.result(graph.shutdown(), timeout.duration)

  private def sqns =
    graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))

  /** A hub part that relays, across each outgoing `e` edge, whether the node on the far side has `propKey`. */
  private def acrossEdge(propKey: String): MultipleValuesStandingQuery.SubscribeAcrossEdge =
    MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(edgeName),
      edgeDirection = Some(EdgeDirection.Outgoing),
      andThen = MultipleValuesStandingQuery.LocalProperty(
        propKey = Symbol(propKey),
        propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
        aliasedAs = Some(Symbol(propKey)),
      ),
    )

  /** Register a standing query whose root is `query`. Nodes are not told: this graph propagates only on request. */
  private def startQuery(name: String, query: MultipleValuesStandingQuery): StandingQueryId = {
    val sqId = StandingQueryId.fresh()
    val _ = sqns.createStandingQuery(
      name = name,
      pattern = StandingQueryPattern.MultipleValuesQueryPattern(
        query,
        includeCancellation = false,
        origin = PatternOrigin.DirectSqV4,
      ),
      outputs = Map.empty,
      sqId = sqId,
    )
    sqId
  }

  private def cancelQuery(sqId: StandingQueryId): Future[Unit] =
    sqns.cancelStandingQuery(sqId).getOrElse(fail(s"standing query $sqId should be running")).map(_ => ())

  /** Wake `node` with `propKey` set, before the test's query exists, so its wake registers none of that query's
    * states. A node that wakes registers every query running then, which includes queries that earlier tests leave
    * running.
    */
  private def wakeWithProperty(node: QuineId, propKey: String): Future[Unit] =
    graph.literalOps(namespace).setProp(node, propKey, QuineValue.Integer(1L))

  private def subscribe(node: QuineId, subscriber: NodeSubscriber, query: MultipleValuesStandingQuery): Unit =
    graph.relayTell(
      SpaceTimeQuineId(node, namespace, None),
      CreateMultipleValuesStandingQuerySubscription(subscriber, query),
    )

  /** The node's state, read through its mailbox after the subscription: a node that stopped does not answer. */
  private def stateOf(node: QuineId): Future[NodeInternalState] = graph.literalOps(namespace).logState(node)

  private def statesOf(state: NodeInternalState, sqId: StandingQueryId): Vector[LocallyRegisteredStandingQuery] =
    state.multipleValuesStandingQueryStates.filter(_.globalId == sqId.toString)

  test("a running query's subscription creates a state on the node it is sent to") {
    // Without this, the stale-message tests below would also pass if the message never reached the handler.
    val node = idProvider.customIdToQid(1L)
    val hub = idProvider.customIdToQid(2L)
    val part = acrossEdge("running")
    for {
      _ <- wakeWithProperty(node, "running")
      sqId = startQuery("running", part)
      subscriber = NodeSubscriber(hub, sqId, part.andThen.queryPartId)
      _ = subscribe(node, subscriber, part)
      state <- stateOf(node)
    } yield {
      val registered = statesOf(state, sqId)
      withClue("this query's states on the node: ")(registered.map(_.id) shouldBe Vector(part.queryPartId.toString))
      registered.head.subscribers shouldBe Set(subscriber.pretty(idProvider, logConfig))
    }
  }

  test("a cancelled query's edge subscription is dropped, and the node keeps answering") {
    // Recorded before it is found missing, this state stops the restarted actor: its constructor reads the state's
    // query, which was never set, and the node never answers again.
    val node = idProvider.customIdToQid(10L)
    val hub = idProvider.customIdToQid(11L)
    val part = acrossEdge("acrossEdge")
    for {
      _ <- wakeWithProperty(node, "acrossEdge")
      sqId = startQuery("cancelled-across-edge", part)
      _ <- cancelQuery(sqId)
      _ = subscribe(node, NodeSubscriber(hub, sqId, part.andThen.queryPartId), part)
      state <- stateOf(node)
    } yield {
      withClue("this query's states on the node: ")(statesOf(state, sqId) shouldBe empty)
      state.properties.keySet shouldBe Set(Symbol("acrossEdge"))
    }
  }

  test("a cancelled query's reciprocal subscription is dropped, and the node keeps its properties") {
    // Recorded before it is found missing, this state crashes the node and the restart succeeds, from the empty
    // properties this node woke with. The property set after that wake is gone from memory.
    val node = idProvider.customIdToQid(20L)
    val hub = idProvider.customIdToQid(21L)
    val part = acrossEdge("reciprocal")
    val reciprocal =
      MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
        halfEdge = HalfEdge(edgeName, EdgeDirection.Incoming, hub),
        andThenId = part.andThen.queryPartId,
      )
    for {
      _ <- wakeWithProperty(node, "reciprocal")
      sqId = startQuery("cancelled-reciprocal", part)
      _ <- cancelQuery(sqId)
      _ = subscribe(node, NodeSubscriber(hub, sqId, part.queryPartId), reciprocal)
      state <- stateOf(node)
    } yield {
      withClue("this query's states on the node: ")(statesOf(state, sqId) shouldBe empty)
      state.properties.keySet shouldBe Set(Symbol("reciprocal"))
    }
  }
}

package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.HalfEdge
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}
import com.thatdot.quine.util.TestLogging._

/** Regression coverage for [[com.thatdot.quine.graph.messaging.LiteralMessage.DeleteNodeCommand]] (the path used by
  * `DETACH DELETE` and `literalOps.deleteNode`): deleting a node must remove each neighbor's reciprocal half edge, so
  * nothing is left dangling.
  *
  * `DeleteNodeCommand` reads `edges.all` (a single-use iterator) both to build the node's own edge-removal events and
  * to message each neighbor a `RemoveHalfEdgeCommand`. If that iterator is consumed once and read a second time, the
  * neighbor fan-out sees an empty iterator and silently notifies no one, leaving dangling reciprocals. A dangling half
  * edge is invisible to Cypher (edge matching is reciprocity-checked) and observable only via `getHalfEdges`, so these
  * assert against that directly.
  *
  * The graph is built with the anti-sleep grace windows disabled so that [[BaseGraph.requestNodeSleep]] takes effect
  * promptly, letting the tests control neighbor residency deterministically (observed via [[BaseGraph.recentNodes]]).
  */
class DeleteNodeCommandTests extends AnyFunSuite with Eventually with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(10.seconds)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(150, Millis))

  val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  val graph: LiteralOpsGraph = Await.result(
    GraphService(
      "delete-node-command-tests",
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = idProvider,
      declineSleepWhenWriteWithinMillis = 0L,
      declineSleepWhenAccessWithinMillis = 0L,
    ),
    timeout.duration,
  )

  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher
  val namespace: NamespaceId = defaultNamespaceId
  private def ops = graph.literalOps(namespace)

  override def afterAll(): Unit = Await.result(graph.shutdown(), timeout.duration * 2L)

  /** Half edges on `neighbor` that point at `deleted` — i.e. the reciprocals that deleting `deleted` should remove.
    * Uses `getHalfEdges` (not `getEdges`) so a dangling reciprocal with no matching other half is still observed.
    */
  private def reciprocalsPointingAt(neighbor: QuineId, deleted: QuineId): Set[HalfEdge] =
    Await.result(ops.getHalfEdges(neighbor, withId = Some(deleted)), timeout.duration)

  private def awakeNodes(): Set[QuineId] =
    Await.result(graph.recentNodes(10000, namespace), timeout.duration)

  private def sleepAndConfirmAsleep(qid: QuineId): Unit = {
    Await.result(graph.requestNodeSleep(namespace, qid), timeout.duration)
    val _ = eventually(assert(!awakeNodes().contains(qid), s"node $qid should be asleep"))
  }

  test("deleteNode removes an awake neighbor's reciprocal half edge") {
    val hub = graph.idProvider.newQid()
    val neighbor = graph.idProvider.newQid()
    Await.result(ops.addEdge(hub, neighbor, "knows"), timeout.duration)

    assert(awakeNodes().contains(neighbor))
    assert(reciprocalsPointingAt(neighbor, hub).nonEmpty)

    Await.result(ops.deleteNode(hub), timeout.duration)
    assert(reciprocalsPointingAt(neighbor, hub).isEmpty)
  }

  test("deleteNode removes a sleeping neighbor's reciprocal half edge (waking it)") {
    val hub = graph.idProvider.newQid()
    val neighbor = graph.idProvider.newQid()
    Await.result(ops.addEdge(hub, neighbor, "knows"), timeout.duration)
    assert(reciprocalsPointingAt(neighbor, hub).nonEmpty)

    sleepAndConfirmAsleep(neighbor)

    Await.result(ops.deleteNode(hub), timeout.duration)
    assert(reciprocalsPointingAt(neighbor, hub).isEmpty)
  }

  test("deleteNode cleans up reciprocals across many neighbors") {
    val hub = graph.idProvider.newQid()
    val neighbors = Vector.fill(6)(graph.idProvider.newQid())
    neighbors.foreach(n => Await.result(ops.addEdge(hub, n, "knows"), timeout.duration))
    neighbors.foreach(n => assert(reciprocalsPointingAt(n, hub).nonEmpty))

    Await.result(ops.deleteNode(hub), timeout.duration)
    neighbors.foreach(n => assert(reciprocalsPointingAt(n, hub).isEmpty, s"reciprocal on $n should be gone"))
  }

  test("deleteNode removes multiple half edges to the same neighbor") {
    val hub = graph.idProvider.newQid()
    val neighbor = graph.idProvider.newQid()
    Await.result(ops.addEdge(hub, neighbor, "knows"), timeout.duration)
    Await.result(ops.addEdge(hub, neighbor, "likes"), timeout.duration)
    assert(reciprocalsPointingAt(neighbor, hub).size == 2)

    Await.result(ops.deleteNode(hub), timeout.duration)
    assert(reciprocalsPointingAt(neighbor, hub).isEmpty)
  }
}

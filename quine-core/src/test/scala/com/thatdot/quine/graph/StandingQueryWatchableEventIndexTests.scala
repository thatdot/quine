package com.thatdot.quine.graph

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.StandingQueryWatchableEventIndex.EventSubscriber
import com.thatdot.quine.graph.cypher.EdgeSubscriptionReciprocalState
import com.thatdot.quine.graph.edges.EdgeCollectionView
import com.thatdot.quine.model.{DomainEdge, EdgeDirection, GenericEdge, HalfEdge, PropertyValue, QuineValue}

/** Which query parts the index says to notify, as parts come and go.
  *
  * A property and an edge can share a name (nothing stops a graph having both `owner` the property and `owner` the
  * edge), so the two indexes are keyed by the same symbols and one standing in for the other is invisible until a
  * part unsubscribes and takes an unrelated one with it.
  */
class StandingQueryWatchableEventIndexTests extends AnyFunSuite {

  private val sqId = StandingQueryId(new UUID(1L, 2L))
  private def subscriber(n: Int): EventSubscriber =
    EventSubscriber(sqId -> MultipleValuesStandingQueryPartId(new UUID(0L, n.toLong)))

  private val shared = Symbol("owner")

  /** A node with no edges. What is registered here watches edges by name, and the answer is always "none". */
  private def emptyEdges: EdgeCollectionView = new EdgeCollectionView {
    def size: Int = 0
    def all: Iterator[HalfEdge] = Iterator.empty
    def nonEmpty: Boolean = false
    def matching(edgeType: Symbol): Iterator[HalfEdge] = Iterator.empty
    def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge] = Iterator.empty
    def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge] = Iterator.empty
    def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = Iterator.empty
    def matching(direction: EdgeDirection): Iterator[HalfEdge] = Iterator.empty
    def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = Iterator.empty
    def matching(id: QuineId): Iterator[HalfEdge] = Iterator.empty
    def matching(genEdge: GenericEdge): Iterator[HalfEdge] = Iterator.empty
    def contains(edge: HalfEdge): Boolean = false
    def hasUniqueGenEdges(requiredEdges: Set[DomainEdge], thisQid: QuineId): Boolean = requiredEdges.isEmpty
  }

  private def noProperties: Map[Symbol, PropertyValue] = Map.empty

  /** A node whose edges cannot be looked at without it being noticed. */
  private def edgesThatRefuseToBeRead: EdgeCollectionView = new EdgeCollectionView {
    private def refuse: Nothing = throw new AssertionError(
      "registering a subscriber that can learn nothing from this node's edges read them anyway. Producing the " +
      "description is what reads them, so asking for one and discarding it is not free. It is the whole cost, " +
      "paid on exactly the nodes that can least afford it.",
    )
    def size: Int = refuse
    def all: Iterator[HalfEdge] = refuse
    def nonEmpty: Boolean = refuse
    def matching(edgeType: Symbol): Iterator[HalfEdge] = refuse
    def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge] = refuse
    def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge] = refuse
    def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = refuse
    def matching(direction: EdgeDirection): Iterator[HalfEdge] = refuse
    def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = refuse
    def matching(id: QuineId): Iterator[HalfEdge] = refuse
    def matching(genEdge: GenericEdge): Iterator[HalfEdge] = refuse
    def contains(edge: HalfEdge): Boolean = refuse
    def hasUniqueGenEdges(requiredEdges: Set[DomainEdge], thisQid: QuineId): Boolean = refuse
  }

  test("registering interest without a description never looks at the node") {
    val index = StandingQueryWatchableEventIndex.empty
    val watcher = subscriber(1)

    index.registerStandingQuery(watcher, WatchableEventType.EdgeChange(Some(shared)))

    assert(index.watchingForEdge.get(shared).exists(_.contains(watcher)), "the subscriber was not registered")
    withClue("and it hears about an edge from then on, exactly as one registered with a description does") {
      var told = 0
      index.standingQueriesWatchingNodeEvent(
        EdgeEvent.EdgeAdded(HalfEdge(shared, EdgeDirection.Outgoing, QuineId(Array(2.toByte)))),
        _ => { told += 1; false },
      )
      assert(told == 1)
    }
  }

  test("a reciprocal declares it can learn nothing from the node's edges, so registering it reads none of them") {
    val index = StandingQueryWatchableEventIndex.empty
    val reciprocal = EdgeSubscriptionReciprocalState(
      MultipleValuesStandingQueryPartId(new UUID(10L, 11L)),
      HalfEdge(shared, EdgeDirection.Incoming, QuineId(Array(1.toByte))),
      MultipleValuesStandingQueryPartId(new UUID(12L, 13L)),
    )
    val labels = Symbol("__LABEL")
    val canInform = reciprocal.initialEventTypes(labels).toSet
    assert(canInform.isEmpty, "a reciprocal claimed it could learn something from the node's current state")

    // The rule a node applies to each of a state's relevant categories: describe it where the state says it can
    // learn something, and otherwise register without asking for a description at all. Under a node that cannot
    // bear to have its edges read, only one of those two is survivable.
    reciprocal.relevantEventTypes(labels).foreach { eventType =>
      if (canInform.contains(eventType))
        index.registerStandingQuery(subscriber(1), eventType, Map.empty, edgesThatRefuseToBeRead)
      else index.registerStandingQuery(subscriber(1), eventType)
    }
    assert(index.watchingForEdge.get(shared).exists(_.contains(subscriber(1))))
  }

  test("a part that stops watching an edge leaves the property watchers of the same name alone") {
    val index = StandingQueryWatchableEventIndex.empty
    val watchesProperty = subscriber(1)
    val watchesEdge = subscriber(2)

    val _ = index.registerStandingQuery(
      watchesProperty,
      WatchableEventType.PropertyChange(shared),
      Map(shared -> PropertyValue(QuineValue.Str("x"))),
      emptyEdges,
    )
    val _ =
      index.registerStandingQuery(watchesEdge, WatchableEventType.EdgeChange(Some(shared)), noProperties, emptyEdges)

    // The last (and only) part watching that edge goes away. What must go with it is that edge's entry.
    index.unregisterStandingQuery(watchesEdge, WatchableEventType.EdgeChange(Some(shared)))

    assert(!index.watchingForEdge.contains(shared), "the edge index kept an entry with nobody in it")
    assert(
      index.watchingForProperty.get(shared).exists(_.contains(watchesProperty)),
      "unregistering an edge watcher evicted the property watchers that happened to share its name",
    )
  }

  test("a part that stops watching a property leaves the edge watchers of the same name alone") {
    val index = StandingQueryWatchableEventIndex.empty
    val watchesProperty = subscriber(1)
    val watchesEdge = subscriber(2)

    val _ =
      index.registerStandingQuery(watchesProperty, WatchableEventType.PropertyChange(shared), noProperties, emptyEdges)
    val _ =
      index.registerStandingQuery(watchesEdge, WatchableEventType.EdgeChange(Some(shared)), noProperties, emptyEdges)

    index.unregisterStandingQuery(watchesProperty, WatchableEventType.PropertyChange(shared))

    assert(!index.watchingForProperty.contains(shared))
    assert(index.watchingForEdge.get(shared).exists(_.contains(watchesEdge)))
  }
}

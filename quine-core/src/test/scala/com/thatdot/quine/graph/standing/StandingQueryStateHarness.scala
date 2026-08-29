package com.thatdot.quine.graph.standing

import scala.collection.mutable

import org.scalactic.source.Position
import org.scalatest.Assertions

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.{
  CrossState,
  EdgeSubscriptionReciprocalState,
  HeapEdgeContributionStore,
  MultipleValuesInitializationEffects,
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryEffects,
  QueryContext,
  SubscribeAcrossEdgeState,
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{
  MultipleValuesStandingQuerySubscriber,
  NewMultipleValuesStateResult,
}
import com.thatdot.quine.graph.{
  AbstractNodeActor,
  EdgeEvent,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  PropertyEvent,
  QuineIdLongProvider,
  StandingQueryId,
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineIdProvider}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.util.TestLogging._

/** Mocked up handler of standing query effects - instead of actually doing anything with the
  * effects, they just get queued up for easy testing
  *
  * @param subscriptionsCreated queue of calls made to `createSubscription`
  * @param subscriptionsCancelled queue of calls made to `cancelSubscription`
  * @param resultsReported queue of calls made to `reportNewResult`
  * @param executingNodeId ID of the fake node on which this is running
  * @param idProvider ID provider
  */
final case class MultipleValuesStandingQueryEffectsTester(
  subscriptionsCreated: mutable.Queue[(QuineId, MultipleValuesStandingQuery)],
  subscriptionsCancelled: mutable.Queue[(QuineId, MultipleValuesStandingQueryPartId)],
  resultsReported: mutable.Queue[Seq[QueryContext]],
  resultsReportedToNode: mutable.Queue[(QuineId, Seq[QueryContext])],
  resultsReportedToRemoteParts: mutable.Queue[(QuineId, MultipleValuesStandingQueryPartId, Seq[QueryContext])],
  executingNodeId: QuineId,
  idProvider: QuineIdProvider,
  knownQueries: mutable.Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery],
) extends MultipleValuesStandingQueryEffects
    with MultipleValuesInitializationEffects {

  var currentProperties: Map[Symbol, PropertyValue] = Map.empty
  val labelsProperty: Symbol = MultipleValuesStandingQueryEffectsTester.labelsProperty

  private[this] def refuseIfDeriving(effect: String): Unit =
    MultipleValuesStandingQueryEffectsTester.refuseIfDeriving(effect)

  /** The subscribers a state is answering, as the node would hold them. */
  val subscribers: mutable.Set[MultipleValuesStandingQuerySubscriber] = mutable.Set.empty

  /** This node's edges. A node applies edge events to its collection before telling its standing queries about them,
    * so the harness does the same: a state that asks about its edges while handling an event sees the event applied.
    */
  val edges: mutable.Set[HalfEdge] = mutable.Set.empty

  def trackPropertyEffects(events: Seq[NodeChangeEvent]): Unit = events.foreach {
    case PropertySet(key, value) => currentProperties += key -> value
    case PropertyRemoved(key, _) => currentProperties -= key
    case _: EdgeEvent => ()
  }

  def trackEdgeEffects(events: Seq[NodeChangeEvent]): Unit = events.foreach {
    case EdgeEvent.EdgeAdded(halfEdge) => edges += halfEdge
    case EdgeEvent.EdgeRemoved(halfEdge) => edges -= halfEdge
    case _: PropertyEvent => ()
  }

  def createSubscription(onNode: QuineId, query: MultipleValuesStandingQuery): Unit = {
    refuseIfDeriving("a subscription")
    knownQueries += query.queryPartId -> query
    subscriptionsCreated.enqueue(onNode -> query)
  }

  def cancelSubscription(onNode: QuineId, queryId: MultipleValuesStandingQueryPartId): Unit = {
    refuseIfDeriving("a cancellation")
    subscriptionsCancelled.enqueue(onNode -> queryId)
  }

  def reportUpdatedResults(resultGroup: Seq[QueryContext]): Unit = {
    refuseIfDeriving("a result report")
    resultsReported.enqueue(resultGroup)
  }

  def reportUpdatedResultsTo(
    subscriber: MultipleValuesStandingQuerySubscriber,
    resultGroup: Seq[QueryContext],
  ): Unit = {
    refuseIfDeriving("a result report")
    subscriber match {
      case MultipleValuesStandingQuerySubscriber.NodeSubscriber(subscribingNode, _, _) =>
        resultsReportedToNode.enqueue(subscribingNode -> resultGroup)
      case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(_) =>
        resultsReported.enqueue(resultGroup)
    }
  }

  def reportUpdatedResultsToNode(onNode: QuineId, resultGroup: Seq[QueryContext]): Unit = {
    refuseIfDeriving("a result report")
    if (subscribingNodes.exists(_ == onNode)) resultsReportedToNode.enqueue(onNode -> resultGroup)
  }

  def reportUpdatedResultsToRemotePart(
    onNode: QuineId,
    forPart: MultipleValuesStandingQueryPartId,
    resultGroup: Seq[QueryContext],
  ): Unit = {
    refuseIfDeriving("a result report")
    resultsReportedToRemoteParts.enqueue((onNode, forPart, resultGroup))
  }

  def reportUpdatedResultsToEntitledNodes(resultGroup: Seq[QueryContext], checkAtMost: Int)(
    entitled: QuineId => Boolean,
  ): Unit = {
    refuseIfDeriving("a result report")
    // The same walk the node does, so that what a test pins about the bound is the rule rather than this stand-in.
    MultipleValuesStandingQueryEffects.eachEntitledNodeSubscriber(
      subscribers,
      checkAtMost,
      node => {
        entitlementQuestions.enqueue(node)
        entitled(node)
      },
    )(subscriber => resultsReportedToNode.enqueue(subscriber.subscribingNode -> resultGroup))
  }

  /** Which nodes `entitled` was actually asked about, so a test can pin that the bound is what stopped the asking
    * rather than the subscribers running out. An observation rather than an effect, so it is not in [[isEmpty]].
    */
  val entitlementQuestions: mutable.Queue[QuineId] = mutable.Queue.empty

  private[this] def subscribingNodes: Iterable[QuineId] =
    subscribers.collect { case MultipleValuesStandingQuerySubscriber.NodeSubscriber(subscribingNode, _, _) =>
      subscribingNode
    }

  /** Stand in for a node whose edges are in the persistor and whose read of them failed.
    *
    * The only thing a state can observe about that is the `None`, so that is the whole of what this does. It is a
    * variable rather than a constructor argument because the interesting tests turn it on partway through: a state
    * behaves one way while the node can answer and another way once it cannot, and the difference is the point.
    */
  var edgesCanBeRead: Boolean = true

  def matchingEdgesTo(
    edgeName: Option[Symbol],
    edgeDirection: Option[EdgeDirection],
    other: QuineId,
  ): Option[Seq[HalfEdge]] =
    Option.when(edgesCanBeRead)(
      edges.iterator
        .filter(halfEdge =>
          halfEdge.other == other &&
          edgeName.forall(_ == halfEdge.edgeType) &&
          edgeDirection.forall(_ == halfEdge.direction),
        )
        .toSeq,
    )

  def isEmpty: Boolean =
    subscriptionsCreated.isEmpty && subscriptionsCancelled.isEmpty &&
    resultsReported.isEmpty && resultsReportedToNode.isEmpty && resultsReportedToRemoteParts.isEmpty

  def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery = knownQueries(
    queryPartId,
  )
}
object MultipleValuesStandingQueryEffectsTester {

  /** Create an empty effects tester
    *
    * @param idProvider ID provider
    * @return empty effects tester
    */
  def empty(
    query: MultipleValuesStandingQuery,
    initiallyKnownQueries: Seq[MultipleValuesStandingQuery] = Seq.empty,
    idProvider: QuineIdProvider = QuineIdLongProvider(),
  ): MultipleValuesStandingQueryEffectsTester =
    new MultipleValuesStandingQueryEffectsTester(
      mutable.Queue.empty,
      mutable.Queue.empty,
      mutable.Queue.empty,
      mutable.Queue.empty,
      mutable.Queue.empty,
      idProvider.newQid(),
      idProvider,
      knownQueries =
        mutable.Map(query.queryPartId -> query) ++= initiallyKnownQueries.map(sq => sq.queryPartId -> sq).toMap,
    )

  val labelsProperty: Symbol = Symbol("__LABEL")

  /** What a state is doing, if what it is doing must not reach past the node it runs on.
    *
    * Working out what a state already knows (rehydrating it, reading its results, filling a copy of what a
    * co-located child holds) has to be answerable from this node alone. That is the whole reason a node persists
    * what crossed the wire: so that coming back does not require asking anybody. A state that sent a message while
    * deriving would turn every wake into a storm of subscriptions, and would do it silently, because the results
    * would still be right.
    *
    * Held here rather than per-tester because one derivation can span several states, each holding its own tester:
    * filling a parent's copy from its child is exactly that. These tests are single-threaded, so one flag is enough.
    */
  private[this] var deriving: Option[String] = None

  /** Run something that is only allowed to consult this node, failing the test if it does not stay here. */
  def whileDeriving[A](what: String)(derive: => A): A = {
    val outer = deriving
    deriving = Some(what)
    try derive
    finally deriving = outer
  }

  def refuseIfDeriving(effect: String): Unit =
    deriving.foreach { what =>
      throw new AssertionError(
        s"A standing query state performed $effect during $what, which must be answerable from this node alone. " +
        "Deriving may never leave the node. That is what makes waking cheap rather than a subscription storm.",
      )
    }
}

/** Harness for checking the behaviour of a [[StandingQueryState]] when it receives different
  * data
  *
  * @param query the query being checked
  * @param effects how effects are mocked up
  */
class StandingQueryStateWrapper[S <: MultipleValuesStandingQuery](
  final val query: S,
  final val knownQueries: Seq[MultipleValuesStandingQuery] = Seq.empty,
) extends Assertions {
  final val sqState: query.State = query.createState()
  final val effects: MultipleValuesStandingQueryEffectsTester =
    MultipleValuesStandingQueryEffectsTester.empty(query, knownQueries)

  /** Whether to put this state through the persistence codec after every step and check that what comes back says
    * the same thing.
    *
    * A state must answer correctly at any point after it is initialized, including after the node it lives on has
    * slept and woken, which is a claim about every step of every scenario, not about the end of one. Doing it here
    * makes each existing test into a test of that too, at every point along the way.
    *
    * Off where a revival legitimately answers differently, with the reason stated at the exemption.
    */
  def checkpointsEveryStep: Boolean = sqState match {
    // A cross's not-ready-yet gate is deliberately not persisted (a revived cross re-reports rather than
    // remembering it had reported), so its answer before the first report legitimately differs across a revival.
    case _: CrossState => false
    // A state keeping its per-edge rows outside the heap deliberately writes none of them into its blob, so the blob
    // alone is not what it comes back from: the rows are read back from wherever the store put them. Reviving one of
    // those is a property of the store, and is checked where that store is.
    case acrossEdge: SubscribeAcrossEdgeState
        if !acrossEdge.contributionStore.isInstanceOf[HeapEdgeContributionStore] =>
      false
    case _ => true
  }

  def testInvariants()(implicit pos: Position): Unit = ()

  /** Write this state down, read it back, and require the revived one to answer exactly as the live one does.
    *
    * The revived state is then discarded rather than swapped in. A wake is allowed to change what a state *does*
    * next (some states deliberately keep nothing about what they last reported, so the first event after a wake
    * reports again), but never what it *answers*, which is the thing every reader depends on.
    */
  private[this] def checkpoint()(implicit pos: Position): Unit = if (checkpointsEveryStep) {
    val subscription = MultipleValuesStandingQueryPartSubscription(
      query.queryPartId,
      StandingQueryStateWrapper.globalId,
      mutable.Set.from(effects.subscribers),
    )
    val bytes = MultipleValuesStandingQueryStateCodec.format.write(subscription -> sqState)
    val revived = MultipleValuesStandingQueryStateCodec.format
      .read(bytes)
      .fold(err => fail(s"a state this test had just built could not be read back: $err"), _._2)
    MultipleValuesStandingQueryEffectsTester.whileDeriving("rehydrate after a checkpoint")(revived.rehydrate(effects))
    val revivedAnswer = MultipleValuesStandingQueryEffectsTester.whileDeriving("readResults after a checkpoint")(
      revived.readResults(effects.currentProperties, effects.labelsProperty),
    )
    val liveAnswer = MultipleValuesStandingQueryEffectsTester.whileDeriving("readResults")(
      sqState.readResults(effects.currentProperties, effects.labelsProperty),
    )
    assert(
      revivedAnswer == liveAnswer,
      s"a state written down and read back answers $revivedAnswer where the state it was written from answers " +
      s"$liveAnswer",
    )

    // Once per subscriber as well, because a state whose answer depends on who is asking has no answer that does
    // not: the reciprocal's `readResults` is always `None`, so comparing it alone compares nothing at all.
    effects.subscribers.foreach { subscriber =>
      val revivedFor = MultipleValuesStandingQueryEffectsTester.whileDeriving("readResultsFor after a checkpoint")(
        revived.readResultsFor(subscriber, effects),
      )
      val liveFor = MultipleValuesStandingQueryEffectsTester.whileDeriving("readResultsFor")(
        sqState.readResultsFor(subscriber, effects),
      )
      assert(
        revivedFor == liveFor,
        s"a state written down and read back answers $revivedFor to $subscriber where the state it was written " +
        s"from answers $liveFor",
      )
    }

    // And where it says its rows are. A revived state that has forgotten they are somewhere other than its own blob
    // goes looking in a heap that holds none of them, and reports an emptiness it has no grounds for.
    (sqState, revived) match {
      case (live: EdgeSubscriptionReciprocalState, back: EdgeSubscriptionReciprocalState) =>
        assert(
          !(live.subscriberStore.isDefined || live.subscribersExternalized) || back.subscribersExternalized,
          "a state whose subscribers are recorded elsewhere was written down without saying so",
        )
        assert(back.externalSubscriberForQuery == live.externalSubscriberForQuery)
        ()
      case (live: SubscribeAcrossEdgeState, back: SubscribeAcrossEdgeState) =>
        assert(
          !(live.contributionStore.keepsRowsElsewhere || live.edgeResultsExternalized) || back.edgeResultsExternalized,
          "a state whose per-edge rows are recorded elsewhere was written down without saying so",
        )
        ()
      case _ => ()
    }
  }

  def initialize[A](
    initialProperties: Map[Symbol, PropertyValue] = Map.empty,
  )(
    thenCheck: (MultipleValuesStandingQueryEffectsTester, Option[Seq[QueryContext]]) => A,
  )(implicit pos: Position): A = {
    val initialPropertyEvents: Seq[NodeChangeEvent] = initialProperties.map { case (k, v) => PropertySet(k, v) }.toSeq
    effects.trackPropertyEffects(initialPropertyEvents)
    // Rehydration is derivation; initialization is not, and is where a state is meant to reach off the node.
    MultipleValuesStandingQueryEffectsTester.whileDeriving("rehydrate")(sqState.rehydrate(effects))
    sqState.onInitialize(effects)
    sqState.onNodeEvents(initialPropertyEvents, effects)
    testInvariants()
    checkpoint()
    thenCheck(effects, readResults())
  }

  /** Simulate node change events
    *
    * @param events events being simulated
    * @param shouldHaveEffects assert whether this should cause an update in the state
    * @param thenCheck after processing the events, check something about the state
    * @return output of the check
    */
  def reportNodeEvents[A](events: Seq[NodeChangeEvent], shouldHaveEffects: Boolean)(
    thenCheck: MultipleValuesStandingQueryEffectsTester => A,
  )(implicit pos: Position): A = {
    // emulate deduplication behavior of nodes w.r.t propertyevents
    val finalEvents =
      if (events.forall(_.isInstanceOf[PropertyEvent]))
        AbstractNodeActor.internallyDeduplicatePropertyEvents(
          events.collect { case pe: PropertyEvent => pe }.toList,
        )
      else events
    // emulate the node applying events to itself before reporting them to standing queries
    effects.trackPropertyEffects(finalEvents)
    effects.trackEdgeEffects(finalEvents)
    val hadEffects = sqState.onNodeEvents(finalEvents, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "New node events did not have the expected effects analysis",
    )
    testInvariants()
    checkpoint()
    thenCheck(effects)
  }

  /** Simulate new subscription results
    *
    * @param result subscription result simulated
    * @param shouldHaveEffects assert whether this should cause an update in the state
    * @param thenCheck after processing the subscription, check something about the state
    * @return output of the check
    */
  def reportNewSubscriptionResult[A](result: NewMultipleValuesStateResult, shouldHaveEffects: Boolean)(
    thenCheck: MultipleValuesStandingQueryEffectsTester => A,
  )(implicit pos: Position): A = {
    val hadEffects = sqState.onNewSubscriptionResult(result, effects)
    assert(
      shouldHaveEffects == hadEffects,
      "New node events did not have the expected effects analysis",
    )
    testInvariants()
    checkpoint()
    thenCheck(effects)
  }

  def readResults(): Option[Seq[QueryContext]] =
    MultipleValuesStandingQueryEffectsTester.whileDeriving("readResults")(
      sqState.readResults(effects.currentProperties, effects.labelsProperty),
    )

  /** Add a subscriber, as a node would on receiving a subscription, and read what this state owes it. */
  def addSubscriber(subscriber: MultipleValuesStandingQuerySubscriber): Option[Seq[QueryContext]] = {
    effects.subscribers += subscriber
    MultipleValuesStandingQueryEffectsTester.whileDeriving("readResultsFor")(
      sqState.readResultsFor(subscriber, effects),
    )
  }
}

object StandingQueryStateWrapper {

  /** Any query id will do: what is under test is the state, and the id only has to survive the round trip. */
  val globalId: StandingQueryId = StandingQueryId(new java.util.UUID(1L, 2L))
}

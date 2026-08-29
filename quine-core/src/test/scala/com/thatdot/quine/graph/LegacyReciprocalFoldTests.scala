package com.thatdot.quine.graph

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.LegacyReciprocalFoldFixtures.{
  andThenQuery,
  answeredCachedResult,
  reciprocalQueryTo,
  subscribeAcrossEdgeQuery,
  subscribingRoots,
}
import com.thatdot.quine.graph.NodeActor.MultipleValuesStandingQueries
import com.thatdot.quine.graph.StaticNodeSupport.{foldLegacyReciprocals, persistLegacyReciprocalFold}
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.{EdgeSubscriptionReciprocalState, MultipleValuesStandingQuery}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber.NodeSubscriber
import com.thatdot.quine.persistor.InMemoryPersistor
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.util.TestLogging._

/** Folding reciprocal states that were persisted under per-half-edge part ids into the one state per constraints
  * that serves them now.
  *
  * The inputs are the captured bytes in [[LegacyReciprocalFoldFixtures]]: rows exactly as an earlier release left
  * them on disk, whose ids nothing can compute anymore. There is no wipe migration behind this, and the fold is
  * the entire compatibility story for these rows, so what these tests check is not that some cleanup happens, but
  * that a deployment's standing queries keep working across the identity change with no operator action.
  */
class LegacyReciprocalFoldTests extends AnyFunSuite with OptionValues {

  private def lookupPart(partId: MultipleValuesStandingQueryPartId): Option[MultipleValuesStandingQuery] =
    Seq[MultipleValuesStandingQuery](subscribeAcrossEdgeQuery, andThenQuery).find(_.queryPartId == partId)

  private def loadStates(): MultipleValuesStandingQueries = {
    val states: MultipleValuesStandingQueries = mutable.Map.empty
    LegacyReciprocalFoldFixtures.load().foreach { case pair @ (subscription, _) =>
      states += (subscription.globalId -> subscription.forQuery) -> pair
    }
    states
  }

  private val globalId = LegacyReciprocalFoldFixtures.globalId
  private val canonicalPartId = reciprocalQueryTo(subscribingRoots.head).queryPartId

  test("the ids in the captured rows are not the ids their content hashes to now") {
    // If this fails, the fixtures no longer represent legacy data, or the funnel regressed to keying per edge.
    val decoded =
      LegacyReciprocalFoldFixtures.load().collect { case (subscription, _: EdgeSubscriptionReciprocalState) =>
        subscription.forQuery
      }
    assert(decoded.size == subscribingRoots.size)
    decoded.foreach(legacyId => assert(legacyId != canonicalPartId))
    withClue("and every root's subscription now names one shared state") {
      assert(subscribingRoots.map(reciprocalQueryTo(_).queryPartId).distinct == Seq(canonicalPartId))
    }
  }

  test("legacy rows fold to one canonical state carrying every subscriber") {
    val states = loadStates()
    val legacyKeys = states.keySet.filter { case (_, partId) =>
      states(globalId -> partId)._2.isInstanceOf[EdgeSubscriptionReciprocalState]
    }
    val fold = foldLegacyReciprocals(states, lookupPart)

    withClue("one canonical reciprocal replaces the per-root states") {
      val (subscription, state) = states.get(globalId -> canonicalPartId).value
      val reciprocal = state match { case r: EdgeSubscriptionReciprocalState => r; case other => fail(other.toString) }
      assert(
        subscription.subscribers == subscribingRoots
          .map(NodeSubscriber(_, globalId, subscribeAcrossEdgeQuery.queryPartId))
          .toSet,
      )
      withClue("seeded with an answered cache regardless of which row folded first") {
        assert(reciprocal.cachedResult.value == answeredCachedResult)
      }
      assert(!states.keySet.exists(legacyKeys))
    }

    withClue("the andThen's subscriber set names the canonical state, once") {
      val (andThenSubscription, _) = states.get(globalId -> andThenQuery.queryPartId).value
      assert(
        andThenSubscription.subscribers ==
          Set(NodeSubscriber(LegacyReciprocalFoldFixtures.farNode, globalId, canonicalPartId)),
      )
    }

    withClue("what is owed to disk: canonical writes before the andThen, deletes of exactly the legacy rows") {
      assert(fold.updatedStates.head == (globalId -> canonicalPartId))
      assert(fold.updatedStates.contains(globalId -> andThenQuery.queryPartId))
      assert(fold.deletableRows.toSet == legacyKeys)
    }
  }

  test("folding is idempotent: a folded map folds to nothing") {
    val states = loadStates()
    val _ = foldLegacyReciprocals(states, lookupPart)
    val before = states.toMap
    val second = foldLegacyReciprocals(states, lookupPart)
    assert(!second.nonEmpty)
    assert(states.toMap == before)
  }

  test("a fold interrupted after the canonical write converges at the next wake") {
    // First wake: fold, but only the canonical blob reached disk before the crash. The andThen still names the
    // legacy ids and the legacy rows are still there. The next wake reads that mix and folds it again.
    val firstWake = loadStates()
    val firstFold = foldLegacyReciprocals(firstWake, lookupPart)
    val canonicalPair = firstWake.get(globalId -> canonicalPartId).value

    val secondWake = loadStates() // the legacy rows, as the interrupted deletes left them
    secondWake += (globalId -> canonicalPartId) -> canonicalPair // the one write that landed
    val secondFold = foldLegacyReciprocals(secondWake, lookupPart)

    withClue("no subscriber is lost and the end state is identical") {
      val (subscription, _) = secondWake.get(globalId -> canonicalPartId).value
      assert(
        subscription.subscribers == subscribingRoots
          .map(NodeSubscriber(_, globalId, subscribeAcrossEdgeQuery.queryPartId))
          .toSet,
      )
      val (andThenSubscription, _) = secondWake.get(globalId -> andThenQuery.queryPartId).value
      assert(
        andThenSubscription.subscribers ==
          Set(NodeSubscriber(LegacyReciprocalFoldFixtures.farNode, globalId, canonicalPartId)),
      )
      assert(secondFold.deletableRows.toSet == firstFold.deletableRows.toSet)
    }
  }

  test("a legacy row with no subscribers left is deleted without creating anything") {
    val states = loadStates()
    states.collect { case (key, (subscription, _: EdgeSubscriptionReciprocalState)) => key -> subscription }.foreach {
      case (_, subscription) => subscription.subscribers.clear()
    }
    val fold = foldLegacyReciprocals(states, lookupPart)
    assert(states.get(globalId -> canonicalPartId).isEmpty)
    assert(fold.deletableRows.size == subscribingRoots.size)
    withClue("and the andThen does not keep entries for the deleted rows: it would report to them forever") {
      val (andThenSubscription, _) = states.get(globalId -> andThenQuery.queryPartId).value
      assert(andThenSubscription.subscribers.isEmpty)
      assert(fold.updatedStates.contains(globalId -> andThenQuery.queryPartId))
    }
  }

  /** The canonical states whose node is not subscribed to their `andThen`, the invariant described on
    * [[EdgeSubscriptionReciprocalState]]. The fold may leave exactly the states it assembled out of rows that had
    * never subscribed; `updateMultipleValuesStandingQueriesOnNode` links those after every wake, so what these
    * tests pin is that the fold leaves no *other* state unlinked, and reports honestly which ones it did.
    */
  private def unlinkedReciprocals(
    states: MultipleValuesStandingQueries,
  ): Set[(StandingQueryId, MultipleValuesStandingQueryPartId)] =
    states.collect {
      case (key @ (sqId, partId), (_, reciprocal: EdgeSubscriptionReciprocalState))
          if !states
            .get(sqId -> reciprocal.andThenId)
            .exists(_._1.subscribers.contains(NodeSubscriber(LegacyReciprocalFoldFixtures.farNode, sqId, partId))) =>
        key
    }.toSet

  test("rows that never subscribed fold to a canonical state, and the fold says it could not link it") {
    // A reciprocal persisted while its own edge was absent (the subscribing node's half existed, this node's half
    // did not, or came and went) has subscribers but no subscription to the andThen, and possibly no andThen
    // state on the node at all. The fold merges its subscribers like any other row, but it has nothing to hand the
    // canonical state by way of a subscription: establishing one takes the ordinary subscription path, which runs
    // at wake, after the fold.
    val states: MultipleValuesStandingQueries = mutable.Map.empty
    val legacyKeys = subscribingRoots.zipWithIndex.map { case (root, i) =>
      val legacyPartId = MultipleValuesStandingQueryPartId(new java.util.UUID(0L, 7000L + i))
      val legacyState = EdgeSubscriptionReciprocalState(
        legacyPartId,
        LegacyReciprocalFoldFixtures.halfEdgeTo(root),
        andThenQuery.queryPartId,
      )
      val subscription = MultipleValuesStandingQueryPartSubscription(
        legacyPartId,
        globalId,
        mutable.Set[MultipleValuesStandingQuerySubscriber](
          NodeSubscriber(root, globalId, subscribeAcrossEdgeQuery.queryPartId),
        ),
      )
      states += (globalId -> legacyPartId) -> (subscription -> legacyState)
      globalId -> legacyPartId
    }

    val fold = foldLegacyReciprocals(states, lookupPart)

    val (subscription, _) = states.get(globalId -> canonicalPartId).value
    assert(
      subscription.subscribers == subscribingRoots
        .map(NodeSubscriber(_, globalId, subscribeAcrossEdgeQuery.queryPartId))
        .toSet,
    )
    assert(fold.deletableRows.toSet == legacyKeys.toSet)
    withClue("no andThen state was conjured, so the canonical state is the one thing left to link at wake") {
      assert(states.get(globalId -> andThenQuery.queryPartId).isEmpty)
      assert(unlinkedReciprocals(states) == Set(globalId -> canonicalPartId))
    }
  }

  test("a fold of rows that had subscribed leaves nothing for the wake-time link check to do") {
    val states = loadStates()
    val _ = foldLegacyReciprocals(states, lookupPart)
    assert(unlinkedReciprocals(states).isEmpty)
  }

  test("what a fold writes is decided before any of it is written") {
    val handedOver = mutable.ArrayBuffer.empty[(MultipleValuesStandingQueryPartId, Array[Byte])]
    val held = Promise[Unit]()
    val persistor = new InMemoryPersistor() {
      override def setMultipleValuesStandingQueryState(
        standingQuery: StandingQueryId,
        id: QuineId,
        standingQueryPartId: MultipleValuesStandingQueryPartId,
        state: Option[Array[Byte]],
      ): Future[Unit] = {
        state.foreach(bytes => handedOver.synchronized(handedOver += (standingQueryPartId -> bytes)))
        held.future.flatMap(_ =>
          super.setMultipleValuesStandingQueryState(standingQuery, id, standingQueryPartId, state),
        )(
          ExecutionContext.parasitic,
        )
      }
    }

    val states = loadStates()
    val fold = foldLegacyReciprocals(states, lookupPart)
    val expectedSubscribers = states(globalId -> canonicalPartId)._1.subscribers.toSet
    val expectedAndThenSubscribers = states(globalId -> andThenQuery.queryPartId)._1.subscribers.toSet
    val persistence = persistLegacyReciprocalFold(fold, states, persistor, LegacyReciprocalFoldFixtures.farNode)

    // The node is live-correct from the moment the fold returns, so it runs, and its subscribers change while the
    // writes are still in flight. A chain that serialized each state as it reached it would run that on whichever
    // thread finished the write before, reading a subscriber set the node was in the middle of changing, and would
    // then delete the legacy rows that were the only other copy.
    val latecomer = NodeSubscriber(QuineId(Array(99.toByte)), globalId, subscribeAcrossEdgeQuery.queryPartId)
    states(globalId -> canonicalPartId)._1.subscribers += latecomer
    states(globalId -> andThenQuery.queryPartId)._1.subscribers += latecomer
    held.success(())
    Await.result(persistence.deleted, 5.seconds)

    def subscribersWritten(partId: MultipleValuesStandingQueryPartId): Set[MultipleValuesStandingQuerySubscriber] = {
      val bytes = handedOver.collectFirst { case (`partId`, written) => written }.value
      MultipleValuesStandingQueryStateCodec.format.read(bytes).get._1.subscribers.toSet
    }

    // Both of them, because it is the second write that tells the difference: its state is only reached once the
    // first has landed, which is after the node has been running for as long as that write took.
    assert(
      subscribersWritten(canonicalPartId) == expectedSubscribers,
      "a fold wrote a subscriber set the node had changed after the fold decided what to write",
    )
    assert(
      subscribersWritten(andThenQuery.queryPartId) == expectedAndThenSubscribers,
      "a fold serialized a later state while the node was already changing it",
    )
    assert(!subscribersWritten(andThenQuery.queryPartId).contains(latecomer))
  }

  test("persisting a fold writes the touched blobs before deleting the rows they replace") {
    val operations = mutable.ArrayBuffer.empty[(MultipleValuesStandingQueryPartId, Boolean)]
    val persistor = new InMemoryPersistor() {
      override def setMultipleValuesStandingQueryState(
        standingQuery: StandingQueryId,
        id: QuineId,
        standingQueryPartId: MultipleValuesStandingQueryPartId,
        state: Option[Array[Byte]],
      ): Future[Unit] = {
        operations.synchronized(operations += (standingQueryPartId -> state.isDefined))
        super.setMultipleValuesStandingQueryState(standingQuery, id, standingQueryPartId, state)
      }
    }
    // Seed the store with the legacy rows, as a deployment's disk would hold them
    LegacyReciprocalFoldFixtures.load().foreach { case pair @ (subscription, _) =>
      Await.result(
        persistor.setMultipleValuesStandingQueryState(
          subscription.globalId,
          LegacyReciprocalFoldFixtures.farNode,
          subscription.forQuery,
          Some(com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec.format.write(pair)),
        ),
        5.seconds,
      )
    }
    operations.clear()

    val states = loadStates()
    val fold = foldLegacyReciprocals(states, lookupPart)
    Await.result(
      persistLegacyReciprocalFold(fold, states, persistor, LegacyReciprocalFoldFixtures.farNode).deleted,
      5.seconds,
    )

    withClue("every write precedes every delete") {
      val firstDelete = operations.indexWhere(!_._2)
      val lastWrite = operations.lastIndexWhere(_._2)
      assert(firstDelete > lastWrite)
    }

    withClue("and the store afterwards holds exactly the folded states") {
      val remaining = Await
        .result(persistor.getMultipleValuesStandingQueryStates(LegacyReciprocalFoldFixtures.farNode), 5.seconds)
        .keySet
      assert(remaining == Set(globalId -> canonicalPartId, globalId -> andThenQuery.queryPartId))
    }
  }
}

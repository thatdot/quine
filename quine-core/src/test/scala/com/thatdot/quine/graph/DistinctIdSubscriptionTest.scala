package com.thatdot.quine.graph

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription

/** The accessors on [[DistinctIdSubscription]], which holds who is subscribed to one pattern rooted on a node,
  * which top-level queries each of them depends on that node for, and what they were last told.
  *
  * `subscribers` and `relatedQueries` are views over `queriesPerSubscriber` rather than fields of their own, so
  * what is pinned here is that they agree with it after every mutation. Two of these differ from the behaviour
  * of the separate fields they replaced, and are called out where they are asserted.
  */
class DistinctIdSubscriptionTest extends AnyFunSuite with Matchers {

  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  private val peerA: Notifiable = Left(idProvider.customIdToQid(1L))
  private val peerB: Notifiable = Left(idProvider.customIdToQid(2L))

  private val q1: StandingQueryId = StandingQueryId.fresh()
  private val q2: StandingQueryId = StandingQueryId.fresh()
  private val q3: StandingQueryId = StandingQueryId.fresh()

  private val queryA: Notifiable = Right(q1)

  test("a subscription with nobody subscribed has no subscribers and no related queries") {
    val unsubscribed = DistinctIdSubscription()
    unsubscribed.isEmpty shouldBe true
    unsubscribed.nonEmpty shouldBe false
    unsubscribed.subscribers shouldBe Set.empty[Notifiable]
    unsubscribed.relatedQueries shouldBe Set.empty[StandingQueryId]
    unsubscribed.latestAnswer shouldBe None
  }

  test("subscribers is exactly the key set of queriesPerSubscriber") {
    val subscription = DistinctIdSubscription(
      latestAnswer = Some(true),
      queriesPerSubscriber = Map(peerA -> Set(q1), queryA -> Set(q1, q2)),
    )
    subscription.subscribers shouldBe Set(peerA, queryA)
    subscription.subscribers shouldBe subscription.queriesPerSubscriber.keySet
  }

  test("relatedQueries is the union over every subscriber") {
    val subscription =
      DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1), peerB -> Set(q2, q3), queryA -> Set.empty))
    subscription.relatedQueries shouldBe Set(q1, q2, q3)
  }

  test("queriesFor returns what a subscriber named, and nothing for one that is not subscribed") {
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1, q2)))
    subscription.queriesFor(peerA) shouldBe Set(q1, q2)
    subscription.queriesFor(peerB) shouldBe Set.empty[StandingQueryId]
  }

  test("hasSubscriber is true for a subscriber that named nothing") {
    // `add` never writes one, but a subscription decoded from a snapshot written before the per-subscriber map
    // existed can carry one, and it is a subscriber: it is in the key set, so it is told answers.
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set.empty))
    subscription.hasSubscriber(peerA) shouldBe true
    subscription.subscribers shouldBe Set(peerA)
    subscription.hasSubscriber(peerB) shouldBe false
  }

  test("isSubscribedFor requires both that the subscriber is present and that it named the query") {
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1)))
    subscription.isSubscribedFor(peerA, q1) shouldBe true
    subscription.isSubscribedFor(peerA, q2) shouldBe false
    subscription.isSubscribedFor(peerB, q1) shouldBe false
  }

  test("isOnlySubscriber distinguishes the last subscriber from one of several") {
    val alone = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1)))
    alone.isOnlySubscriber(peerA) shouldBe true

    val shared = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1), peerB -> Set(q2)))
    shared.isOnlySubscriber(peerA) shouldBe false
    // Not subscribed at all is not "the only subscriber", or removing an unknown would retire the subscription.
    alone.isOnlySubscriber(peerB) shouldBe false
    DistinctIdSubscription().isOnlySubscriber(peerA) shouldBe false
  }

  test("addSubscriber registers a subscriber that was not there") {
    val subscription = DistinctIdSubscription().addSubscriber(peerA, Set(q1))
    subscription.subscribers shouldBe Set(peerA)
    subscription.queriesFor(peerA) shouldBe Set(q1)
    subscription.relatedQueries shouldBe Set(q1)
  }

  test("addSubscriber unions onto what a subscriber already named rather than replacing it") {
    val subscription = DistinctIdSubscription()
      .addSubscriber(peerA, Set(q1))
      .addSubscriber(peerA, Set(q2))
    subscription.subscribers shouldBe Set(peerA)
    subscription.queriesFor(peerA) shouldBe Set(q1, q2)
  }

  test("addSubscriber leaves the latest answer alone") {
    val subscription = DistinctIdSubscription(latestAnswer = Some(true))
      .addSubscriber(peerA, Set(q1))
    subscription.latestAnswer shouldBe Some(true)
  }

  test("removeSubscriber drops the subscriber from every view") {
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1), peerB -> Set(q2)))
      .removeSubscriber(peerA)
    subscription.subscribers shouldBe Set(peerB)
    subscription.hasSubscriber(peerA) shouldBe false
    subscription.queriesFor(peerA) shouldBe Set.empty[StandingQueryId]
  }

  test("removing a subscriber that is not there changes nothing") {
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1)))
    subscription.removeSubscriber(peerB) shouldBe subscription
  }

  test("relatedQueries shrinks when the subscriber that named a query is removed") {
    // Differs from the separate `relatedQueries` field this replaced, which only ever grew: a departed
    // subscriber's query id stayed in the union, and outbound subscriptions went on naming a dead query.
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1), peerB -> Set(q2)))
    subscription.relatedQueries shouldBe Set(q1, q2)
    subscription.removeSubscriber(peerA).relatedQueries shouldBe Set(q2)
  }

  test("a query named by two subscribers survives the removal of one of them") {
    val subscription = DistinctIdSubscription(queriesPerSubscriber = Map(peerA -> Set(q1), peerB -> Set(q1, q2)))
    subscription.removeSubscriber(peerA).relatedQueries shouldBe Set(q1, q2)
  }

  test("removing the last subscriber leaves an empty subscription, not a stale one") {
    val subscription = DistinctIdSubscription(
      latestAnswer = Some(true),
      queriesPerSubscriber = Map(peerA -> Set(q1)),
    ).removeSubscriber(peerA)
    subscription.isEmpty shouldBe true
    subscription.relatedQueries shouldBe Set.empty[StandingQueryId]
    // The answer is not cleared by losing a subscriber; only `forgetAnswer` does that.
    subscription.latestAnswer shouldBe Some(true)
  }

  test("the infix forms agree with the methods they stand for") {
    val base = DistinctIdSubscription(queriesPerSubscriber = Map(peerB -> Set(q3)))
    (base + (peerA -> Set(q1))) shouldBe base.addSubscriber(peerA, Set(q1))
    (base - peerB) shouldBe base.removeSubscriber(peerB)
  }

  test("subscribers and queriesPerSubscriber agree after a sequence of mutations") {
    val subscription = DistinctIdSubscription()
      .addSubscriber(peerA, Set(q1))
      .addSubscriber(peerB, Set(q2))
      .addSubscriber(queryA, Set(q1, q3))
      .removeSubscriber(peerB)
      .addSubscriber(peerA, Set(q3))

    subscription.subscribers shouldBe subscription.queriesPerSubscriber.keySet
    subscription.subscribers shouldBe Set(peerA, queryA)
    subscription.queriesFor(peerA) shouldBe Set(q1, q3)
    subscription.relatedQueries shouldBe Set(q1, q3)
  }
}

package com.thatdot.quine.graph.cypher

import scala.concurrent.Future
import scala.util.Try

import com.thatdot.common.quineid.QuineId

/** Keeps an [[EdgeSubscriptionReciprocalState]]'s subscribers somewhere other than the node's subscriber set, for a
  * node with too many of them to hold in the heap or write into one blob.
  *
  * A subscriber is recorded by node alone. That is enough because every subscriber this state answers subscribes for
  * the same query part, which the state records once in
  * [[EdgeSubscriptionReciprocalState.externalSubscriberForQuery]], and a subscriber citing any other part is kept
  * in the node's subscriber set instead, never here.
  *
  * Recording a subscriber is a level: recording one already present changes nothing, which is why [[add]] is a blind
  * write with no answer. Every question, by contrast, is answered after a round trip to wherever the rows live, so
  * each is delivered to a callback on the node's thread rather than returned, and the implementation is expected to
  * pause the node's message processing for the trip, so that nothing else runs between a question and what the node
  * does with its answer.
  */
abstract class ReciprocalSubscriberStore {

  /** Record a subscriber. A blind write: no read, no answer, and recording one already recorded changes nothing. */
  def add(subscriber: QuineId): Unit

  /** Forget a subscriber, then answer whether no subscriber at all remains recorded here.
    *
    * The two are one operation because the answer is what the removal is for: the node discards the whole state
    * when its last subscriber goes, and must not decide that against rows the removal has not reached yet.
    */
  def remove(subscriber: QuineId)(andThen: Try[Boolean] => Unit): Unit

  /** Answer whether no subscriber at all is recorded here, for the same discard decision as [[remove]] when the
    * subscriber that just went was one the node held itself.
    */
  def isEmpty(andThen: Try[Boolean] => Unit): Unit

  /** Run `andThen` only if `subscriber` is recorded here: a point membership question, for relaying the held result
    * across an edge that just appeared (or retracting it across one that just went) without addressing every node
    * that merely has such an edge.
    */
  def ifSubscribed(subscriber: QuineId)(andThen: () => Unit): Unit

  /** Send to every recorded subscriber the node still has a matching edge to, for a result that changed for all of
    * them at once.
    *
    * `send` is invoked from the store's own stream over its rows, off the node's thread. That is the one place an
    * effect escapes the node's thread, made safe by the pause: nothing else the node might do can interleave with the
    * sends, so each subscriber still sees this state's levels in the order they happened.
    */
  def reportToEntitledSubscribers(send: QuineId => Unit): Unit

  /** Forget every subscriber recorded here, because the state that would answer them is going away. */
  def dropAll(): Future[Unit]
}

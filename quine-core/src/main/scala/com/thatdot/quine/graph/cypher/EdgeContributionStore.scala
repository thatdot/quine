package com.thatdot.quine.graph.cypher

import scala.collection.mutable
import scala.concurrent.Future

import com.thatdot.quine.model.HalfEdge

/** What each edge has contributed to a [[SubscribeAcrossEdgeState]]'s result group, and the running total of those
  * contributions.
  *
  * A contribution is a *level*: the whole group the far side of that edge currently produces, rather than a change to
  * it. Three properties follow, and they are what let these rows live somewhere other than the heap:
  *
  *   1. Applying the same contribution twice is a no-op.
  *   2. Contributions from different edges commute: the order they arrive in does not change the total.
  *   3. The total is derived. It can be rebuilt by reading the rows back, and is never the record of anything.
  *
  * Operations hand their outcome to a callback rather than returning it, because an implementation that keeps rows
  * outside the heap answers after a round trip to storage. The callback always runs on the node's thread, but not
  * necessarily before the call returns.
  */
abstract class EdgeContributionStore {

  /** Begin tracking an edge whose contribution is not yet known.
    *
    * Never clears what an edge already contributed. An edge can be restated, since events in one batch are not
    * deduplicated against each other and registration describes the node's current state, and a restatement is not
    * news that the edge stopped answering.
    */
  def track(halfEdge: HalfEdge): Unit

  /** Whether this query part has any matching edge at all, answered or not.
    *
    * This is the difference between "no edges match, so there are affirmatively no results" and "edges match but none
    * has answered, so there is no result yet": two answers that are not the same, and only one of which the rows
    * can tell you about.
    *
    * Deliberately not "any edge this store was told to track": an implementation that records nothing for an
    * unanswered edge has nothing of the kind to consult, and answers from the node's edges instead. The two agree
    * wherever the state has been told about every matching edge, which is what registration is for, so the
    * difference is in how the question is answered, not in what is being asked.
    */
  def hasTrackedEdges: Boolean

  /** How many tracked edges have contributed a level. */
  def answeredEdges: Int

  /** The running total of every edge's contribution: how many times each row is currently produced.
    *
    * Derived from the rows, so a caller may read it but never correct it.
    */
  def total: collection.Map[QueryContext, Int]

  /** Record what one edge currently contributes, handing what that did to `andThen`. */
  def contribute(halfEdge: HalfEdge, level: Seq[QueryContext])(andThen: ContributionOutcome => Unit): Unit

  /** Stop tracking one edge and take back what it contributed, handing what that did to `andThen`. */
  def retract(halfEdge: HalfEdge)(andThen: ContributionOutcome => Unit): Unit

  /** Every tracked edge and what it contributed, for the codec that writes this state down. */
  def entries: Iterator[(HalfEdge, Option[Seq[QueryContext]])]

  /** Whether this store's rows live somewhere other than the state's own blob.
    *
    * The codec cannot tell an empty heap store from a store whose rows are elsewhere, since both have no entries
    * to write, and the difference matters to whoever reads the blob back: rows that exist elsewhere must be adopted,
    * and a node that cannot adopt them should say so rather than silently report nothing.
    */
  def keepsRowsElsewhere: Boolean = false

  /** Put back an entry read from storage, without reporting a change: nothing has changed, this is what was already
    * the case being loaded back into memory.
    */
  def restore(halfEdge: HalfEdge, level: Option[Seq[QueryContext]]): Unit

  /** Forget everything this query part has recorded on this node.
    *
    * For when the part itself is going away, or when what is under its key belongs to an incarnation that ended. A
    * store holding its rows in the heap has nothing to do here: the rows go when the state does. A store that put
    * them somewhere else has to say so, because nothing else will ever look at them again and nothing else knows
    * they are there.
    */
  def dropAll(): Future[Unit] = Future.unit
}

/** What a mutation did to the store.
  *
  * @param edgeChanged this edge's own contribution is not what it was, so an implementation that writes rows down
  *                    has something to write
  * @param totalChanged the result group this state reports is not what it was, so it owes its subscribers a report
  */
final case class ContributionOutcome(edgeChanged: Boolean, totalChanged: Boolean)

object EdgeContributionStore {

  /** How the running total changes when one edge's contribution goes from `previous` to `next`.
    *
    * The whole of the level arithmetic, kept as a function of its inputs so that the properties claimed above
    * (that reapplying a contribution changes nothing, and that edges commute) are statements about a value, not
    * about a sequence of messages.
    */
  def totalDelta(previous: Option[Seq[QueryContext]], next: Option[Seq[QueryContext]]): Map[QueryContext, Int] = {
    val delta = mutable.Map.empty[QueryContext, Int]
    previous.foreach(_.foreach(row => delta.updateWith(row)(count => Some(count.getOrElse(0) - 1))))
    next.foreach(_.foreach(row => delta.updateWith(row)(count => Some(count.getOrElse(0) + 1))))
    delta.filter(_._2 != 0).toMap
  }

  /** Expand a total back into the rows it stands for: a row produced along three edges is three rows. */
  def expand(total: collection.Map[QueryContext, Int]): Seq[QueryContext] =
    total.iterator.flatMap { case (row, count) => Iterator.fill(count)(row) }.toSeq
}

/** Keeps every edge's contribution in the heap, which is what a node with an ordinary number of edges wants: the rows
  * are small, they are already there, and answering costs nothing.
  */
final class HeapEdgeContributionStore extends EdgeContributionStore {

  private[this] val rows: mutable.Map[HalfEdge, Option[Seq[QueryContext]]] = mutable.Map.empty
  private[this] val runningTotal: mutable.Map[QueryContext, Int] = mutable.Map.empty
  private[this] var answered: Int = 0

  def track(halfEdge: HalfEdge): Unit = if (!rows.contains(halfEdge)) rows += (halfEdge -> None)

  def hasTrackedEdges: Boolean = rows.nonEmpty

  def answeredEdges: Int = answered

  def total: collection.Map[QueryContext, Int] = runningTotal

  def contribute(halfEdge: HalfEdge, level: Seq[QueryContext])(andThen: ContributionOutcome => Unit): Unit = {
    val previous = rows.getOrElse(halfEdge, None)
    rows += (halfEdge -> Some(level))
    if (previous.isEmpty) answered += 1
    andThen(
      ContributionOutcome(
        edgeChanged = !previous.contains(level),
        totalChanged = applyToTotal(EdgeContributionStore.totalDelta(previous, Some(level))),
      ),
    )
  }

  def retract(halfEdge: HalfEdge)(andThen: ContributionOutcome => Unit): Unit = {
    val wasTracked = rows.contains(halfEdge)
    val previous = rows.remove(halfEdge).flatten
    if (previous.isDefined) answered -= 1
    andThen(
      ContributionOutcome(
        edgeChanged = wasTracked,
        totalChanged = applyToTotal(EdgeContributionStore.totalDelta(previous, None)),
      ),
    )
  }

  def entries: Iterator[(HalfEdge, Option[Seq[QueryContext]])] = rows.iterator

  def restore(halfEdge: HalfEdge, level: Option[Seq[QueryContext]]): Unit = {
    rows += (halfEdge -> level)
    if (level.isDefined) {
      answered += 1
      val _ = applyToTotal(EdgeContributionStore.totalDelta(None, level))
    }
  }

  private[this] def applyToTotal(delta: Map[QueryContext, Int]): Boolean = {
    delta.foreach { case (row, change) =>
      val updated = runningTotal.getOrElse(row, 0) + change
      if (updated == 0) runningTotal -= row else runningTotal += (row -> updated)
    }
    delta.nonEmpty
  }
}

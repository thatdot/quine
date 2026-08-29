package com.thatdot.quine.graph

import java.util.UUID

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.{
  Expr,
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryState,
  QueryContext,
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec

/** A correlated scenario of persisted standing query states as they exist on the far side of a
  * `SubscribeAcrossEdge` today: one `EdgeSubscriptionReciprocalState` per subscribing root, each keyed by a part id
  * that hashes the root's id (via the reciprocal's half edge), plus the shared `andThen` state whose subscriber set
  * names each of those reciprocals.
  *
  * The scenario is captured as serialized bytes because the part ids in it can only be computed by the funnel in use
  * at capture time. When the reciprocal's identity stops depending on which node subscribed, nothing will be able to
  * produce these ids again (`reciprocalQueryTo(root).queryPartId` will compute the new, shared id) while the rows
  * already on disk in every deployment keep the old ones. Code that folds those rows into the shared state gets its
  * inputs from here; the ids it must recognize as legacy are read out of the captured bytes, never recomputed.
  *
  * Everything else about the scenario is deterministic and re-derivable at test time: the query ASTs below hash to
  * the same part ids before and after any change that leaves their own funnels alone, which is what lets a test
  * resolve `lookupQuery` against them for the states these bytes decode to.
  */
object LegacyReciprocalFoldFixtures {

  /** The node the states live on: the far side of the subscribed-across edge. */
  val farNode: QuineId = QuineId(Array(0xF.toByte))

  /** The roots that subscribed across their edge to [[farNode]], each of which left one reciprocal state. */
  val subscribingRoots: Seq[QuineId] = Seq(QuineId(Array(1.toByte)), QuineId(Array(2.toByte)))

  val globalId: StandingQueryId = StandingQueryId(new UUID(0L, 1L))

  val andThenAliasedAs: Symbol = Symbol("p")

  val andThenQuery: MultipleValuesStandingQuery.LocalProperty = MultipleValuesStandingQuery.LocalProperty(
    Symbol("p"),
    MultipleValuesStandingQuery.LocalProperty.Any,
    Some(andThenAliasedAs),
  )

  /** The root-side part whose state issued the subscriptions; a subscriber's `forQuery` names this. */
  val subscribeAcrossEdgeQuery: MultipleValuesStandingQuery.SubscribeAcrossEdge =
    MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = Some(Symbol("x")),
      edgeDirection = Some(EdgeDirection.Outgoing),
      andThen = andThenQuery,
    )

  /** [[farNode]]'s own half of the edge to `root`: the half edge a root's `EdgeAdded` was reflected into. */
  def halfEdgeTo(root: QuineId): HalfEdge = HalfEdge(Symbol("x"), EdgeDirection.Incoming, root)

  /** The reciprocal query as the root would synthesize it for `root`'s edge. At capture time its `queryPartId` was
    * the id the state was filed under; recomputing it after the funnel changes gives the shared id instead.
    */
  def reciprocalQueryTo(root: QuineId): MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(halfEdgeTo(root), andThenQuery.queryPartId)

  /** One of the captured rows carries an answered `andThen` result and the other does not: rows that fold together
    * may have been persisted at different moments, so their cached copies legitimately differ.
    */
  val answeredCachedResult: Seq[QueryContext] = Seq(QueryContext(Map(andThenAliasedAs -> Expr.Str("answered"))))

  /** The captured scenario, as decoded pairs. The generator that produced the resource no longer exists, because the
    * funnel it needed no longer does; the bytes are the record now (see the class doc).
    */
  def load(): Seq[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)] =
    MultipleValuesStandingQueryStateFixtures
      .load(MultipleValuesStandingQueryStateFixtures.foldResourcePath)
      .map(fixture => MultipleValuesStandingQueryStateCodec.format.read(fixture.bytes).get)
}

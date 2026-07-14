package com.thatdot.quine.webapp.v2api

import com.thatdot.quine.routes.{QueryLanguage, QuerySort, QuickQuery, UiNodePredicate, UiNodeQuickQuery}
import com.thatdot.quine.v2api.routes.{V2QuerySort, V2QuickQuery, V2UiNodePredicate, V2UiNodeQuickQuery}

/** Wire-boundary conversions between the V1 and V2 quick-query types, for the paths where
  * one API version's payload must serve the other's surface (V1 reads feeding V2-shaped
  * consumers, V2-shaped state saved through the V1 route).
  */
object QuickQueryConversions {

  def v1ToV2(v1: UiNodeQuickQuery): V2UiNodeQuickQuery =
    V2UiNodeQuickQuery(
      predicate = V2UiNodePredicate(v1.predicate.propertyKeys, v1.predicate.knownValues, v1.predicate.dbLabel),
      quickQuery = V2QuickQuery(
        name = v1.quickQuery.name,
        querySuffix = v1.quickQuery.querySuffix,
        sort = v1.quickQuery.sort match {
          case QuerySort.Node => V2QuerySort.Node
          case QuerySort.Text => V2QuerySort.Text
        },
        edgeLabel = v1.quickQuery.edgeLabel,
      ),
    )

  def v2ToV1(v2: V2UiNodeQuickQuery): UiNodeQuickQuery =
    UiNodeQuickQuery(
      predicate = UiNodePredicate(v2.predicate.propertyKeys, v2.predicate.knownValues, v2.predicate.dbLabel),
      quickQuery = QuickQuery(
        name = v2.quickQuery.name,
        querySuffix = v2.quickQuery.querySuffix,
        queryLanguage = QueryLanguage.Cypher,
        sort = v2.quickQuery.sort match {
          case V2QuerySort.Node => QuerySort.Node
          case V2QuerySort.Text => QuerySort.Text
        },
        edgeLabel = v2.quickQuery.edgeLabel,
      ),
    )
}

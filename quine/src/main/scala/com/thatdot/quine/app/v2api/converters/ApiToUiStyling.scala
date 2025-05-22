package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.v2api.definitions.{ApiUiStyling => Api}
import com.thatdot.quine.{routes => V1}

object ApiToUiStyling {

  def apply(sample: Api.SampleQuery): V1.SampleQuery =
    V1.SampleQuery(name = sample.name, query = sample.query)

  def apply(sort: Api.QuerySort): V1.QuerySort = sort match {
    case Api.QuerySort.Node => V1.QuerySort.Node
    case Api.QuerySort.Text => V1.QuerySort.Text
  }

  def apply(query: Api.QuickQuery): V1.QuickQuery =
    V1.QuickQuery(
      name = query.name,
      querySuffix = query.querySuffix,
      sort = apply(query.sort),
      edgeLabel = query.edgeLabel,
      queryLanguage = V1.QueryLanguage.Cypher,
    )

  def apply(predicate: Api.UiNodePredicate): V1.UiNodePredicate =
    V1.UiNodePredicate(
      propertyKeys = predicate.propertyKeys,
      knownValues = predicate.knownValues,
      dbLabel = predicate.dbLabel,
    )

  def apply(query: Api.UiNodeQuickQuery): V1.UiNodeQuickQuery =
    V1.UiNodeQuickQuery(predicate = apply(query.predicate), quickQuery = apply(query.quickQuery))

  def apply(label: Api.UiNodeLabel): V1.UiNodeLabel = label match {
    case Api.UiNodeLabel.Constant(value) => V1.UiNodeLabel.Constant(value)
    case Api.UiNodeLabel.Property(key, prefix) => V1.UiNodeLabel.Property(key, prefix)
  }

  def apply(query: Api.UiNodeAppearance): V1.UiNodeAppearance =
    V1.UiNodeAppearance(
      predicate = apply(query.predicate),
      size = query.size,
      icon = query.icon,
      color = query.color,
      label = query.label.map(apply),
    )

}

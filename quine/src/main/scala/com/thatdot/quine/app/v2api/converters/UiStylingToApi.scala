package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.v2api.definitions.{ApiUiStyling => Api}
import com.thatdot.quine.{routes => V1}

object UiStylingToApi {

  def apply(sample: V1.SampleQuery): Api.SampleQuery =
    Api.SampleQuery(name = sample.name, query = sample.query)

  def apply(sort: V1.QuerySort): Api.QuerySort = sort match {
    case V1.QuerySort.Node => Api.QuerySort.Node
    case V1.QuerySort.Text => Api.QuerySort.Text
  }

  def apply(query: V1.QuickQuery): Api.QuickQuery =
    Api.QuickQuery(
      name = query.name,
      querySuffix = query.querySuffix,
      sort = apply(query.sort),
      edgeLabel = query.edgeLabel,
    )

  def apply(predicate: V1.UiNodePredicate): Api.UiNodePredicate =
    Api.UiNodePredicate(
      propertyKeys = predicate.propertyKeys,
      knownValues = predicate.knownValues,
      dbLabel = predicate.dbLabel,
    )

  def apply(query: V1.UiNodeQuickQuery): Api.UiNodeQuickQuery =
    Api.UiNodeQuickQuery(predicate = apply(query.predicate), quickQuery = apply(query.quickQuery))

  def apply(label: V1.UiNodeLabel): Api.UiNodeLabel = label match {
    case V1.UiNodeLabel.Constant(value) => Api.UiNodeLabel.Constant(value)
    case V1.UiNodeLabel.Property(key, prefix) => Api.UiNodeLabel.Property(key, prefix)
  }

  def apply(query: V1.UiNodeAppearance): Api.UiNodeAppearance =
    Api.UiNodeAppearance(
      predicate = apply(query.predicate),
      size = query.size,
      icon = query.icon,
      color = query.color,
      label = query.label.map(apply),
    )

}

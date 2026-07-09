package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.v2api.routes.{V2QuerySort, V2UiNodePredicate, V2UiNodeQuickQuery}
import com.thatdot.quine.webapp.Styles

object QuickQueryManager {

  def apply(
    quickQueries: Signal[Vector[V2UiNodeQuickQuery]],
    onEdit: Int => Unit,
    onNew: () => Unit,
    onDelete: Int => Unit,
  ): HtmlElement = {
    val searchVar = Var("")

    val filtered: Signal[Vector[(V2UiNodeQuickQuery, Int)]] =
      quickQueries.combineWith(searchVar.signal).map { case (qqs, search) =>
        val needle = search.trim.toLowerCase
        val indexed = qqs.zipWithIndex
        if (needle.isEmpty) indexed
        else
          indexed.filter { case (qq, _) =>
            qq.quickQuery.name.toLowerCase.contains(needle) ||
              qq.quickQuery.querySuffix.toLowerCase.contains(needle) ||
              predicateSummary(qq.predicate).toLowerCase.contains(needle)
          }
      }

    div(
      div(
        cls := Styles.managerHeader,
        span("Quick Queries"),
        button("New", onClick --> (_ => onNew())),
      ),
      div(
        cls := Styles.managerSearch,
        htmlTag("i")(cls := "ion-ios-search"),
        input(
          typ := "text",
          placeholder := "Search quick queries…",
          controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
        ),
      ),
      child <-- filtered.map { items =>
        if (items.isEmpty)
          div(cls := Styles.managerListEmpty, "No quick queries match your search.")
        else
          div(
            cls := Styles.managerList,
            items.map { case (qq, idx) =>
              val sortIcon = qq.quickQuery.sort match {
                case V2QuerySort.Node => "⚭"
                case V2QuerySort.Text => "≡"
              }
              val predSummary = predicateSummary(qq.predicate)
              div(
                cls := Styles.managerListItem,
                onClick --> (_ => onEdit(idx)),
                div(
                  cls := Styles.managerListItemMain,
                  span(cls := Styles.managerListItemName, s"$sortIcon ${qq.quickQuery.name}"),
                  span(cls := Styles.managerListItemDetail, s"${qq.quickQuery.querySuffix} — $predSummary"),
                ),
                div(
                  cls := Styles.managerListItemActions,
                  htmlTag("i")(
                    cls := "ion-edit",
                    title := "Edit",
                    onClick --> { e =>
                      e.stopPropagation()
                      onEdit(idx)
                    },
                  ),
                  htmlTag("i")(
                    cls := "ion-ios-trash-outline",
                    title := "Delete",
                    onClick --> { e =>
                      e.stopPropagation()
                      onDelete(idx)
                    },
                  ),
                ),
              )
            },
          )
      },
      div(
        cls := Styles.managerFooter,
        child.text <-- quickQueries.map(qqs => s"${qqs.size} quick ${if (qqs.size == 1) "query" else "queries"}"),
      ),
    )
  }

  def predicateSummary(pred: V2UiNodePredicate): String =
    if (pred == V2UiNodePredicate.every) "All nodes"
    else {
      val parts = Seq(
        pred.dbLabel.map(l => s"label=$l"),
        if (pred.propertyKeys.nonEmpty) Some(s"keys=[${pred.propertyKeys.mkString(", ")}]") else None,
        if (pred.knownValues.nonEmpty)
          Some(pred.knownValues.map { case (k, v) => s"$k=${v.noSpaces}" }.mkString(", "))
        else None,
      ).flatten
      parts.mkString(", ")
    }
}

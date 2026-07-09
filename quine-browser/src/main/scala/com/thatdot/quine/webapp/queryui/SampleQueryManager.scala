package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

object SampleQueryManager {

  def apply(
    sampleQueries: Signal[Vector[SampleQuery]],
    onEdit: Int => Unit,
    onNew: () => Unit,
    onSelect: String => Unit,
    onDelete: Int => Unit,
  ): HtmlElement = {
    val searchVar = Var("")

    val filtered: Signal[Vector[(SampleQuery, Int)]] =
      sampleQueries.combineWith(searchVar.signal).map { case (sqs, search) =>
        val needle = search.trim.toLowerCase
        val indexed = sqs.zipWithIndex
        if (needle.isEmpty) indexed
        else
          indexed.filter { case (sq, _) =>
            sq.name.toLowerCase.contains(needle) || sq.query.toLowerCase.contains(needle)
          }
      }

    div(
      div(
        cls := Styles.managerHeader,
        span("Sample Queries"),
        button("New", onClick --> (_ => onNew())),
      ),
      div(
        cls := Styles.managerSearch,
        htmlTag("i")(cls := "ion-ios-search"),
        input(
          typ := "text",
          placeholder := "Search sample queries…",
          controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
        ),
      ),
      child <-- filtered.map { items =>
        if (items.isEmpty)
          div(cls := Styles.managerListEmpty, "No sample queries match your search.")
        else
          div(
            cls := Styles.managerList,
            items.map { case (sq, idx) =>
              div(
                cls := Styles.managerListItem,
                onClick --> (_ => onSelect(sq.query)),
                div(
                  cls := Styles.managerListItemMain,
                  span(cls := Styles.managerListItemName, sq.name),
                  span(cls := Styles.managerListItemDetail, sq.query),
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
        child.text <-- sampleQueries.map(sqs => s"${sqs.size} sample ${if (sqs.size == 1) "query" else "queries"}"),
      ),
    )
  }
}

package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.{UiNodeAppearance, UiNodePredicate}
import com.thatdot.quine.webapp.Styles

object NodeAppearanceManager {

  private lazy val codepointToName: Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    val sheets = dom.document.styleSheets
    for (i <- 0 until sheets.length) {
      val sheet = sheets(i).asInstanceOf[dom.CSSStyleSheet]
      val rules =
        try sheet.cssRules
        catch { case _: Throwable => null }
      if (rules != null) {
        for (j <- 0 until rules.length)
          rules(j) match {
            case rule: dom.CSSStyleRule =>
              val sel = rule.selectorText
              if (sel.startsWith(".ion-") && sel.endsWith("::before")) {
                val name = sel.drop(5).dropRight(8) // strip ".ion-" and "::before"
                val content = rule.style.getPropertyValue("content")
                if (content.length >= 3) {
                  val char = content.substring(1, content.length - 1) // strip quotes
                  builder += (char -> name)
                }
              }
            case _ =>
          }
      }
    }
    builder.result()
  }

  def iconName(icon: Option[String]): Option[String] =
    icon.flatMap(codepointToName.get)

  def apply(
    appearances: Signal[Vector[UiNodeAppearance]],
    onEdit: Int => Unit,
    onNew: () => Unit,
    onDelete: Int => Unit,
  ): HtmlElement = {
    val searchVar = Var("")

    val filtered: Signal[Vector[(UiNodeAppearance, Int)]] =
      appearances.combineWith(searchVar.signal).map { case (apps, search) =>
        val needle = search.trim.toLowerCase
        val indexed = apps.zipWithIndex
        if (needle.isEmpty) indexed
        else
          indexed.filter { case (app, _) =>
            predicateSummary(app.predicate).toLowerCase.contains(needle) ||
              detailSummary(app).toLowerCase.contains(needle) ||
              iconName(app.icon).exists(_.toLowerCase.contains(needle))
          }
      }

    div(
      div(
        cls := Styles.managerHeader,
        span("Node Appearances"),
        button("New", onClick --> (_ => onNew())),
      ),
      div(
        cls := Styles.managerSearch,
        htmlTag("i")(cls := "ion-ios-search"),
        input(
          typ := "text",
          placeholder := "Search appearances…",
          controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
        ),
      ),
      child <-- filtered.map { items =>
        if (items.isEmpty)
          div(cls := Styles.managerListEmpty, "No appearances match your search.")
        else
          div(
            cls := Styles.managerList,
            items.map { case (app, idx) =>
              div(
                cls := Styles.managerListItem,
                onClick --> (_ => onEdit(idx)),
                span(
                  fontFamily := "Ionicons",
                  fontSize := "1.4em",
                  color := app.color.getOrElse("#97c2fc"),
                  width := "28px",
                  textAlign := "center",
                  flexShrink := 0,
                  display := "inline-block",
                  app.icon.getOrElse[String](""),
                ),
                div(
                  cls := Styles.managerListItemMain,
                  span(cls := Styles.managerListItemName, predicateSummary(app.predicate)),
                  span(
                    cls := Styles.managerListItemDetail,
                    detailSummary(app) + iconName(app.icon).fold("")(n => s" ($n)"),
                  ),
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
        child.text <-- appearances.map(apps => s"${apps.size} ${if (apps.size == 1) "appearance" else "appearances"}"),
      ),
    )
  }

  def predicateSummary(pred: UiNodePredicate): String =
    if (pred == UiNodePredicate.every) "All nodes"
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

  def detailSummary(app: UiNodeAppearance): String = {
    val parts = Seq(
      app.size.map(s => s"size=$s"),
      app.color.map(c => s"color=$c"),
      app.label.map {
        case com.thatdot.quine.routes.UiNodeLabel.Constant(v) => s"""label="$v""""
        case com.thatdot.quine.routes.UiNodeLabel.Property(k, _) => s"label=.$k"
      },
    ).flatten
    if (parts.isEmpty) "" else parts.mkString(", ")
  }
}

package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.syntax._

import com.thatdot.quine.routes.UiNode
import com.thatdot.quine.v2api.routes.{V2QuerySort, V2QuickQuery, V2UiNodePredicate, V2UiNodeQuickQuery}
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.{ApiJsonPreview, PredicateBuilder}

sealed trait QuickQueryEditorMode
object QuickQueryEditorMode {
  final case class Creating(prefillNode: Option[UiNode[String]]) extends QuickQueryEditorMode
  final case class Editing(index: Int) extends QuickQueryEditorMode
}

object QuickQueryEditor {

  def apply(
    mode: QuickQueryEditorMode,
    initialValue: Option[V2UiNodeQuickQuery],
    currentNodes: Signal[Seq[UiNode[String]]],
    onSave: V2UiNodeQuickQuery => Unit,
    onDelete: Option[() => Unit],
    onCancel: () => Unit,
  ): HtmlElement = {

    val (initialName, initialSuffix, initialSort, initialEdgeLabel, initialPredicate) = initialValue match {
      case Some(V2UiNodeQuickQuery(pred, qq)) =>
        (qq.name, qq.querySuffix, qq.sort, qq.edgeLabel, pred)
      case None =>
        mode match {
          case QuickQueryEditorMode.Creating(Some(node)) =>
            (
              "",
              "",
              V2QuerySort.Node: V2QuerySort,
              Option.empty[String],
              V2UiNodePredicate(
                propertyKeys = Vector.empty,
                knownValues = Map.empty,
                dbLabel = if (node.label.nonEmpty) Some(node.label) else None,
              ),
            )
          case _ =>
            ("", "", V2QuerySort.Node: V2QuerySort, Option.empty[String], V2UiNodePredicate.every)
        }
    }

    val nameVar = Var(initialName)
    val querySuffixVar = Var(initialSuffix)
    val sortVar = Var(initialSort)
    val edgeLabelVar = Var(initialEdgeLabel.getOrElse(""))
    val predicateVar = Var(initialPredicate)
    val showMatchingNodes = Var(initialPredicate != V2UiNodePredicate.every)

    val canSave: Signal[Boolean] = nameVar.signal
      .combineWith(querySuffixVar.signal)
      .map { case (n, q) => n.trim.nonEmpty && q.trim.nonEmpty }

    val jsonPreview: Signal[Json] = nameVar.signal
      .combineWith(querySuffixVar.signal, sortVar.signal, edgeLabelVar.signal, predicateVar.signal)
      .map { case (name, suffix, sort, edgeLabel, pred) =>
        Json.obj(
          "predicate" -> Json.obj(
            "dbLabel" -> pred.dbLabel.fold(Json.Null)(Json.fromString),
            "propertyKeys" -> pred.propertyKeys.asJson,
            "knownValues" -> pred.knownValues.asJson,
          ),
          "quickQuery" -> Json.obj(
            "name" -> Json.fromString(name),
            "querySuffix" -> Json.fromString(suffix),
            "sort" -> Json.fromString(sort match {
              case V2QuerySort.Node => "NODE"
              case V2QuerySort.Text => "TEXT"
            }),
            "edgeLabel" -> (if (sort == V2QuerySort.Node && edgeLabel.trim.nonEmpty)
                              Json.fromString(edgeLabel.trim)
                            else Json.Null),
          ),
        )
      }

    div(
      cls := Styles.editorForm,
      p(
        fontSize := "0.88em",
        color := "#56618f",
        marginBottom := "4px",
        "Appears in the right-click menu of matching nodes. Saved to the server — ",
        b("shared with everyone"),
        ".",
      ),
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Name"),
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "e.g. Recent Logins",
          controlled(
            value <-- nameVar.signal,
            onInput.mapToValue --> nameVar.writer,
          ),
        ),
      ),
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Query"),
        span(fontSize := "0.78em", color := "#8a93b5", " — runs from the clicked node, bound as n"),
        div(
          display := "flex",
          alignItems := "center",
          gap := "6px",
          span(cls := Styles.editorAnchorChip, "(n) ▸"),
          input(
            cls := Styles.editorInput,
            typ := "text",
            flexGrow := 1,
            placeholder := "MATCH (n)--(m) RETURN DISTINCT m",
            fontFamily := "\"SF Mono\", ui-monospace, Menlo, Consolas, monospace",
            fontSize := "0.88em",
            controlled(
              value <-- querySuffixVar.signal,
              onInput.mapToValue --> querySuffixVar.writer,
            ),
          ),
        ),
      ),
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Results"),
        div(
          display := "flex",
          gap := "0",
          button(
            cls <-- sortVar.signal.map(s =>
              if (s == V2QuerySort.Node) Styles.editorToggleActive else Styles.editorToggle,
            ),
            borderRadius := "8px 0 0 8px",
            "⚭ Nodes on canvas",
            onClick --> (_ => sortVar.set(V2QuerySort.Node)),
          ),
          button(
            cls <-- sortVar.signal.map(s =>
              if (s == V2QuerySort.Text) Styles.editorToggleActive else Styles.editorToggle,
            ),
            borderRadius := "0 8px 8px 0",
            "≡ Text panel",
            onClick --> (_ => sortVar.set(V2QuerySort.Text)),
          ),
        ),
      ),
      child <-- sortVar.signal.map {
        case V2QuerySort.Node =>
          div(
            fontSize := "0.85em",
            color := "#56618f",
            display := "flex",
            alignItems := "center",
            gap := "8px",
            span("Dotted edge back to the source, labeled:"),
            input(
              cls := Styles.editorInput,
              typ := "text",
              width := "140px",
              placeholder := "(optional) e.g. login",
              fontSize := "0.88em",
              controlled(
                value <-- edgeLabelVar.signal,
                onInput.mapToValue --> edgeLabelVar.writer,
              ),
            ),
          )
        case V2QuerySort.Text => emptyNode
      },
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Appears on"),
        div(
          display := "flex",
          gap := "0",
          marginBottom := "8px",
          button(
            cls <-- showMatchingNodes.signal.map(show => if (!show) Styles.editorToggleActive else Styles.editorToggle),
            borderRadius := "8px 0 0 8px",
            "All nodes",
            onClick --> { _ =>
              showMatchingNodes.set(false)
              predicateVar.set(V2UiNodePredicate.every)
            },
          ),
          button(
            cls <-- showMatchingNodes.signal.map(show => if (show) Styles.editorToggleActive else Styles.editorToggle),
            borderRadius := "0 8px 8px 0",
            "Matching nodes",
            onClick --> (_ => showMatchingNodes.set(true)),
          ),
        ),
        child <-- showMatchingNodes.signal.map {
          case true => PredicateBuilder.v2(predicateVar, currentNodes)
          case false => emptyNode
        },
      ),
      ApiJsonPreview(jsonPreview),
      div(
        cls := Styles.editorActions,
        onDelete
          .map { deleteFn =>
            button("Delete", onClick --> (_ => deleteFn()))
          }
          .getOrElse(emptyNode),
        span(flexGrow := 1),
        button(
          "Cancel",
          background := "none",
          border := "none",
          color := "#56618f",
          fontWeight := "600",
          cursor := "pointer",
          onClick --> (_ => onCancel()),
        ),
        button(
          "Save quick query",
          disabled <-- canSave.map(!_),
          onClick --> { _ =>
            val edgeLabel =
              if (sortVar.now() == V2QuerySort.Node && edgeLabelVar.now().trim.nonEmpty)
                Some(edgeLabelVar.now().trim)
              else None
            val qq = V2QuickQuery(
              name = nameVar.now().trim,
              querySuffix = querySuffixVar.now().trim,
              sort = sortVar.now(),
              edgeLabel = edgeLabel,
            )
            onSave(V2UiNodeQuickQuery(predicateVar.now(), qq))
          },
        ),
      ),
    )
  }
}

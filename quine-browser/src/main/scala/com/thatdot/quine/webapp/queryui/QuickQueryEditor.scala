package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.syntax._

import com.thatdot.quine.routes.{QueryLanguage, QuerySort, QuickQuery, UiNode, UiNodePredicate, UiNodeQuickQuery}
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.streams.{EmbeddedEditorConfig, EmbeddedQueryEditor}
import com.thatdot.quine.webapp.components.{ApiJsonPreview, PredicateBuilder}

sealed trait QuickQueryEditorMode
object QuickQueryEditorMode {
  final case class Creating(prefillNode: Option[UiNode[String]]) extends QuickQueryEditorMode
  final case class Editing(index: Int) extends QuickQueryEditorMode
}

object QuickQueryEditor {

  /** Edits a quick query in the v1 shape, which carries the entry's `queryLanguage`. This
    * editor can only author Cypher (new entries are saved as Cypher), but a pre-existing
    * Gremlin quick query is still editable: its buffer opens as plain text (Cypher
    * highlighting/validation would be wrong for Gremlin) with a note saying so, and it
    * stays Gremlin when saved.
    */
  def apply(
    mode: QuickQueryEditorMode,
    initialValue: Option[UiNodeQuickQuery],
    currentNodes: Signal[Seq[UiNode[String]]],
    editorConfig: EmbeddedEditorConfig,
    onSave: UiNodeQuickQuery => Unit,
    onDelete: Option[() => Unit],
    onCancel: () => Unit,
  ): HtmlElement = {

    // Edits preserve the language the entry already has; entries created here are Cypher,
    // the only language this editor can author.
    val queryLanguage: QueryLanguage =
      initialValue.map(_.quickQuery.queryLanguage).getOrElse(QueryLanguage.Cypher)

    val (initialName, initialSuffix, initialSort, initialEdgeLabel, initialPredicate) = initialValue match {
      case Some(UiNodeQuickQuery(pred, qq)) =>
        (qq.name, qq.querySuffix, qq.sort, qq.edgeLabel, pred)
      case None =>
        mode match {
          case QuickQueryEditorMode.Creating(Some(node)) =>
            (
              "",
              "",
              QuerySort.Node: QuerySort,
              Option.empty[String],
              UiNodePredicate(
                propertyKeys = Vector.empty,
                knownValues = Map.empty,
                dbLabel = if (node.label.nonEmpty) Some(node.label) else None,
              ),
            )
          case _ =>
            ("", "", QuerySort.Node: QuerySort, Option.empty[String], UiNodePredicate.every)
        }
    }

    val nameVar = Var(initialName)
    val querySuffixVar = Var(initialSuffix)
    val sortVar = Var(initialSort)
    val edgeLabelVar = Var(initialEdgeLabel.getOrElse(""))
    val predicateVar = Var(initialPredicate)
    val showMatchingNodes = Var(initialPredicate != UiNodePredicate.every)

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
              case QuerySort.Node => "NODE"
              case QuerySort.Text => "TEXT"
            }),
            "edgeLabel" -> (if (sort == QuerySort.Node && edgeLabel.trim.nonEmpty)
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
        "Appears in the right-click menu of matching nodes. Saved to the server and ",
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
        span(fontSize := "0.78em", color := "#8a93b5", " runs from the clicked node, bound as n"),
        if (queryLanguage == QueryLanguage.Gremlin)
          div(
            fontSize := "0.85em",
            color := "#8a6d3b",
            backgroundColor := "#fdf6e3",
            border := "1px solid #f0e0b6",
            borderRadius := "6px",
            padding := "6px 10px",
            marginBottom := "6px",
            b("Gremlin quick query"),
            " is edited as plain text since syntax highlighting and validation are Cypher-only. It stays Gremlin when saved.",
          )
        else emptyNode,
        div(
          display := "flex",
          alignItems := "center",
          gap := "6px",
          span(cls := Styles.editorAnchorChip, "(n) ▸"),
          div(
            flexGrow := 1,
            EmbeddedQueryEditor(
              currentValue = querySuffixVar.signal,
              onUpdate = v => querySuffixVar.set(v),
              placeholderText = "MATCH (n)--(m) RETURN DISTINCT m",
              editorConfig = editorConfig,
              bufferLanguage =
                if (queryLanguage == QueryLanguage.Gremlin) EmbeddedQueryEditor.BufferLanguage.PlainText
                else EmbeddedQueryEditor.BufferLanguage.Cypher,
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
              if (s == QuerySort.Node) Styles.editorToggleActive else Styles.editorToggle,
            ),
            borderRadius := "8px 0 0 8px",
            "⚭ Nodes on canvas",
            onClick --> (_ => sortVar.set(QuerySort.Node)),
          ),
          button(
            cls <-- sortVar.signal.map(s =>
              if (s == QuerySort.Text) Styles.editorToggleActive else Styles.editorToggle,
            ),
            borderRadius := "0 8px 8px 0",
            "≡ Text panel",
            onClick --> (_ => sortVar.set(QuerySort.Text)),
          ),
        ),
      ),
      child <-- sortVar.signal.map {
        case QuerySort.Node =>
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
        case QuerySort.Text => emptyNode
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
              predicateVar.set(UiNodePredicate.every)
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
          case true => PredicateBuilder(predicateVar, currentNodes)
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
              if (sortVar.now() == QuerySort.Node && edgeLabelVar.now().trim.nonEmpty)
                Some(edgeLabelVar.now().trim)
              else None
            val qq = QuickQuery(
              name = nameVar.now().trim,
              querySuffix = querySuffixVar.now().trim,
              queryLanguage = queryLanguage,
              sort = sortVar.now(),
              edgeLabel = edgeLabel,
            )
            onSave(UiNodeQuickQuery(predicateVar.now(), qq))
          },
        ),
      ),
    )
  }
}

package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.ApiJsonPreview
import com.thatdot.quine.webapp.components.streams.{EmbeddedEditorConfig, EmbeddedQueryEditor}

sealed trait SampleQueryEditorMode
object SampleQueryEditorMode {
  case object Creating extends SampleQueryEditorMode
  final case class Editing(index: Int) extends SampleQueryEditorMode
}

object SampleQueryEditor {

  def apply(
    mode: SampleQueryEditorMode,
    initialValue: Option[SampleQuery],
    editorConfig: EmbeddedEditorConfig,
    onSave: SampleQuery => Unit,
    onDelete: Option[() => Unit],
    onCancel: () => Unit,
  ): HtmlElement = {

    val (initialName, initialQuery) = initialValue match {
      case Some(sq) => (sq.name, sq.query)
      case None => ("", "")
    }

    val nameVar = Var(initialName)
    val queryVar = Var(initialQuery)

    val canSave: Signal[Boolean] = nameVar.signal
      .combineWith(queryVar.signal)
      .map { case (n, q) => n.trim.nonEmpty && q.trim.nonEmpty }

    val jsonPreview: Signal[Json] = nameVar.signal
      .combineWith(queryVar.signal)
      .map { case (name, query) =>
        Json.obj(
          "name" -> Json.fromString(name),
          "query" -> Json.fromString(query),
        )
      }

    div(
      cls := Styles.editorForm,
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Name"),
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "e.g. Get recent nodes",
          controlled(
            value <-- nameVar.signal,
            onInput.mapToValue --> nameVar.writer,
          ),
        ),
      ),
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Query"),
        EmbeddedQueryEditor(
          currentValue = queryVar.signal,
          onUpdate = v => queryVar.set(v),
          placeholderText = "CALL recentNodes(10)",
          editorConfig = editorConfig,
        ),
      ),
      ApiJsonPreview(jsonPreview),
      div(
        cls := Styles.editorActions,
        button(
          "Save",
          disabled <-- canSave.map(!_),
          onClick --> { _ =>
            onSave(SampleQuery(nameVar.now().trim, queryVar.now().trim))
          },
        ),
        onDelete
          .map { deleteFn =>
            button("Delete", onClick --> (_ => deleteFn()))
          }
          .getOrElse(emptyNode),
        button("Cancel", onClick --> (_ => onCancel())),
      ),
    )
  }
}

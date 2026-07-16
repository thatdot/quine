package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.syntax._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.ApiJsonPreview
import com.thatdot.quine.webapp.components.streams.{EmbeddedEditorConfig, EmbeddedQueryEditor}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2StandingQueryInfo, V2SyntheticEdge, V2TapQuery}

sealed trait TapQueryEditorMode
object TapQueryEditorMode {
  case object Creating extends TapQueryEditorMode
  final case class Editing(index: Int) extends TapQueryEditorMode
}

object TapQueryEditor {

  private val RawSentinel = "__raw__"
  private val NoSqSentinel = "__no_sq__"

  /** The `nodeIdsFrom` value every synthetic edge is saved with. The endpoint IDs are always read
    * from the wiretap message (`NodeIdsSource.WiretapMessage`), so this is not surfaced in the UI.
    */
  private val WiretapMessageSource = "WIRETAP_MESSAGE"

  /** Edge directions offered in the dropdown, in display order. */
  private val DirectionOptions = Vector("OUT", "IN", "UNDIRECTED")

  private def directionLabel(direction: String): String = direction match {
    case "OUT" => "→  out"
    case "IN" => "←  in"
    case "UNDIRECTED" => "—  undirected"
    case other => other
  }

  private def normalizeDirection(direction: String): String =
    if (DirectionOptions.contains(direction)) direction else "OUT"

  /** One editable synthetic-edge row. Immutable — edits produce a new value in `edgeRowsVar`. The
    * `id` is a stable key so Laminar keeps each row's DOM (and focus) across edits.
    */
  final private case class EdgeRow(
    id: Int,
    fromNode: String,
    toNode: String,
    label: String,
    direction: String,
  )

  /** Turn the rows into wire edges, dropping any row missing a from/to node ID. */
  private def buildEdges(rows: Vector[EdgeRow]): Vector[V2SyntheticEdge] =
    rows.collect {
      case r if r.fromNode.trim.nonEmpty && r.toNode.trim.nonEmpty =>
        V2SyntheticEdge(
          fromNode = r.fromNode.trim,
          toNode = r.toNode.trim,
          label = r.label.trim,
          direction = r.direction,
          nodeIdsFrom = WiretapMessageSource,
        )
    }

  def apply(
    mode: TapQueryEditorMode,
    initialValue: Option[V2TapQuery],
    standingQueries: Signal[Seq[V2StandingQueryInfo]],
    editorConfig: EmbeddedEditorConfig,
    onSave: V2TapQuery => Unit,
    onDelete: Option[() => Unit],
    onCancel: () => Unit,
  ): HtmlElement = {

    val (initialName, initialDescription, initialSq, initialOutput, initialQuery, initialEdges) =
      initialValue match {
        case Some(m) =>
          (m.name, m.description.getOrElse(""), m.standingQueryName, m.outputName, m.query, m.syntheticEdges)
        case None => ("", "", "", Option.empty[String], "", Vector.empty[V2SyntheticEdge])
      }

    val nameVar = Var(initialName)
    val descriptionVar = Var(initialDescription)
    val sqNameVar = Var(initialSq)
    val outputNameVar: Var[Option[String]] = Var(initialOutput)
    val queryVar = Var(initialQuery)

    val initialRows: Vector[EdgeRow] = initialEdges.zipWithIndex.map { case (e, i) =>
      EdgeRow(i, e.fromNode, e.toNode, e.label, normalizeDirection(e.direction))
    }
    val edgeRowsVar = Var(initialRows)
    val nextIdVar = Var(initialRows.size)

    def updateRow(id: Int, f: EdgeRow => EdgeRow): Unit =
      edgeRowsVar.update(_.map(r => if (r.id == id) f(r) else r))

    def removeRow(id: Int): Unit =
      edgeRowsVar.update(_.filterNot(_.id == id))

    def addRow(): Unit = {
      val id = nextIdVar.now()
      nextIdVar.set(id + 1)
      edgeRowsVar.update(_ :+ EdgeRow(id, "", "", "", "OUT"))
    }

    val isEditing = mode.isInstanceOf[TapQueryEditorMode.Editing]

    val edgesSignal: Signal[Vector[V2SyntheticEdge]] = edgeRowsVar.signal.map(buildEdges)

    val canSave: Signal[Boolean] = nameVar.signal
      .combineWith(sqNameVar.signal, queryVar.signal)
      .map { case (n, sq, q) =>
        n.trim.nonEmpty && sq.trim.nonEmpty && q.trim.nonEmpty
      }

    val jsonPreview: Signal[Json] = nameVar.signal
      .combineWith(descriptionVar.signal, sqNameVar.signal, outputNameVar.signal, queryVar.signal, edgesSignal)
      .map { case (name, desc, sq, out, q, edges) =>
        Json.obj(
          "name" -> Json.fromString(name),
          "description" -> (if (desc.trim.nonEmpty) Json.fromString(desc) else Json.Null),
          "standingQueryName" -> Json.fromString(sq),
          "outputName" -> out.fold(Json.Null)(Json.fromString),
          "query" -> Json.fromString(q),
          "syntheticEdges" -> edges.map { e =>
            Json.obj(
              "fromNode" -> Json.fromString(e.fromNode),
              "toNode" -> Json.fromString(e.toNode),
              "label" -> Json.fromString(e.label),
              "direction" -> Json.fromString(e.direction),
              "nodeIdsFrom" -> Json.fromString(e.nodeIdsFrom),
            )
          }.asJson,
        )
      }

    def renderEdgeRow(id: Int, rowSignal: Signal[EdgeRow]): HtmlElement =
      div(
        cls := "synthetic-edge-row",
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "from node ID path",
          controlled(
            value <-- rowSignal.map(_.fromNode),
            onInput.mapToValue --> (v => updateRow(id, _.copy(fromNode = v))),
          ),
        ),
        select(
          cls := "form-select form-select-sm",
          controlled(
            value <-- rowSignal.map(_.direction),
            onChange.mapToValue --> (v => updateRow(id, _.copy(direction = v))),
          ),
          DirectionOptions.map(d => option(value := d, directionLabel(d))),
        ),
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "to node ID path",
          controlled(
            value <-- rowSignal.map(_.toNode),
            onInput.mapToValue --> (v => updateRow(id, _.copy(toNode = v))),
          ),
        ),
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "label",
          controlled(
            value <-- rowSignal.map(_.label),
            onInput.mapToValue --> (v => updateRow(id, _.copy(label = v))),
          ),
        ),
        button(
          typ := "button",
          cls := "synthetic-edge-remove",
          title := "Remove edge",
          "×",
          onClick --> (_ => removeRow(id)),
        ),
      )

    div(
      cls := Styles.editorForm,
      p(
        fontSize := "0.88em",
        color := "#56618f",
        marginBottom := "4px",
        "Tap a Standing Query output and run a Cypher query against each match. Saved to the server — ",
        b("shared with everyone"),
        ".",
      ),
      // Name
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Name"),
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "e.g. login-attempts",
          controlled(
            value <-- nameVar.signal,
            onInput.mapToValue --> nameVar.writer,
          ),
        ),
      ),
      // Description
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Description (optional)"),
        input(
          cls := Styles.editorInput,
          typ := "text",
          placeholder := "What this tap query does",
          controlled(
            value <-- descriptionVar.signal,
            onInput.mapToValue --> descriptionVar.writer,
          ),
        ),
      ),
      // Standing Query
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Standing Query"),
        select(
          cls := "form-select form-select-sm",
          onChange.mapToValue --> { v =>
            sqNameVar.set(if (v == NoSqSentinel) "" else v)
            // Reset output when SQ changes — the previously selected output may not exist on the new SQ.
            outputNameVar.set(None)
          },
          children <-- standingQueries.combineWith(sqNameVar.signal).distinct.map { case (sqs, sel) =>
            option(value := NoSqSentinel, selected := sel.isEmpty, "-- select --") ::
              sqs.map(sq => option(value := sq.name, selected := sel == sq.name, sq.name)).toList
          },
        ),
      ),
      // Tap point (outputs of selected SQ + Raw)
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Tap point"),
        child <-- sqNameVar.signal.combineWith(standingQueries).distinct.map { case (sqName, sqs) =>
          val outputs = sqs.find(_.name == sqName).map(_.outputs).getOrElse(Nil)
          select(
            cls := "form-select form-select-sm",
            onChange.mapToValue --> { v =>
              outputNameVar.set(if (v == RawSentinel) None else Some(v))
            },
            option(
              value := RawSentinel,
              selected <-- outputNameVar.signal.map(_.isEmpty),
              "Raw (before any output workflow)",
            ),
            outputs.map { o =>
              option(
                value := o.name,
                selected <-- outputNameVar.signal.map(_.contains(o.name)),
                s"Post-enrichment: ${o.name}",
              )
            },
          )
        },
      ),
      // Query
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Cypher query"),
        span(
          fontSize := "0.78em",
          color := "#8a93b5",
          " — match fields available as $field_name (or WIRETAP_MESSAGE for the whole object)",
        ),
        EmbeddedQueryEditor(
          currentValue = queryVar.signal,
          onUpdate = v => queryVar.set(v),
          placeholderText = "MATCH (n) WHERE n.id = $userId RETURN n",
          editorConfig = editorConfig,
        ),
      ),
      // Synthetic edges — one editable row per edge.
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Synthetic edges (optional)"),
        p(
          fontSize := "0.78em",
          color := "#8a93b5",
          margin := "0 0 4px",
          "Draw extra relationships in the graph that aren't in your data. For each match, the ",
          b("from"),
          " and ",
          b("to"),
          " node IDs are read from the wiretap message at the paths you enter, then joined by an " +
          "edge with the given label and direction.",
        ),
        div(
          cls := "synthetic-edge-rows",
          children <-- edgeRowsVar.signal.splitSeq(_.id)(rowSig => renderEdgeRow(rowSig.key, rowSig)),
        ),
        button(
          typ := "button",
          cls := "synthetic-edge-add",
          "+ Add edge",
          onClick --> (_ => addRow()),
        ),
        child <-- edgeRowsVar.signal.map { rows =>
          val hasIncomplete = rows.exists(r => r.fromNode.trim.nonEmpty != r.toNode.trim.nonEmpty)
          if (hasIncomplete)
            div(cls := "text-muted small mt-1", "Rows missing a from or to node ID are ignored.")
          else emptyNode
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
          if (isEditing) "Save tap query" else "Create tap query",
          disabled <-- canSave.map(!_),
          onClick --> { _ =>
            val descOpt = if (descriptionVar.now().trim.nonEmpty) Some(descriptionVar.now().trim) else None
            val edges = buildEdges(edgeRowsVar.now())
            val tapQuery = V2TapQuery(
              name = nameVar.now().trim,
              description = descOpt,
              standingQueryName = sqNameVar.now().trim,
              outputName = outputNameVar.now(),
              query = queryVar.now().trim,
              syntheticEdges = edges,
            )
            onSave(tapQuery)
          },
        ),
      ),
    )
  }

}

package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.syntax._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.ApiJsonPreview
import com.thatdot.quine.webapp.components.streams.{EmbeddedEditorConfig, EmbeddedQueryEditor}
import com.thatdot.quine.webapp.resultspanel.cards.TapCardQuery
import com.thatdot.quine.webapp.resultspanel.tapmodal.SqPipelineTree
import com.thatdot.quine.webapp.resultspanel.{TapCatalogEntry, TapOutput, TapPoint, TapTarget}
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2StandingQueryInfo, V2SyntheticEdge, V2TapQuery}

sealed trait TapQueryEditorMode
object TapQueryEditorMode {
  case object Creating extends TapQueryEditorMode

  /** Editing the definition that had `name` when edit mode was entered. Keyed by name, not
    * an index into the definitions list — the list keeps refetching underneath an open edit
    * (other sessions, reconcile), so a captured index can silently point at the wrong entry
    * by save/delete time.
    */
  final case class Editing(name: String) extends TapQueryEditorMode
}

/** The graph-feed editor (the UI name for a `V2TapQuery`): instead of naming a standing
  * query and a tap point in dropdowns, the source is picked on the same pipeline diagram the
  * Standing Query Inspection modal uses ([[SqPipelineTree]] in `Variant.PickPoint`) — click the
  * point to draw from, then write the Cypher that runs for every result arriving there.
  */
object TapQueryEditor {

  /** The `nodeIdsFrom` value every synthetic edge is saved with. The endpoint IDs are always read
    * from the projected result message (`NodeIdsSource.WiretapMessage`), so this is not surfaced
    * in the UI.
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

  /** The saved source as a pipeline point: no output means the raw match stream, an output
    * name means the stream after that output's enrichment — or after its transformation only,
    * when the `preEnrichment` flag is set.
    */
  private def initialTarget(value: Option[V2TapQuery]): Option[TapTarget] =
    value.map { m =>
      TapTarget(
        m.standingQueryName,
        m.outputName.fold[TapPoint](TapPoint.Raw) { out =>
          if (m.preEnrichment) TapPoint.PreEnrichment(out) else TapPoint.PostEnrichment(out)
        },
      )
    }

  /** The wire `(outputName, preEnrichment)` pair for a picked point. */
  private def wireSource(target: TapTarget): (Option[String], Boolean) = target.tapPoint match {
    case TapPoint.Raw => (None, false)
    case TapPoint.PostEnrichment(out) => (Some(out), false)
    case TapPoint.PreEnrichment(out) => (Some(out), true)
  }

  def apply(
    mode: TapQueryEditorMode,
    initialValue: Option[V2TapQuery],
    standingQueries: Signal[Seq[V2StandingQueryInfo]],
    editorConfig: EmbeddedEditorConfig,
    onSave: V2TapQuery => Unit,
    onDelete: Option[() => Unit],
    onCancel: () => Unit,
    // True while the host's save round-trip is in flight; disables the save button so a
    // double-click can't dispatch the whole-list save (and its onSaved side effects) twice.
    saving: Signal[Boolean] = Signal.fromValue(false),
  ): HtmlElement = {

    val (initialName, initialDescription, initialQuery, initialEdges) =
      initialValue match {
        case Some(m) => (m.name, m.description.getOrElse(""), m.query, m.syntheticEdges)
        case None => ("", "", "", Vector.empty[V2SyntheticEdge])
      }

    val nameVar = Var(initialName)
    val descriptionVar = Var(initialDescription)
    val queryVar = Var(initialQuery)
    val selectionVar: Var[Option[TapTarget]] = Var(initialTarget(initialValue))

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

    // The picker's catalog, mapped from the standing-query feed. Only outputs with a
    // transformation or an enrichment offer a point to draw from (an output with neither
    // delivers the raw match stream unchanged), but a previously saved selection stays
    // offered while editing, even if its output has since lost the step that defined it,
    // so opening an old definition doesn't silently blank its source.
    val savedSelection: Option[TapTarget] = initialTarget(initialValue)
    val catalog: Signal[Pot[Vector[TapCatalogEntry]]] = standingQueries.map { sqs =>
      Pot.Ready(
        sqs.toVector.map { sq =>
          val pinnedPost: Option[String] = savedSelection.collect {
            case TapTarget(sqName, TapPoint.PostEnrichment(out)) if sqName == sq.name => out
          }
          val pinnedPre: Option[String] = savedSelection.collect {
            case TapTarget(sqName, TapPoint.PreEnrichment(out)) if sqName == sq.name => out
          }
          TapCatalogEntry(
            sq.name,
            sq.outputs.map(o =>
              TapOutput(
                o.name,
                o.hasEnrichment || pinnedPost.contains(o.name),
                o.hasTransformation || pinnedPre.contains(o.name),
                o.enrichmentQuery,
                o.transformationType,
              ),
            ),
            sq.pattern.flatMap(_.query),
          )
        },
      )
    }

    // The query behind the picked point's data — the standing query's match pattern for Raw,
    // its output's enrichment query for Enriched — shown under the picker as a hint at the
    // data the "Run for every result" Cypher will receive (for a Transformed point the
    // label notes the transformation has reshaped it since; see TapCardQuery.labelFor).
    // `Left(label)` when the point is picked but its query text isn't in the catalog — a
    // pinned selection whose output has since lost its enrichment, or a non-Cypher pattern —
    // so the panel can say so instead of silently showing nothing.
    val selectedQuery: Signal[Option[Either[String, TapCardQuery]]] =
      catalog.combineWith(selectionVar.signal).map { case (pot, selOpt) =>
        for {
          sel <- selOpt
          sqs <- pot.toOption
          sq <- sqs.find(_.sqName == sel.sqName)
        } yield TapCardQuery.forPoint(sq, sel.tapPoint).toRight(TapCardQuery.labelFor(sel.tapPoint))
      }

    // The example query's parameter shape follows the picked point: a raw standing-query match
    // (and any output with neither step, which the picker folds into the raw stream) exposes
    // result fields under `$data`, while an enriched or transformed point exposes its fields as
    // top-level parameters. The Monaco handle has no live "set placeholder" API, so the editor
    // is remounted when the shape flips; `.distinct` keeps that to the rare point change (not
    // every keystroke), and the query text survives the remount via the `currentValue` binder.
    val exampleQuerySignal: Signal[String] =
      selectionVar.signal.map {
        case Some(TapTarget(_, TapPoint.PostEnrichment(_) | TapPoint.PreEnrichment(_))) =>
          "MATCH (n) WHERE id(n) = $id RETURN n"
        case _ => "MATCH (n) WHERE id(n) = $data.id RETURN n"
      }.distinct

    val edgesSignal: Signal[Vector[V2SyntheticEdge]] = edgeRowsVar.signal.map(buildEdges)

    // What the user still has to provide before the feed can be saved, in form order;
    // empty means saveable. Rendered next to the save button so the disabled state
    // explains itself.
    val missingFields: Signal[Vector[String]] = nameVar.signal
      .combineWith(selectionVar.signal, queryVar.signal)
      .map { case (n, sel, q) =>
        Vector(
          Option.when(n.trim.isEmpty)("a name"),
          Option.when(sel.isEmpty)("a point to draw from"),
          Option.when(q.trim.isEmpty)("the query to run"),
        ).flatten
      }

    val canSave: Signal[Boolean] = missingFields.map(_.isEmpty)

    val jsonPreview: Signal[Json] = nameVar.signal
      .combineWith(
        descriptionVar.signal,
        selectionVar.signal,
        queryVar.signal,
        edgesSignal,
      )
      .map { case (name, desc, sel, q, edges) =>
        val (outName, pre) = sel.map(wireSource).getOrElse((None, false))
        Json.obj(
          "name" -> Json.fromString(name),
          "description" -> (if (desc.trim.nonEmpty) Json.fromString(desc) else Json.Null),
          "standingQueryName" -> sel.fold(Json.Null)(t => Json.fromString(t.sqName)),
          "outputName" -> outName.fold(Json.Null)(Json.fromString),
          "preEnrichment" -> Json.fromBoolean(pre),
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
        "A graph feed watches a point in a standing query's pipeline and draws every matching result " +
        "onto the graph, live. Saved to the server, ",
        b("shared with everyone"),
        ".",
      ),
      // Name + description, side by side: the name is the feed's identity everywhere
      // (delete, enable, reconcile), the description is free text for the list.
      div(
        cls := Styles.editorField,
        div(
          display := "flex",
          gap := "10px",
          div(
            flex := "0 0 240px",
            display := "flex",
            flexDirection := "column",
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
          div(
            flex := "1 1 auto",
            display := "flex",
            flexDirection := "column",
            span(cls := Styles.editorFieldLabel, "Description (optional)"),
            input(
              cls := Styles.editorInput,
              typ := "text",
              placeholder := "What this feed shows",
              controlled(
                value <-- descriptionVar.signal,
                onInput.mapToValue --> descriptionVar.writer,
              ),
            ),
          ),
        ),
      ),
      // Where to draw from: the same pipeline diagram the Standing Query Inspection modal
      // uses, in single-selection mode.
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Draw from"),
        div(
          cls := "graph-feed-picker",
          SqPipelineTree(
            catalog = catalog,
            tappedKeys = selectionVar.signal.map(_.map(_.key).toSet),
            onPickNew = target => selectionVar.set(Some(target)),
            // Re-clicking the selected point keeps it selected — there is nothing to focus here.
            onFocusExisting = target => selectionVar.set(Some(target)),
            variant = SqPipelineTree.Variant.PickPoint,
            initialSqName = savedSelection.map(_.sqName),
          ),
        ),
        div(
          cls := "graph-feed-picker-status",
          child <-- selectionVar.signal.map {
            case Some(target) => span("Drawing from ", b(target.label))
            case None => span("No point selected yet. Click one above.")
          },
        ),
        // The query that produced the picked point's data, so the user can see what shape
        // of data (and which fields) their own Cypher below will run against.
        child <-- selectedQuery.map {
          case Some(Right(q)) =>
            div(
              cls := "graph-feed-picker-query",
              span(cls := "graph-feed-picker-query-label", q.label),
              q.note.map(n => span(cls := "graph-feed-picker-query-note", n)).getOrElse(emptyNode: Node),
              pre(cls := "graph-feed-picker-query-pre", q.query),
            )
          case Some(Left(label)) =>
            div(
              cls := "graph-feed-picker-query",
              span(cls := "graph-feed-picker-query-label", label),
              span(
                cls := "graph-feed-picker-query-missing",
                "No query text available for this point. The source may have changed since this definition was saved.",
              ),
            )
          case None => emptyNode
        },
      ),
      // What to run there
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Run for every result"),
        span(
          fontSize := "0.78em",
          color := "#8a93b5",
          "This Cypher query must ",
          b("return nodes"),
          " which are drawn onto the graph. Returning scalars, relationships, or other values instead " +
          "produces errors. Each result is passed in as " +
          "parameters, and where the fields live depends on the point you picked. For matches taken " +
          "directly on the standing query, and for outputs with neither transformation nor enrichment, " +
          "the returned columns are the fields of ",
          b("$data"),
          " (e.g. ",
          b("$data.id"),
          "), alongside ",
          b("$meta"),
          " match metadata. For enriched outputs, the enrichment query's returned columns are top-level " +
          "parameters instead (e.g. ",
          b("$id"),
          "). For a transformed point, the transformation's returned object fields are top-level " +
          "parameters the same way; a transformation returning something other than an object " +
          "provides no parameters.",
        ),
        child <-- exampleQuerySignal.map { example =>
          EmbeddedQueryEditor(
            currentValue = queryVar.signal,
            onUpdate = v => queryVar.set(v),
            placeholderText = example,
            editorConfig = editorConfig,
          )
        },
      ),
      // Synthetic edges — one editable row per edge.
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Synthetic edges (optional)"),
        p(
          fontSize := "0.78em",
          color := "#8a93b5",
          margin := "0 0 4px",
          "Draw extra relationships in the graph that aren't in your data. For each result, the ",
          b("from"),
          " and ",
          b("to"),
          " node IDs are read from the result message at the paths you enter, then joined by an " +
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
            button("Delete", cls := "editor-danger-action", onClick --> (_ => deleteFn()))
          }
          .getOrElse(emptyNode),
        // While the save button is disabled, say what's still needed instead of leaving
        // a dead-looking button unexplained.
        span(
          flexGrow := 1,
          cls := "graph-feed-missing-hint",
          child <-- missingFields.map { missing =>
            if (missing.isEmpty) emptyNode
            else span(s"Still needed: ${missing.mkString(", ")}")
          },
        ),
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
          if (isEditing) "Save feed" else "Create feed",
          cls := "editor-primary-action",
          disabled <-- canSave.combineWith(saving).map { case (ok, inFlight) => !ok || inFlight },
          onClick --> { _ =>
            selectionVar.now().foreach { target =>
              val descOpt = if (descriptionVar.now().trim.nonEmpty) Some(descriptionVar.now().trim) else None
              val edges = buildEdges(edgeRowsVar.now())
              val (outName, pre) = wireSource(target)
              val tapQuery = V2TapQuery(
                name = nameVar.now().trim,
                description = descOpt,
                standingQueryName = target.sqName,
                outputName = outName,
                preEnrichment = pre,
                query = queryVar.now().trim,
                syntheticEdges = edges,
              )
              onSave(tapQuery)
            }
          },
        ),
      ),
    )
  }

}

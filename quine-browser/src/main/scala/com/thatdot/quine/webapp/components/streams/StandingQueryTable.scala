package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.openapi._

/** Renders the standing queries table with expandable output rows.
  *
  * Pure renderer: receives Signals to read, Observers to write. No API
  * knowledge — the parent wires observers to API calls.
  *
  * Takes a `Signal[List[(name, json)]]` and uses [[splitSeq]] keyed by name so that
  * the DOM for each SQ row (including any open inline output form inside its expanded
  * row) is kept stable across the 5-second polling update. Cells inside a row re-read
  * from the row's signal to stay in sync with the latest stats.
  */
object StandingQueryTable {

  def apply(
    entriesSignal: Signal[List[(String, Json)]],
    onDeleteSq: Observer[String],
    onRemoveOutput: Observer[(String, String)],
    expandedVar: Var[Set[String]],
    addingOutputFor: Var[Option[String]],
    outputFormState: OutputForm.State,
    outputSchema: Option[SchemaNode],
    spec: ParsedSpec,
    onAddOutput: (String, Json) => Future[Either[String, Json]],
  ): HtmlElement =
    table(
      cls := "table table-hover mb-0",
      thead(
        tr(
          th(styleAttr := "width: 40px"),
          th("Name"),
          th("Pattern"),
          th("Mode"),
          th("Outputs"),
          th("Rate (1m)"),
          th("Actions"),
        ),
      ),
      children <-- entriesSignal.splitSeq(_._1) { strictSignal =>
        val name = strictSignal.key
        val jsonSignal = strictSignal.map(_._2)
        val isExpanded = expandedVar.signal.map(_.contains(name)).distinct
        tbody(
          renderMainRow(name, jsonSignal, isExpanded, expandedVar, onDeleteSq),
          renderExpandedRow(
            name,
            jsonSignal,
            isExpanded,
            spec,
            onRemoveOutput,
            addingOutputFor,
            outputFormState,
            outputSchema,
            onAddOutput,
          ),
        )
      },
    )

  private def renderMainRow(
    name: String,
    jsonSignal: Signal[Json],
    isExpanded: Signal[Boolean],
    expandedVar: Var[Set[String]],
    onDeleteSq: Observer[String],
  ): HtmlElement = {
    val patternSignal: Signal[String] = jsonSignal.map { json =>
      val cursor = json.hcursor
      cursor
        .downField("pattern")
        .downField("query")
        .as[String]
        .toOption
        .getOrElse(cursor.downField("pattern").focus.map(_.noSpaces).getOrElse("-"))
    }.distinct
    val modeSignal: Signal[String] = jsonSignal
      .map(_.hcursor.downField("pattern").downField("mode").as[String].toOption.getOrElse("-"))
      .distinct
    val outputsCountSignal: Signal[Int] = jsonSignal.map { json =>
      json.hcursor
        .downField("outputs")
        .focus
        .flatMap(j => j.asObject.map(_.size).orElse(j.asArray.map(_.size)))
        .getOrElse(0)
    }.distinct
    val rateSignal: Signal[String] = jsonSignal.map { json =>
      json.hcursor
        .downField("stats")
        .focus
        .flatMap { statsJson =>
          statsJson.asObject
            .flatMap(_.values.headOption)
            .orElse(Some(statsJson))
            .flatMap(_.hcursor.downField("rates").get[Double]("oneMinute").toOption)
        }
        .map(r => f"$r%.1f/s")
        .getOrElse("-")
    }.distinct

    tr(
      td(
        button(
          cls := "btn btn-sm btn-ghost-secondary p-0",
          child <-- isExpanded.map { exp =>
            if (exp) i(cls := "cil-chevron-bottom") else i(cls := "cil-chevron-right")
          },
          onClick --> { _ =>
            expandedVar.update(exp => if (exp.contains(name)) exp - name else exp + name)
          },
        ),
      ),
      td(name),
      td(
        span(
          cls := "text-truncate d-inline-block",
          styleAttr := "max-width: 300px",
          title <-- patternSignal,
          child.text <-- patternSignal,
        ),
      ),
      td(span(cls := "badge bg-info", child.text <-- modeSignal)),
      td(span(cls := "badge bg-secondary", child.text <-- outputsCountSignal.map(_.toString))),
      td(child.text <-- rateSignal),
      td(
        button(
          cls := "btn btn-sm btn-ghost-danger",
          title := "Delete",
          i(cls := "cil-trash"),
          onClick --> { _ => onDeleteSq.onNext(name) },
        ),
      ),
    )
  }

  private def renderExpandedRow(
    name: String,
    jsonSignal: Signal[Json],
    isExpanded: Signal[Boolean],
    spec: ParsedSpec,
    onRemoveOutput: Observer[(String, String)],
    addingOutputFor: Var[Option[String]],
    outputFormState: OutputForm.State,
    outputSchema: Option[SchemaNode],
    onAddOutput: (String, Json) => Future[Either[String, Json]],
  ): HtmlElement = {
    val outputEntriesSignal: Signal[List[(String, Json)]] = jsonSignal.map { json =>
      json.hcursor
        .downField("outputs")
        .focus
        .flatMap(_.asArray)
        .getOrElse(Vector.empty)
        .toList
        .map { item =>
          val n = item.hcursor.get[String]("name").getOrElse("unknown")
          n -> item
        }
    }

    val isAddingHere: Signal[Boolean] = addingOutputFor.signal.map(_.contains(name)).distinct

    tr(
      cls := "bg-body-tertiary",
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan := 7,
        div(
          cls := "ms-4 py-2",
          div(
            cls := "d-flex justify-content-between align-items-center mb-2",
            strong("Outputs"),
            child <-- isAddingHere.map { adding =>
              if (adding) span(display := "none")
              else
                button(
                  cls := "btn btn-sm btn-outline-primary",
                  i(cls := "cil-plus me-1"),
                  "Add Output",
                  onClick --> { _ =>
                    outputFormState.reset()
                    addingOutputFor.set(Some(name))
                  },
                )
            },
          ),
          child <-- outputEntriesSignal.map { outputEntries =>
            if (outputEntries.isEmpty)
              div(cls := "text-body-secondary text-center py-2", "No outputs configured.")
            else
              table(
                cls := "table table-sm mb-0",
                thead(tr(th("Name"), th("Type"), th("Actions"))),
                tbody(
                  outputEntries.map { case (outputName, outputJson) =>
                    val outputType = describeOutputType(outputJson)
                    tr(
                      td(outputName),
                      td(code(outputType)),
                      td(
                        button(
                          cls := "btn btn-sm btn-ghost-danger",
                          title := "Remove output",
                          i(cls := "cil-trash"),
                          onClick --> { _ => onRemoveOutput.onNext((name, outputName)) },
                        ),
                      ),
                    )
                  },
                ),
              )
          },
          child <-- isAddingHere.map { adding =>
            if (!adding) emptyNode
            else
              OutputForm(
                sqName = name,
                spec = spec,
                outputSchema = outputSchema,
                state = outputFormState,
                onSubmit = body => onAddOutput(name, body),
                onComplete = { () =>
                  outputFormState.reset()
                  addingOutputFor.set(None)
                },
                onCancel = { () =>
                  outputFormState.reset()
                  addingOutputFor.set(None)
                },
              )
          },
        ),
      ),
    )
  }

  private def describeOutputType(outputJson: Json): String = {
    val destTypes = outputJson.hcursor
      .downField("destinations")
      .focus
      .flatMap(_.asArray)
      .getOrElse(Vector.empty)
      .toList
      .flatMap(_.hcursor.get[String]("type").toOption)
      .distinct
    destTypes match {
      case Nil => "(empty)"
      case single :: Nil => single
      case many => many.mkString(", ")
    }
  }
}

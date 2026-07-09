package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom.window

import com.thatdot.quine.openapi._
import com.thatdot.quine.webapp.queryui.{WiretapHandler, WiretapOwner, WiretapStatus, WiretapStore}

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
    namespace: String,
    wiretapStore: WiretapStore,
    editorConfig: EmbeddedEditorConfig,
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
      children <-- entriesSignal
        .splitSeq(_._1) { strictSignal =>
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
              namespace,
              wiretapStore,
              editorConfig,
            ),
          )
        }
        .distinct,
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
    val rateSignal: Signal[String] = jsonSignal
      .map(json => clusterRatePerSecond(json).fold("-")(r => f"$r%.1f/s"))
      .distinct

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
    namespace: String,
    wiretapStore: WiretapStore,
    editorConfig: EmbeddedEditorConfig,
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
    }.distinct

    val isAddingHere: Signal[Boolean] = addingOutputFor.signal.map(_.contains(name)).distinct
    val configExpanded = Var(false)

    tr(
      cls := "bg-body-tertiary",
      display <-- isExpanded.map(if (_) "table-row" else "none"),
      td(
        colSpan := 7,
        div(
          cls := "ms-4 py-2",
          div(
            cls := "mb-3",
            div(
              cls := "d-flex align-items-center cursor-pointer mb-1",
              styleAttr := "cursor: pointer",
              onClick --> { _ => configExpanded.update(!_) },
              i(
                cls <-- configExpanded.signal.map(e => if (e) "cil-chevron-bottom me-2" else "cil-chevron-right me-2"),
              ),
              strong("Configuration"),
            ),
            pre(
              display <-- configExpanded.signal.map(if (_) "block" else "none"),
              cls := "mb-0 mt-1 p-2 bg-body rounded border",
              styleAttr := "max-height: 24em; overflow: auto; font-size: 0.85em;",
              child.text <-- jsonSignal.map(configOnly(_).spaces2).distinct,
            ),
            div(
              display <-- configExpanded.signal.map(if (_) "block" else "none"),
              cls := "mt-2",
              strong("Live Stats"),
              pre(
                cls := "mb-0 mt-1 p-2 bg-body rounded border",
                styleAttr := "max-height: 24em; overflow: auto; font-size: 0.85em;",
                child.text <-- jsonSignal.map(liveOnly(_).spaces2),
              ),
            ),
          ),
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
                editorConfig = editorConfig,
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
          renderWiretapsSection(name, outputEntriesSignal.map(_.map(_._1)), jsonSignal, namespace, wiretapStore),
        ),
      ),
    )
  }

  // Cluster-wide one-minute match rate, summed across members. A standing query is
  // registered on every cluster member and each one reports its own stats keyed by
  // host; taking any one host's value would show arbitrary, load-balancer-routed
  // throughput rather than the SQ's true workload.
  private def clusterRatePerSecond(json: Json): Option[Double] = {
    val perHostRates = json.hcursor
      .downField("stats")
      .focus
      .map(statsJson => statsJson.asObject.map(_.values.toVector).getOrElse(Vector(statsJson)))
      .getOrElse(Vector.empty)
      .flatMap(_.hcursor.downField("rates").get[Double]("oneMinute").toOption)
    if (perHostRates.isEmpty) None else Some(perHostRates.sum)
  }

  // Threshold above which opening a wiretap forces a confirmation. A wiretap stream
  // aggregates matches across cluster members, so each match becomes cross-host
  // chatter; high-rate SQs can pin a sizable fraction of the cluster's coordination
  // capacity if the user isn't ready for it.
  private val HighRateWarningThreshold: Double = 100.0

  // Owner for wiretaps opened from the streams page. Lets the per-SQ section filter
  // the shared `WiretapStore.active` map to just the taps it owns; tap queries enabled
  // from Explorer Settings use a different owner, so handlers don't collide. The
  // per-handler `key` within this owner is the source key (one tap per source).
  private val StreamsWiretapOwner = WiretapOwner("streams")

  private def streamsHandlerKey(sqName: String, outputName: Option[String]): String =
    WiretapHandler.sourceKey(sqName, outputName)

  private val RawTapSentinel = "__raw_tap__"

  private def renderWiretapsSection(
    sqName: String,
    outputNamesSignal: Signal[List[String]],
    sqJsonSignal: Signal[Json],
    namespace: String,
    wiretapStore: WiretapStore,
  ): HtmlElement = {
    // Per-row picker state, persisted across polling rebuilds (the per-row signal stays
    // stable thanks to `splitSeq(_._1)` in the table body).
    val selectedOutputVar: Var[Option[String]] = Var(None)
    // Mirror the latest cluster-wide match rate into a Var the click handler can sample.
    // `Signal.now` is protected, so we materialize a writable copy bound to the source.
    val latestRateVar: Var[Double] = Var(0.0)

    // Live list of handlers this section owns, filtered by owner + SQ name.
    val sectionHandlersSignal: Signal[List[WiretapHandler]] =
      wiretapStore.active.map { active =>
        active.getOrElse((namespace, StreamsWiretapOwner), List.empty).filter(_.sqName == sqName)
      }

    div(
      cls := "mt-3",
      sqJsonSignal.map(clusterRatePerSecond(_).getOrElse(0.0)) --> latestRateVar.writer,
      div(
        cls := "d-flex justify-content-between align-items-center mb-2",
        strong("Wiretaps"),
      ),
      // Picker row: tap-point dropdown + Start button.
      div(
        cls := "d-flex align-items-center gap-2 mb-2",
        select(
          cls := "form-select form-select-sm",
          styleAttr := "max-width: 24em",
          value <-- selectedOutputVar.signal.map(_.getOrElse(RawTapSentinel)),
          onChange.mapToValue --> { v =>
            selectedOutputVar.set(if (v == RawTapSentinel) None else Some(v))
          },
          children <-- outputNamesSignal.distinct.map { outs =>
            option(value := RawTapSentinel, "Raw (before any output workflow)") ::
            outs.map(n => option(value := n, s"Post-enrichment: $n"))
          },
        ),
        button(
          cls := "btn btn-sm btn-primary",
          i(cls := "cil-media-play me-1"),
          "Start wiretap",
          onClick --> { _ =>
            val outputName = selectedOutputVar.now()
            val rate = latestRateVar.now()
            val proceed =
              if (rate >= HighRateWarningThreshold)
                window.confirm(
                  f"""Standing query "$sqName" is currently matching at $rate%.1f/s (1-minute rate).
                     |
                     |Opening a wiretap on a high-rate standing query streams every match to your browser and can noticeably slow down quine.
                     |
                     |Open the wiretap anyway?""".stripMargin,
                )
              else true
            if (proceed)
              wiretapStore.open(
                namespace,
                StreamsWiretapOwner,
                streamsHandlerKey(sqName, outputName),
                sqName,
                outputName,
              )
          },
        ),
      ),
      // Active wiretaps list — one card per handler with results.
      div(
        cls := "d-flex flex-column gap-2",
        children <-- sectionHandlersSignal.splitSeq(_.key) { strictSignal =>
          renderActiveWiretap(
            strictSignal.now(),
            key => wiretapStore.close(namespace, StreamsWiretapOwner, key),
          )
        },
      ),
    )
  }

  private def statusBadge(status: WiretapStatus): HtmlElement = status match {
    case WiretapStatus.Connecting => span(cls := "badge bg-warning text-dark", "Connecting")
    case WiretapStatus.Live => span(cls := "badge bg-success", "Live")
    case WiretapStatus.Error(msg) => span(cls := "badge bg-danger", title := msg, "Error")
    case WiretapStatus.Closed => span(cls := "badge bg-secondary", "Closed")
  }

  private def renderActiveWiretap(handler: WiretapHandler, onClose: String => Unit): HtmlElement = {
    val displayName = handler.outputName.fold(s"${handler.sqName} (raw)")(out => s"${handler.sqName} / $out")
    div(
      cls := "border rounded p-2 bg-body",
      div(
        cls := "d-flex justify-content-between align-items-center mb-2",
        div(
          cls := "d-flex align-items-center gap-2 flex-grow-1 me-2",
          styleAttr := "min-width: 0",
          span(cls := "fw-semibold text-truncate", displayName),
          child <-- handler.status.signal.map(statusBadge),
          span(
            cls := "small text-body-secondary",
            child.text <-- handler.matchCount.signal.map(n => s"$n ${if (n == 1) "match" else "matches"}"),
          ),
        ),
        button(
          cls := "btn btn-sm btn-outline-danger py-0 px-2 flex-shrink-0",
          title := "Stop wiretap",
          "✕",
          onClick --> { _ => onClose(handler.key) },
        ),
      ),
      // Result log — newest at the bottom. WiretapHandler caps `messages` at MaxMessages,
      // so this list won't grow unbounded.
      child <-- handler.messages.signal.map(_.isEmpty).distinct.map {
        case true =>
          div(cls := "text-body-secondary small", "No matches yet.")
        case false =>
          pre(
            cls := "mb-0 p-2 bg-body-tertiary rounded border small",
            styleAttr := "max-height: 16em; overflow: auto;",
            children <-- handler.messages.signal.map { msgs =>
              msgs.toList.map(m => div(cls := "text-break", m))
            },
          )
      },
    )
  }

  private val VolatileFields = Set("stats", "status", "message")

  private def configOnly(json: Json): Json =
    json.asObject.fold(json)(obj => Json.fromJsonObject(obj.filterKeys(k => !VolatileFields.contains(k))))

  private def liveOnly(json: Json): Json =
    json.asObject.fold(json)(obj => Json.fromJsonObject(obj.filterKeys(VolatileFields.contains)))

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

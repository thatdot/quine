package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom.window

import com.thatdot.quine.openapi._
import com.thatdot.quine.webapp.dataservice.{
  WiretapHandler,
  WiretapOwner,
  WiretapService,
  WiretapStatus,
  WiretapTapPoint,
}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2StandingQueryInfo, V2StandingQueryOutput}

/** Renders the standing queries table with expandable output rows.
  *
  * Pure renderer: receives Signals to read, Observers to write. No API
  * knowledge — the parent wires observers to API calls.
  *
  * Takes a `Signal[List[(name, info)]]` and uses [[splitSeq]] keyed by name so that
  * the DOM for each SQ row (including any open inline output form inside its expanded
  * row) is kept stable across the 5-second polling update. Cells inside a row re-read
  * from the row's signal to stay in sync with the latest stats.
  */
object StandingQueryTable {

  def apply(
    entriesSignal: Signal[List[(String, V2StandingQueryInfo)]],
    canWriteOutputs: Boolean,
    canDelete: Boolean,
    onDeleteSq: Observer[String],
    onRemoveOutput: Observer[(String, String)],
    expandedVar: Var[Set[String]],
    addingOutputFor: Var[Option[String]],
    outputFormState: OutputForm.State,
    outputSchema: Option[SchemaNode],
    spec: ParsedSpec,
    onAddOutput: (String, Json) => Future[Either[String, Json]],
    wiretap: WiretapService,
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
      children <-- entriesSignal.splitSeq(_._1) { strictSignal =>
        val name = strictSignal.key
        val infoSignal = strictSignal.map(_._2)
        val isExpanded = expandedVar.signal.map(_.contains(name)).distinct
        tbody(
          renderMainRow(name, infoSignal, isExpanded, expandedVar, canDelete, onDeleteSq),
          renderExpandedRow(
            name,
            infoSignal,
            isExpanded,
            spec,
            canWriteOutputs,
            canDelete,
            onRemoveOutput,
            addingOutputFor,
            outputFormState,
            outputSchema,
            onAddOutput,
            wiretap,
            editorConfig,
          ),
        )
      },
    )

  private def renderMainRow(
    name: String,
    infoSignal: Signal[V2StandingQueryInfo],
    isExpanded: Signal[Boolean],
    expandedVar: Var[Set[String]],
    canDelete: Boolean,
    onDeleteSq: Observer[String],
  ): HtmlElement = {
    val patternSignal: Signal[String] = infoSignal.map(_.pattern.flatMap(_.query).getOrElse("-")).distinct
    val modeSignal: Signal[String] = infoSignal.map(_.pattern.flatMap(_.mode).getOrElse("-")).distinct
    val outputsCountSignal: Signal[Int] = infoSignal.map(_.outputs.size).distinct
    val rateSignal: Signal[String] = infoSignal
      .map(info => clusterRatePerSecond(info).fold("-")(r => f"$r%.1f/s"))
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
        Option.when(canDelete)(
          button(
            cls := "btn btn-sm btn-ghost-danger",
            title := "Delete",
            i(cls := "cil-trash"),
            onClick --> { _ => onDeleteSq.onNext(name) },
          ),
        ),
      ),
    )
  }

  private def renderExpandedRow(
    name: String,
    infoSignal: Signal[V2StandingQueryInfo],
    isExpanded: Signal[Boolean],
    spec: ParsedSpec,
    canWriteOutputs: Boolean,
    canDelete: Boolean,
    onRemoveOutput: Observer[(String, String)],
    addingOutputFor: Var[Option[String]],
    outputFormState: OutputForm.State,
    outputSchema: Option[SchemaNode],
    onAddOutput: (String, Json) => Future[Either[String, Json]],
    wiretap: WiretapService,
    editorConfig: EmbeddedEditorConfig,
  ): HtmlElement = {
    val outputEntriesSignal: Signal[List[(String, V2StandingQueryOutput)]] =
      infoSignal.map(_.outputs.map(o => o.name -> o))

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
              child.text <-- infoSignal.map(info => configOnly(info.raw).spaces2).distinct,
            ),
            div(
              display <-- configExpanded.signal.map(if (_) "block" else "none"),
              cls := "mt-2",
              strong("Live Stats"),
              pre(
                cls := "mb-0 mt-1 p-2 bg-body rounded border",
                styleAttr := "max-height: 24em; overflow: auto; font-size: 0.85em;",
                child.text <-- infoSignal.map(info => liveOnly(info.raw).spaces2),
              ),
            ),
          ),
          div(
            cls := "d-flex justify-content-between align-items-center mb-2",
            strong("Outputs"),
            child <-- isAddingHere.map { adding =>
              if (adding || !canWriteOutputs) span(display := "none")
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
                  outputEntries.map { case (outputName, output) =>
                    val outputType = describeOutputType(output)
                    tr(
                      td(outputName),
                      td(code(outputType)),
                      td(
                        Option.when(canDelete)(
                          button(
                            cls := "btn btn-sm btn-ghost-danger",
                            title := "Remove output",
                            i(cls := "cil-trash"),
                            onClick --> { _ => onRemoveOutput.onNext((name, outputName)) },
                          ),
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
          // An output is offered as a tap point only where its stream differs from the raw
          // match stream: a Transformed point when it has a transformation, an Enriched point
          // when it has an enrichment. An output with neither is identical to the raw match
          // stream, which the "All matches" option already covers.
          renderWiretapsSection(
            name,
            outputEntriesSignal.map(_.flatMap { case (outputName, o) =>
              val pre =
                if (o.hasTransformation) List[WiretapTapPoint](WiretapTapPoint.PreEnrichment(outputName)) else Nil
              val post =
                if (o.hasEnrichment) List[WiretapTapPoint](WiretapTapPoint.PostEnrichment(outputName)) else Nil
              pre ::: post
            }),
            infoSignal,
            wiretap,
          ),
        ),
      ),
    )
  }

  // Cluster-wide one-minute match rate, summed across members. A standing query is
  // registered on every cluster member and each one reports its own stats keyed by
  // host; taking any one host's value would show arbitrary, load-balancer-routed
  // throughput rather than the SQ's true workload.
  private def clusterRatePerSecond(info: V2StandingQueryInfo): Option[Double] = {
    val perHostRates = info.stats.values.map(_.rates.oneMinute)
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

  private val RawTapSentinel = "__raw_tap__"

  /** The `<select>` option value for a tap point. `pre:`/`post:` prefixes are
    * collision-proof: output names are resource names, which forbid colons (same guarantee
    * [[WiretapHandler.sourceKey]] relies on).
    */
  private def tapPointValue(tapPoint: WiretapTapPoint): String = tapPoint match {
    case WiretapTapPoint.Raw => RawTapSentinel
    case WiretapTapPoint.PreEnrichment(out) => s"pre:$out"
    case WiretapTapPoint.PostEnrichment(out) => s"post:$out"
  }

  /** Inverse of [[tapPointValue]]. */
  private def parseTapPointValue(value: String): WiretapTapPoint =
    if (value == RawTapSentinel) WiretapTapPoint.Raw
    else if (value.startsWith("pre:")) WiretapTapPoint.PreEnrichment(value.stripPrefix("pre:"))
    else WiretapTapPoint.PostEnrichment(value.stripPrefix("post:"))

  private def renderWiretapsSection(
    sqName: String,
    tapPointsSignal: Signal[List[WiretapTapPoint]],
    sqInfoSignal: Signal[V2StandingQueryInfo],
    wiretap: WiretapService,
  ): HtmlElement = {
    // Per-row picker state, persisted across polling rebuilds (the per-row signal stays
    // stable thanks to `splitSeq(_._1)` in the table body).
    val selectedTapPointVar: Var[WiretapTapPoint] = Var(WiretapTapPoint.Raw)
    // Mirror the latest cluster-wide match rate into a Var the click handler can sample.
    // `Signal.now` is protected, so we materialize a writable copy bound to the source.
    val latestRateVar: Var[Double] = Var(0.0)

    // Live list of handlers this section owns, filtered by owner + SQ name.
    val sectionHandlersSignal: Signal[List[WiretapHandler]] =
      wiretap.wiretapsSignal.map { byOwner =>
        byOwner.getOrElse(StreamsWiretapOwner, List.empty).filter(_.sqName == sqName)
      }

    div(
      cls := "mt-3",
      sqInfoSignal.map(clusterRatePerSecond(_).getOrElse(0.0)) --> latestRateVar.writer,
      div(
        cls := "d-flex justify-content-between align-items-center mb-2",
        strong("SQ Inspections"),
      ),
      // Picker row: tap-point dropdown + Start button.
      div(
        cls := "d-flex align-items-center gap-2 mb-2",
        select(
          cls := "form-select form-select-sm",
          styleAttr := "max-width: 24em",
          value <-- selectedTapPointVar.signal.map(tapPointValue),
          onChange.mapToValue --> { v =>
            selectedTapPointVar.set(parseTapPointValue(v))
          },
          children <-- tapPointsSignal.distinct.map { points =>
            option(value := RawTapSentinel, "All matches (before any output workflow)") ::
            points.map { p =>
              val label = p match {
                case WiretapTapPoint.Raw => "All matches"
                case WiretapTapPoint.PreEnrichment(out) => s"Transformed: $out"
                case WiretapTapPoint.PostEnrichment(out) => s"Enriched: $out"
              }
              option(value := tapPointValue(p), label)
            }
          },
        ),
        button(
          cls := "btn btn-sm btn-primary",
          i(cls := "cil-media-play me-1"),
          "Start SQ inspection",
          onClick --> { _ =>
            val tapPoint = selectedTapPointVar.now()
            val rate = latestRateVar.now()
            val proceed =
              if (rate >= HighRateWarningThreshold)
                window.confirm(
                  f"""Standing query "$sqName" is currently matching at $rate%.1f/s (1-minute rate).
                     |
                     |Opening an SQ inspection on a high-rate standing query streams every match to your browser and can noticeably slow down quine.
                     |
                     |Open the SQ inspection anyway?""".stripMargin,
                )
              else true
            if (proceed)
              wiretap.wiretapDispatch.onNext(
                WiretapService.OpenTap(
                  StreamsWiretapOwner,
                  WiretapHandler.sourceKey(sqName, tapPoint),
                  sqName,
                  tapPoint,
                ),
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
            key => wiretap.wiretapDispatch.onNext(WiretapService.CloseTap(StreamsWiretapOwner, key)),
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
    val displayName = handler.tapPoint match {
      case WiretapTapPoint.Raw => s"${handler.sqName} (matches)"
      case WiretapTapPoint.PreEnrichment(out) => s"${handler.sqName} / $out (transformed)"
      case WiretapTapPoint.PostEnrichment(out) => s"${handler.sqName} / $out"
    }
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
          title := "Stop SQ inspection",
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

  private def describeOutputType(output: V2StandingQueryOutput): String =
    output.destinations.map(_.destinationType).distinct match {
      case Nil => "(empty)"
      case single :: Nil => single
      case many => many.mkString(", ")
    }
}

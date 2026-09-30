package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import com.raquo.laminar.codecs.Codec
import org.scalajs.dom

import com.thatdot.quine.webapp.queryui.AtTimeInput._
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModalStyles

/** The query-time dialog, opened from the toolbar's clock button. Replaces the `window.prompt`
  * that used to explain the accepted string formats in a wall of text.
  *
  * Reuses the tap modal's shell classes (overlay, dialog, header, body) and its shell semantics,
  * host-owned open state, outside-click, Escape, exactly as `BackgroundRunModal` does.
  *
  * One text field holds the moment, in the grammar the old prompt accepted (`now`, `-15s`, a
  * date string, epoch millis), validated as it is typed. Above it, a distance ruler
  * ([[AtTimeRuler]]) shows where that moment lies relative to now and can be dragged, which
  * writes an offset shorthand back into the field, so the text stays the single source of truth
  * and [[AtTimeInput.resolveText]] the only parser. The preview shows what the text resolves to
  * before it is applied; an offset is resolved once at that moment rather than sliding afterwards.
  *
  * Pure form: it hands the resolved `Option[Long]` to `onSet`; the host owns the reset of
  * history and canvas that follows.
  */
object AtTimeModal {

  private val TickMillis = 1000

  /** @param openSignal whether the dialog is currently shown
    * @param setOpen close (`false`) or (re)open (`true`); Escape and outside-click call it with
    *                `false`
    * @param atTime the moment currently tracked, shown as the starting point and prefilled
    * @param onSet receives the chosen moment: `None` for live, `Some(millis)` for a pinned instant
    */
  def apply(
    openSignal: Signal[Boolean],
    setOpen: Observer[Boolean],
    atTime: Signal[Option[Long]],
    onSet: Option[Long] => Unit,
  ): HtmlElement = {

    def nowMillis(): Long = js.Date.now().toLong

    val textVar: Var[String] = Var(initialText(None))

    // Where the pointer hovers over the ruler, for the ghost readout; nothing while dragging.
    val hoverVar: Var[Option[Double]] = Var(None)
    val draggingVar: Var[Boolean] = Var(false)

    // A once-a-second tick, running only while the dialog is open, so the offsets, the relative
    // description, and the ruler (all measured from the browser's now) stay current. It is only
    // a trigger: whatever it wakes reads `nowMillis()` afresh, since its own value can be most of
    // a second old by the time a keystroke resolves against it, and a phrase meaning the present
    // would then land a moment after it and be refused as the future.
    val clock: Signal[Unit] =
      openSignal
        .flatMapSwitch(open => if (open) EventStream.periodic(TickMillis) else EventStream.empty)
        .mapToUnit
        .startWith(())

    // Every signal bound below is `.distinct`: the clock re-emits each second, and without it
    // each binding would rewrite its attribute or rebuild its children every tick even when
    // nothing changed, which wipes hover and pressed highlights (Laminar does not dedupe).
    // (`combineWith` drops the clock's `Unit` from the tuple, so it is purely a re-emit.)
    val resolved: Signal[AtTimeInput.Resolved] =
      textVar.signal.combineWith(clock).map(text => resolveText(text, nowMillis())).distinct

    val canApply: Signal[Boolean] = resolved.map {
      case Invalid(_) => false
      case _ => true
    }.distinct

    def close(): Unit = setOpen.onNext(false)

    def submit(): Unit = resolveText(textVar.now(), nowMillis()) match {
      case Live => onSet(None); close()
      case Pinned(millis) => onSet(Some(millis)); close()
      case Invalid(_) => ()
    }

    // Invalid text is marked by the field's red border and the reason in the preview, nothing
    // more; valid text is not marked at all.
    val textInput: Input = input(
      typ := "text",
      cls := s"form-control ${AtTimeModalStyles.input}",
      cls <-- canApply.map(ok => if (ok) "" else AtTimeModalStyles.inputInvalid).distinct,
      aria.label := "Moment to query",
      aria.describedBy := AtTimeModalStyles.previewId,
      autoComplete := "off",
      spellCheck := false,
      controlled(
        value <-- textVar.signal,
        onInput.mapToValue --> textVar.writer,
      ),
    )

    def pick(text: String): Unit = {
      textVar.set(text)
      textInput.ref.focus()
    }

    // ── Ruler ──────────────────────────────────────────────────────────────────

    /** Pointer position in the ruler's own coordinates. */
    def rulerX(e: dom.PointerEvent): Double = {
      val rect = e.currentTarget.asInstanceOf[dom.Element].getBoundingClientRect()
      (e.clientX - rect.left) / rect.width * AtTimeRuler.Width
    }

    def chooseAt(x: Double): Unit = {
      val now = nowMillis()
      textVar.set(shorthand(now + AtTimeRuler.snappedDeltaOf(x), now))
    }

    // Arrow keys move the pin by a constant factor rather than a constant distance, matching
    // the log scale: each press is a visible step wherever the pin is.
    def nudge(outward: Boolean): Unit = {
      val now = nowMillis()
      val delta = resolveText(textVar.now(), now) match {
        case Pinned(millis) => (millis - now).toDouble
        case _ => -AtTimeRuler.MinMillis
      }
      val factor = if (outward) 1.5 else 1.0 / 1.5
      textVar.set(shorthand(now + math.round(delta * factor), now))
    }

    val ruler: SvgElement = svg.svg(
      svg.cls := AtTimeModalStyles.ruler,
      svg.viewBox := s"0 0 ${AtTimeRuler.Width} ${AtTimeRuler.Height}",
      onPointerDown --> { e =>
        draggingVar.set(true)
        hoverVar.set(None)
        e.currentTarget.asInstanceOf[dom.Element].setPointerCapture(e.pointerId)
        chooseAt(rulerX(e))
      },
      onPointerMove --> { e =>
        if (draggingVar.now()) chooseAt(rulerX(e)) else hoverVar.set(Some(rulerX(e)))
      },
      onPointerUp --> (_ => draggingVar.set(false)),
      onPointerCancel --> (_ => draggingVar.set(false)),
      onPointerLeave --> (_ => hoverVar.set(None)),
      // The drawing is a function of a small state value, deduplicated, so the SVG is only
      // rebuilt when a mark actually moves or a label changes, not on every clock tick.
      children <-- resolved
        .combineWith(clock, atTime, hoverVar.signal)
        .map { case (result, _, tracked, hover) => AtTimeRulerView.State(result, nowMillis(), tracked, hover) }
        .distinct
        .map(AtTimeRulerView(_)),
    )

    def legendItem(marker: String, text: String): HtmlElement =
      span(cls := AtTimeModalStyles.legendItem, span(cls := s"${AtTimeModalStyles.legendDot} $marker"), text)

    val rulerBlock: HtmlElement = div(
      cls := AtTimeModalStyles.rulerBlock,
      div(
        cls := AtTimeModalStyles.rulerHead,
        span(cls := AtTimeModalStyles.fieldLabel, "How far from now"),
        span(
          cls := AtTimeModalStyles.legend,
          legendItem(AtTimeModalStyles.legendNow, "now"),
          legendItem(AtTimeModalStyles.legendChosen, "chosen"),
          legendItem(AtTimeModalStyles.legendTracked, "tracking"),
        ),
      ),
      div(
        cls := AtTimeModalStyles.rulerFocus,
        tabIndex := 0,
        role := "slider",
        aria.label := "Distance from now, drag to choose, arrow keys to step",
        aria.valueText <-- resolved
          .combineWith(clock)
          .map {
            case Pinned(millis) => describeRelative(millis, nowMillis())
            case Live => "now"
            case Invalid(_) => "nothing chosen"
          }
          .distinct,
        onKeyDown.filter(e => e.key == "ArrowLeft" || e.key == "ArrowRight") --> { e =>
          e.preventDefault()
          nudge(outward = e.key == "ArrowLeft")
        },
        ruler,
      ),
    )

    // ── Preview and reference ──────────────────────────────────────────────────

    // The preview's text, as a value. Its rows are built once per kind of preview and each
    // string is bound to its own text node, so a change to one row leaves a selection in
    // another intact. Nothing here is measured from the clock: the browser's time is not the
    // server's, so the live preview shows no "right now" and a pinned instant is shown only as
    // itself, in UTC and in the viewer's zone.
    sealed trait Preview { def kind: PreviewKind }
    final case class PreviewInvalid(reason: String) extends Preview { def kind: PreviewKind = PreviewKind.Invalid }
    case object PreviewLive extends Preview { def kind: PreviewKind = PreviewKind.Live }
    final case class PreviewPinned(iso: String, local: String) extends Preview {
      def kind: PreviewKind = PreviewKind.Pinned
    }

    sealed trait PreviewKind
    object PreviewKind {
      case object Invalid extends PreviewKind
      case object Live extends PreviewKind
      case object Pinned extends PreviewKind
    }

    def previewOf(result: AtTimeInput.Resolved): Preview = result match {
      case Invalid(reason) => PreviewInvalid(reason)
      case Live => PreviewLive
      case Pinned(millis) => PreviewPinned(formatIso(millis), formatLocal(millis))
    }

    val previews: Signal[Preview] = resolved.map(previewOf).distinct

    val pinnedPreview: Signal[Option[PreviewPinned]] = previews.map {
      case p: PreviewPinned => Some(p)
      case _ => None
    }.distinct

    def pinnedText(field: PreviewPinned => String): Signal[String] = pinnedPreview.map(_.fold("")(field)).distinct

    // A value that is copied by clicking it. A faint copy glyph after the text says so, and a
    // green "Copied" takes its place for a moment after the click. An offset such as `-15s`
    // names a different instant every second, so the text cannot be selected for long; the
    // click copies whatever the value is at that moment.
    def copyable(text: Signal[String]): HtmlElement = {
      val copied: Var[Boolean] = Var(false)
      val copy: Observer[String] = Observer(NodePropertiesPopup.copyToClipboard(_, copied))
      span(
        cls := AtTimeModalStyles.copyable,
        cls("copied") <-- copied.signal.distinct,
        role := "button",
        tabIndex := 0,
        title <-- copied.signal.map(c => if (c) "Copied" else "Click to copy").distinct,
        onClick.compose(_.sample(text)) --> copy,
        onKeyDown.filter(e => e.key == "Enter" || e.key == " ").preventDefault.compose(_.sample(text)) --> copy,
        span(child.text <-- text),
        i(cls := s"ion-ios-copy-outline ${AtTimeModalStyles.copyGlyph}", aria.hidden := true),
        span(cls := AtTimeModalStyles.copiedTag, "Copied"),
      )
    }

    def preview(kind: PreviewKind): Seq[HtmlElement] = kind match {
      case PreviewKind.Invalid =>
        Seq(div(child.text <-- previews.map {
          case PreviewInvalid(reason) => reason
          case _ => ""
        }.distinct))
      case PreviewKind.Live =>
        Seq(
          span(cls := AtTimeModalStyles.previewKey, "Query time"),
          span(
            cls := AtTimeModalStyles.previewText,
            span(cls := AtTimeModalStyles.liveDot),
            "Live, always the present moment",
          ),
        )
      case PreviewKind.Pinned =>
        Seq(
          span(cls := AtTimeModalStyles.previewKey, "UTC"),
          span(cls := AtTimeModalStyles.previewValue, copyable(pinnedText(_.iso))),
          span(cls := AtTimeModalStyles.previewKey, "Local"),
          span(cls := AtTimeModalStyles.previewText, copyable(pinnedText(_.local))),
        )
    }

    // The accepted formats, each example a button that puts its text in the field, so the
    // reference doubles as the quick picks and there is one list to read rather than two.
    def example(text: String): HtmlElement = button(
      tpe := "button",
      cls := AtTimeModalStyles.formatExample,
      aria.pressed <-- textVar.signal.map(current => (current.trim == text).toString).distinct,
      onClick --> (_ => pick(text)),
      text,
    )

    val formats: HtmlElement = div(
      cls := AtTimeModalStyles.field,
      label(cls := AtTimeModalStyles.fieldLabel, "Accepted formats, click one to use it"),
      div(
        cls := AtTimeModalStyles.formatsTable,
        AtTimeInput.formats.map { f =>
          div(
            cls := AtTimeModalStyles.formatRow,
            div(cls := AtTimeModalStyles.formatExamples, f.examples.map(example)),
            span(cls := AtTimeModalStyles.formatMeaning, f.meaning),
          )
        },
      ),
    )

    // ── Dialog ─────────────────────────────────────────────────────────────────

    div(
      cls := TapModalStyles.overlay,
      display <-- openSignal.map(if (_) "flex" else "none").distinct,
      onClick.filter(e => e.target == e.currentTarget) --> (_ => close()),
      // Gated on `openSignal`: the overlay stays mounted (only `display` toggles), so without
      // the gate every app-wide Escape would close a dialog that isn't showing.
      documentEvents(_.onKeyDown)
        .filter(_.key == "Escape")
        .withCurrentValueOf(openSignal) --> { case (_, open) => if (open) close() },
      // Each open starts from the moment currently tracked, not from the last attempt. The
      // focus waits a tick for the overlay's `display` to have switched, since a hidden input
      // cannot take it.
      openSignal.updates.filter(identity).withCurrentValueOf(atTime) --> { case (_, current) =>
        textVar.set(initialText(current))
        val _ = js.timers.setTimeout(0) {
          textInput.ref.focus()
          textInput.ref.select()
        }
      },
      div(
        cls := s"${TapModalStyles.dialog} ${AtTimeModalStyles.dialog}",
        onClick.stopPropagation --> (_ => ()),
        div(
          cls := TapModalStyles.header,
          span(cls := TapModalStyles.title, "Query time"),
          button(
            tpe := "button",
            cls := TapModalStyles.closeButton,
            title := "Close",
            onClick --> (_ => close()),
            "×",
          ),
        ),
        div(
          cls := TapModalStyles.body,
          div(
            cls := AtTimeModalStyles.form,
            // Enter submits from anywhere in the form except a button, where Enter activates it
            // (the format examples, and the values that copy on click).
            onKeyDown.filter(e =>
              e.key == "Enter" && !e.target.asInstanceOf[dom.Element].matches("button, [role=button]"),
            ) --> (_ => submit()),
            // The same diamond marks this instant on the ruler and in its legend. A pinned
            // instant copies on click; "now" is not a value worth copying.
            div(
              cls := AtTimeModalStyles.tracking,
              span(cls := s"${AtTimeModalStyles.legendDot} ${AtTimeModalStyles.legendTracked}", aria.hidden := true),
              "Currently tracking",
              child <-- atTime.map(_.map(formatIso)).distinct.map(_.fold(code("now"))(iso => code(copyable(Val(iso))))),
            ),
            rulerBlock,
            div(
              cls := AtTimeModalStyles.field,
              label(cls := AtTimeModalStyles.fieldLabel, "Moment to query"),
              textInput,
            ),
            div(
              idAttr := AtTimeModalStyles.previewId,
              cls := AtTimeModalStyles.preview,
              cls <-- resolved.map {
                case Invalid(_) => AtTimeModalStyles.previewError
                case _ => ""
              }.distinct,
              aria.live := "polite",
              children <-- previews.map(_.kind).distinct.map(preview),
            ),
            formats,
            div(
              cls := AtTimeModalStyles.footer,
              button(tpe := "button", cls := "btn btn-secondary", onClick --> (_ => close()), "Cancel"),
              button(
                tpe := "button",
                cls := "btn btn-primary",
                disabled <-- canApply.map(!_).distinct,
                onClick --> (_ => submit()),
                "Set time",
              ),
            ),
          ),
        ),
      ),
    )
  }
}

/** Draws [[AtTimeRuler]] for one [[AtTimeRulerView.State]] of the dialog: the strip and its ticks,
  * the now dot, the chosen instant's pin with its plain-English offset, the tracked instant's
  * diamond, and a ghost readout under the hovering pointer. Rebuilt whole whenever the state
  * changes; it is a few dozen nodes.
  */
object AtTimeRulerView {

  import AtTimeRuler._

  /** A mark with a label: its position on the strip (to a tenth of a unit, which is finer than
    * a pixel) and its text.
    */
  final case class Mark(x: Double, label: String)

  /** Everything the drawing depends on, as a value: comparing two of these tells whether the
    * SVG needs rebuilding. Positions are rounded so a clock tick that moves nothing visible
    * produces an equal state.
    */
  final case class State(live: Boolean, chosen: Option[Mark], tracked: Option[Double], ghost: Option[Mark])

  object State {
    def apply(result: AtTimeInput.Resolved, now: Long, tracked: Option[Long], hover: Option[Double]): State =
      State(
        live = result == AtTimeInput.Live,
        chosen = result match {
          case AtTimeInput.Pinned(millis) =>
            val delta = millis - now
            val beyond = if (offStrip(delta)) "◂ " else ""
            Some(Mark(tenth(xOf(delta)), beyond + AtTimeInput.describeRelative(millis, now)))
          case _ => None
        },
        tracked = tracked.map(t => tenth(xOf(t - now))),
        ghost = hover.map(x => Mark(tenth(x), AtTimeInput.describeRelative(now + math.round(deltaOf(x)), now))),
      )
  }

  private def tenth(d: Double): Double = math.round(d * 10) / 10.0

  private def num(d: Double): String = tenth(d).toString

  /** Rough width of a label in the ruler's 11px font, to size its bubble. */
  private def textWidth(text: String): Double = 7 + text.length * 6.4

  // Laminar defines no attribute for a gradient stop's position.
  private val stopOffset = svg.svgAttr("offset", Codec.stringAsIs, namespace = None)

  private val defs: SvgElement = svg.defs(
    svg.linearGradient(
      svg.idAttr := "at-time-ruler-past-fill",
      svg.x1 := "0",
      svg.x2 := "1",
      svg.stop(stopOffset := "0", svg.stopColor := "#eef0f5"),
      svg.stop(stopOffset := "1", svg.stopColor := "#e4f6ee"),
    ),
  )

  private def strip: Seq[SvgElement] = Seq(
    svg.rect(
      svg.cls := AtTimeModalStyles.rulerPast,
      svg.x := num(LeftEdge),
      svg.y := num(Baseline - 10),
      svg.width := num(NowX - LeftEdge),
      svg.height := "10",
      svg.rx := "2",
    ),
    svg.text(svg.cls := AtTimeModalStyles.rulerEdge, svg.x := num(LeftEdge), svg.y := num(Baseline - 14), "past"),
    svg.line(
      svg.cls := AtTimeModalStyles.rulerBase,
      svg.x1 := num(LeftEdge),
      svg.y1 := num(Baseline),
      svg.x2 := num(NowX),
      svg.y2 := num(Baseline),
    ),
  )

  private def tick(t: Tick): SvgElement = svg.g(
    svg.cls := s"${AtTimeModalStyles.rulerTick} ${if (t.major) AtTimeModalStyles.rulerTickMajor else ""}",
    svg.line(
      svg.x1 := num(t.x),
      svg.y1 := num(Baseline - (if (t.major) 8 else 5)),
      svg.x2 := num(t.x),
      svg.y2 := num(Baseline),
    ),
    svg.text(svg.x := num(t.x), svg.y := num(Baseline + 14), svg.textAnchor := "middle", t.label),
  )

  private def nowMarker(live: Boolean): SvgElement = svg.g(
    Option.when(live)(
      svg.circle(svg.cls := AtTimeModalStyles.rulerNowRing, svg.cx := num(NowX), svg.cy := num(Baseline), svg.r := "6"),
    ),
    svg.circle(
      svg.cls := AtTimeModalStyles.rulerNowDot,
      svg.cx := num(NowX),
      svg.cy := num(Baseline),
      svg.r := (if (live) "6" else "4.5"),
    ),
    svg.text(
      svg.cls := AtTimeModalStyles.rulerNowText,
      svg.x := num(NowX),
      svg.y := num(Baseline - 14),
      svg.textAnchor := "middle",
      "now",
    ),
  )

  /** Where the tracked marker's label sits, at the top edge, above the chosen pin's head and the
    * ghost readout so it stays legible when the chosen instant is the tracked one.
    */
  private val TrackedLabelY = 10.0

  /** The instant currently tracked: a diamond on the baseline, named by a label at the top
    * edge with a leader line down to it, and the same diamond as the legend and the "Currently
    * tracking" line so the three read as one thing.
    */
  private def trackedMarker(x: Double): SvgElement = {
    val label = "tracking"
    val half = textWidth(label) / 2
    val tx = math.min(Width - half - 2, math.max(half + 2, x))
    svg.g(
      svg.cls := AtTimeModalStyles.rulerTracked,
      svg.line(svg.x1 := num(x), svg.y1 := num(TrackedLabelY + 3), svg.x2 := num(x), svg.y2 := num(Baseline - 7)),
      svg.path(svg.d := s"M${num(x)} ${num(Baseline - 7)} l5 7 -5 7 -5 -7z"),
      svg.text(svg.x := num(tx), svg.y := num(TrackedLabelY), svg.textAnchor := "middle", label),
    )
  }

  /** The chosen instant: a pin on the baseline, a bubble under it giving the offset in words. */
  private def chosenMarker(mark: Mark): SvgElement = {
    val Mark(x, text) = mark
    val w = textWidth(text)
    val bx = math.min(Width - w - 2, math.max(2, x - w / 2))
    svg.g(
      svg.cls := AtTimeModalStyles.rulerChosen,
      svg.line(svg.x1 := num(x), svg.y1 := num(Baseline - 22), svg.x2 := num(x), svg.y2 := num(Baseline + 22)),
      svg.path(svg.cls := AtTimeModalStyles.rulerChosenHead, svg.d := s"M${num(x)} ${num(Baseline - 22)} l-5 -8 h10z"),
      svg.rect(
        svg.cls := AtTimeModalStyles.rulerChosenBubble,
        svg.x := num(bx),
        svg.y := num(Baseline + 24),
        svg.width := num(w),
        svg.height := "18",
        svg.rx := "9",
      ),
      svg.text(
        svg.cls := AtTimeModalStyles.rulerChosenText,
        svg.x := num(bx + w / 2),
        svg.y := num(Baseline + 36.5),
        svg.textAnchor := "middle",
        text,
      ),
    )
  }

  /** What the pointer would choose, shown while it hovers. */
  private def ghost(mark: Mark): SvgElement = {
    val Mark(x, text) = mark
    val w = textWidth(text)
    val tx = math.min(Width - w / 2 - 2, math.max(w / 2 + 2, x))
    svg.g(
      svg.cls := AtTimeModalStyles.rulerGhost,
      svg.line(svg.x1 := num(x), svg.y1 := num(Baseline - 22), svg.x2 := num(x), svg.y2 := num(Baseline + 12)),
      svg.text(svg.x := num(tx), svg.y := num(Baseline - 26), svg.textAnchor := "middle", text),
    )
  }

  def apply(state: State): Seq[SvgElement] =
    Seq(defs) ++
    strip ++
    ticks.map(tick) ++
    state.tracked.map(trackedMarker) ++
    state.ghost.map(ghost) ++
    Seq(nowMarker(state.live)) ++
    state.chosen.map(chosenMarker)
}

object AtTimeModalStyles {
  val dialog = "at-time-dialog" // width modifier composed with `.tap-modal-dialog`
  val form = "at-time-form"
  val field = "at-time-field"
  val fieldLabel = "at-time-field-label"
  val input = "at-time-input"
  val inputInvalid = "at-time-input-invalid"
  val tracking = "at-time-tracking"
  val toolbarPinned = "at-time-pinned"
  val canvasTag = "at-time-tag"
  val formatsTable = "at-time-formats-table"
  val formatRow = "at-time-format-row"
  val formatExamples = "at-time-format-examples"
  val formatExample = "at-time-format-example"
  val formatMeaning = "at-time-format-meaning"
  val previewId = "at-time-preview"
  val preview = "at-time-preview"
  val previewError = "at-time-preview-error"
  val previewKey = "at-time-preview-key"
  val previewValue = "at-time-preview-value"
  val previewText = "at-time-preview-text"
  val copyable = "at-time-copyable"
  val copyGlyph = "at-time-copy-glyph"
  val copiedTag = "at-time-copied-tag"
  val liveDot = "at-time-live-dot"
  val footer = "at-time-footer"

  val rulerBlock = "at-time-ruler-block"
  val rulerHead = "at-time-ruler-head"
  val legend = "at-time-legend"
  val legendItem = "at-time-legend-item"
  val legendDot = "at-time-legend-dot"
  val legendNow = "at-time-legend-now"
  val legendChosen = "at-time-legend-chosen"
  val legendTracked = "at-time-legend-tracked"
  val rulerFocus = "at-time-ruler-focus"
  val ruler = "at-time-ruler"
  val rulerPast = "at-time-ruler-past"
  val rulerEdge = "at-time-ruler-edge"
  val rulerBase = "at-time-ruler-base"
  val rulerTick = "at-time-ruler-tick"
  val rulerTickMajor = "at-time-ruler-tick-major"
  val rulerNowDot = "at-time-ruler-now-dot"
  val rulerNowRing = "at-time-ruler-now-ring"
  val rulerNowText = "at-time-ruler-now-text"
  val rulerChosen = "at-time-ruler-chosen"
  val rulerChosenHead = "at-time-ruler-chosen-head"
  val rulerChosenBubble = "at-time-ruler-chosen-bubble"
  val rulerChosenText = "at-time-ruler-chosen-text"
  val rulerTracked = "at-time-ruler-tracked"
  val rulerGhost = "at-time-ruler-ghost"
}

package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.webapp.dataservice.{WiretapHandler, WiretapOwner, WiretapService, WiretapTapPoint}
import com.thatdot.quine.webapp.fields.{FieldLearner, FieldPath, FieldPicker, JsonKind, PathSeg, SampleStatus}
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModalStyles

/** Layer 4 (UI): the "Sample & generate" button + modal shown on a Novelty output. It samples the
  * standing query's raw wiretap, learns the field structure (generic Layers 1–3), lets the user
  * pick observation/id fields, codegens an inline Novelty transformation
  * ([[NoveltyTransformGenerator]]), and writes it straight into the output form's JSON state.
  *
  * Fields can also be learned from pasted sample results, or added by hand. A standing query being
  * created has no results to sample yet, so for it ([[LiveSample]] absent) those are the only ways.
  *
  * The generated function is written inline into the destination's `transformationFunction` (with
  * `transformationFormat`), so it travels with the output — no separate registration on Novelty and
  * no name to keep in sync. Everything Novelty-specific lives here; the field learning/picking is
  * reused unchanged.
  */
object NoveltySampleGenerate {

  /** Where live results come from: the raw wiretap of standing query `sqName`. */
  final case class LiveSample(sqName: String, wiretap: WiretapService)

  /** Distinct owner so this sampling tap never collides with the inspection section's taps. */
  private val SampleOwner = WiretapOwner("novelty-sample")
  private val SampleBudget = 20

  private val ObservationHelp =
    "Observation: this field's value becomes one component of the observation Novelty scores. Novelty " +
    "judges how unusual each result is from its observation components, compared with everything the " +
    "model has already seen."

  private val IdHelp =
    "id: the node each score gets linked to. Pick the field holding its id, e.g. from strId(n)."

  private val ManualFieldHelp =
    "The field's path in each result, as the list above shows it: keys joined by dots, [*] for each " +
    "element of an array, and [\"key\"] for a key that isn't a plain name. Results are { meta, data }, " +
    "so a column the standing query returns is data.<column>."

  /** The kinds a field added by hand can be given: the scalar values an observation component holds. */
  private val ManualKinds: List[JsonKind] = List(JsonKind.Str, JsonKind.Num, JsonKind.Bool)

  private val ObservationOrderHelp =
    "Novelty reads the components in order, scoring each in the context of the ones before it. Put " +
    "low-cardinality fields (few distinct values) before high-cardinality ones (many distinct values)."

  private def recordFormatHelp(recordFormat: String): String =
    if (recordFormat == "TAGGED")
      "TAGGED: the function returns { observation, tag }, with the id as the tag, so each score comes back " +
      "with the id and is linked to that node."
    else
      "BARE: the function returns just the observation, so scores come back without an id. Score nodes are " +
      "still written, but none is linked to the node it scored."

  /** Hover help for an inline term: a dotted-underlined `abbr` showing `help` as its tooltip. */
  private def term(text: String, help: String): HtmlElement =
    abbr(cls := "fw-semibold", title := help, text)

  /** A small info icon showing `help` as its tooltip. */
  private def helpIcon(help: String): HtmlElement =
    i(cls := "cil-info ms-1 text-body-secondary", title := help, aria.label := help)

  private val ObservationRole =
    FieldPicker.Role("observation", "observation", multi = true, description = ObservationHelp)

  /** The id names the one node a score node is linked to, so it must be a single value: a path
    * through an array (`[*]`) names many values and cannot be the id. It becomes the observation's
    * tag in the generated transformation.
    */
  private val IdRole =
    FieldPicker.Role(
      "id",
      "id",
      multi = false,
      allows = !_.segs.contains(PathSeg.Elem),
      description = IdHelp,
      disallowedHint = "Can't be the id: this field is inside an array, so it has many values, and the id " +
        "must be a single value.",
    )

  /** The chosen observation fields, in the order they become components of the observation, with
    * controls to move each up or down. Shown once there is more than one to order.
    */
  private def observationOrder(selections: Var[Vector[FieldPicker.Selection]]): Modifier[HtmlElement] = {
    def move(index: Int, delta: Int): Unit =
      selections.update(FieldPicker.moveWithinRole(_, ObservationRole.id, index, delta))

    def moveButton(iconCls: String, label: String, isDisabled: Boolean, onMove: () => Unit): HtmlElement =
      button(
        tpe := "button",
        cls := "btn btn-sm btn-ghost-secondary py-0 px-1",
        title := label,
        aria.label := label,
        disabled := isDisabled,
        i(cls := iconCls),
        onClick --> (_ => onMove()),
      )

    child <-- selections.signal.map(_.filter(_.roleId == ObservationRole.id).map(_.path)).distinct.map {
      case paths if paths.size < 2 => emptyNode
      case paths =>
        div(
          cls := "mt-2",
          label(cls := "form-label small fw-semibold mb-1", "Observation order", helpIcon(ObservationOrderHelp)),
          ol(
            cls := "list-group list-group-numbered small",
            paths.zipWithIndex.map { case (path, idx) =>
              li(
                cls := "list-group-item d-flex align-items-center py-1",
                code(cls := "small ms-1 me-auto", path.render),
                moveButton("cil-chevron-top", s"Move ${path.render} up", idx == 0, () => move(idx, -1)),
                moveButton("cil-chevron-bottom", s"Move ${path.render} down", idx == paths.size - 1, () => move(idx, 1)),
              )
            },
          ),
        )
    }
  }

  /** @param live            where to sample live results from, if the standing query exists
    * @param destinationPath the Novelty destination's path in `formState`; its workflow is the
    *                        object holding its `destinations` array
    */
  def apply(
    live: Option[LiveSample],
    destinationPath: List[String],
    formState: Var[Json],
  ): HtmlElement = {
    val openVar = Var(false)

    def generatedFrom(sels: Vector[FieldPicker.Selection]): Option[NoveltyTransformGenerator.Generated] = {
      val obs = sels.filter(_.roleId == ObservationRole.id).map(_.path)
      val tag = sels.find(_.roleId == IdRole.id).map(_.path)
      if (obs.isEmpty) None
      else Some(NoveltyTransformGenerator.generate(obs, tag))
    }

    def renderModal(): HtmlElement = {
      val selections = Var(Vector.empty[FieldPicker.Selection])
      val pasteVar = Var("")
      val pasteError = Var(Option.empty[String])
      val manualPathVar = Var("")
      val manualKindVar = Var[JsonKind](JsonKind.Str)
      val manualError = Var(Option.empty[String])
      val workflowPath = destinationPath.dropRight(2)

      // The function runs on what the destination receives, which is the raw result only when the
      // workflow neither transforms nor enriches it; only the raw result can be sampled here.
      val reshapedSteps: Signal[List[String]] = formState.signal.map { json =>
        List(
          "preEnrichmentTransformation" -> "a pre-enrichment transformation",
          "resultEnrichment" -> "an enrichment query",
        )
          .collect {
            case (field, what) if SchemaFormState.getAt(json, workflowPath :+ field).exists(!_.isNull) => what
          }
      }.distinct

      val key = live.map(l => WiretapHandler.sourceKey(l.sqName, WiretapTapPoint.Raw))

      // The raw tap handler appears in wiretapsSignal once we OpenTap on mount (or is already there,
      // if it was left open); follow its match stream whenever it is there. Each raw frame is the
      // full `{ meta, data }` the destination sends as the observation, so we learn fields over it
      // unchanged.
      val records: EventStream[Json] = live.fold(EventStream.empty: EventStream[Json]) { l =>
        l.wiretap.wiretapsSignal
          .map(_.getOrElse(SampleOwner, Nil).find(h => key.contains(h.key)))
          .distinct
          .flatMapSwitch(_.fold(EventStream.empty: EventStream[Json])(_.matches))
      }

      def closeTap(): Unit =
        for { l <- live; k <- key } l.wiretap.wiretapDispatch.onNext(WiretapService.CloseTap(SampleOwner, k))

      val learner = new FieldLearner(records, budget = SampleBudget, onBudgetFilled = () => closeTap())

      def addManualField(): Unit =
        FieldPath.parse(manualPathVar.now()) match {
          case Right(path) =>
            learner.declare(path, manualKindVar.now())
            manualPathVar.set("")
          case Left(err) => manualError.set(Some(s"Can't add that field: $err."))
        }

      def close(): Unit = openVar.set(false)

      def generatedNow(): Option[NoveltyTransformGenerator.Generated] =
        generatedFrom(selections.now())

      // Write the inline transformation into the destination so it travels with the output.
      def useAndClose(gen: NoveltyTransformGenerator.Generated): Unit = {
        formState.update { json =>
          val withFunction =
            SchemaFormState.setAt(json, destinationPath :+ "transformationFunction", Json.fromString(gen.function))
          SchemaFormState.setAt(
            withFunction,
            destinationPath :+ "transformationFormat",
            Json.fromString(gen.recordFormat),
          )
        }
        close()
      }

      val generatedSignal: Signal[Option[NoveltyTransformGenerator.Generated]] =
        selections.signal.map(generatedFrom)

      val sampleBudgetFull: Signal[Boolean] = learner.status.map {
        case SampleStatus.Frozen(_) => true
        case _ => false
      }

      div(
        cls := TapModalStyles.overlay,
        display := "flex",
        learner.connect,
        onMountCallback(_ =>
          for { l <- live; k <- key } l.wiretap.wiretapDispatch
            .onNext(WiretapService.OpenTap(SampleOwner, k, l.sqName, WiretapTapPoint.Raw)),
        ),
        onUnmountCallback(_ => closeTap()),
        onClick.filter(e => e.target == e.currentTarget) --> (_ => close()),
        div(
          cls := TapModalStyles.dialog,
          onClick.stopPropagation --> (_ => ()),
          div(
            cls := TapModalStyles.header,
            span(cls := TapModalStyles.title, "Sample & generate Novelty transformation"),
            button(
              tpe := "button",
              cls := TapModalStyles.closeButton,
              title := "Close",
              "×",
              onClick --> (_ => close()),
            ),
          ),
          div(
            cls := TapModalStyles.body,
            p(
              cls := "small text-body-secondary",
              if (live.isDefined) "Live results from this standing query are sampled to discover their fields. "
              else
                "This standing query doesn't exist yet, so there are no live results to sample: paste a sample " +
                "result or add fields by hand below. ",
              "Choose which fields become the Novelty ",
              term("observation", ObservationHelp),
              " and which (optional) holds the ",
              term("id", IdHelp),
              " of the node each score links back to. The generated transformation is written inline into this " +
              "output.",
            ),
            div(
              cls := "small mb-2",
              child.text <-- learner.status.map {
                case SampleStatus.Empty if live.isEmpty => "No samples yet."
                case SampleStatus.Empty => "Waiting for the standing query to produce results…"
                case SampleStatus.Collecting(n, b) => s"Sampling… $n of $b"
                case SampleStatus.Frozen(n) => s"Sampled $n result(s)."
              },
            ),
            child <-- reshapedSteps.map {
              case Nil => emptyNode
              case steps =>
                div(
                  cls := "alert alert-warning py-1 px-2 small",
                  s"This output has ${steps.mkString(" and ")}, so Novelty receives reshaped results, not " +
                  "the raw results sampled here. Fields picked below may not exist in what Novelty receives; " +
                  "check the generated transformation against the reshaped result before using it.",
                )
            },
            FieldPicker(learner.schema, Vector(ObservationRole, IdRole), selections),
            observationOrder(selections),
            // Paste fallback for when the SQ isn't currently producing (taps are live and lossy).
            div(
              cls := "mt-2",
              label(cls := "form-label small text-body-secondary mb-1", "No results flowing? Paste a sample result:"),
              textArea(
                cls := "form-control form-control-sm font-monospace",
                rows := 2,
                placeholder := """{"meta":{"isPositiveMatch":true},"data":{"id":"n1","num":42}}""",
                disabled <-- sampleBudgetFull,
                controlled(
                  value <-- pasteVar.signal,
                  onInput.mapToValue --> { text => pasteVar.set(text); pasteError.set(None) },
                ),
              ),
              child.maybe <-- pasteError.signal.map(_.map(err => div(cls := "small text-danger mt-1", err))),
              button(
                tpe := "button",
                cls := "btn btn-sm btn-outline-secondary mt-1",
                "Add sample",
                disabled <-- sampleBudgetFull,
                onClick --> { _ =>
                  io.circe.parser.parse(pasteVar.now()) match {
                    case Right(json) =>
                      learner.observeOne(json)
                      pasteVar.set("")
                    case Left(err) => pasteError.set(Some(s"Not valid JSON: ${err.message}"))
                  }
                },
              ),
              child.maybe <-- sampleBudgetFull.map(full =>
                Option.when(full)(
                  div(cls := "form-text", s"All $SampleBudget samples have been taken; no more can be added."),
                ),
              ),
            ),
            // Add a field the samples don't show (or when there are no samples at all).
            div(
              cls := "mt-2",
              label(
                cls := "form-label small text-body-secondary mb-1",
                "Field missing? Add it by hand:",
                helpIcon(ManualFieldHelp),
              ),
              div(
                cls := "d-flex gap-1",
                input(
                  cls := "form-control form-control-sm font-monospace",
                  typ := "text",
                  placeholder := "data.name",
                  controlled(
                    value <-- manualPathVar.signal,
                    onInput.mapToValue --> { text => manualPathVar.set(text); manualError.set(None) },
                  ),
                  onKeyPress.filter(_.key == "Enter") --> (_ => addManualField()),
                ),
                select(
                  cls := "form-select form-select-sm w-auto",
                  ManualKinds.map(k => option(value := JsonKind.label(k), JsonKind.label(k))),
                  value <-- manualKindVar.signal.map(JsonKind.label),
                  onChange.mapToValue --> { label =>
                    ManualKinds.find(JsonKind.label(_) == label).foreach(manualKindVar.set)
                  },
                ),
                button(
                  tpe := "button",
                  cls := "btn btn-sm btn-outline-secondary text-nowrap",
                  "Add field",
                  onClick --> (_ => addManualField()),
                ),
              ),
              child.maybe <-- manualError.signal.map(_.map(err => div(cls := "small text-danger mt-1", err))),
            ),
            // Live preview of what will be written into the output.
            child <-- generatedSignal.map {
              case None =>
                div(
                  cls := "form-text fst-italic mt-2",
                  "Pick at least one observation field to generate a transformation.",
                )
              case Some(gen) =>
                div(
                  cls := "mt-2",
                  label(
                    cls := "form-label small fw-semibold mb-1",
                    s"Generated transformation (${gen.recordFormat})",
                    helpIcon(recordFormatHelp(gen.recordFormat)),
                  ),
                  textArea(
                    cls := "form-control form-control-sm font-monospace",
                    rows := 2,
                    readOnly := true,
                    value := gen.function,
                  ),
                  if (gen.recordFormat == "BARE")
                    div(
                      cls := "alert alert-warning py-1 px-2 small mt-1 mb-0",
                      "No id chosen — scores are still written, but not linked to the nodes they score. Pick the " +
                      "field holding the node's id (strId(n)).",
                    )
                  else emptyNode,
                )
            },
          ),
          div(
            cls := "d-flex justify-content-end gap-2 p-2 border-top",
            button(tpe := "button", cls := "btn btn-sm btn-secondary", "Cancel", onClick --> (_ => close())),
            button(
              tpe := "button",
              cls := "btn btn-sm btn-primary",
              "Use this transformation",
              disabled <-- generatedSignal.map(_.isEmpty),
              onClick --> { _ => generatedNow().foreach(useAndClose) },
            ),
          ),
        ),
      )
    }

    div(
      button(
        tpe := "button",
        cls := "btn btn-sm btn-outline-info mt-1 mb-2",
        i(cls := "cil-magic-wand me-1"),
        if (live.isDefined) "Sample & generate from results" else "Generate from a sample",
        onClick --> (_ => openVar.set(true)),
      ),
      child <-- openVar.signal.map(if (_) renderModal() else emptyNode),
    )
  }
}

package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec, SchemaAnalysis, SchemaNode, UiHints}

/** Recursive renderer that turns a JSON Schema node into CoreUI form fields.
  * Form values are read from and written to a `Var[Json]` state at a given path.
  *
  * Uses compact styling throughout: `form-control-sm` inputs, `mb-2` spacing,
  * small labels, and two-column layout on large screens for simple fields.
  */
object SchemaFormRenderer {

  private val MaxRenderDepth = 8

  /** Max description length to show inline as placeholder text instead of a separate form-text line. */
  private val MaxPlaceholderLen = 60

  /** Render form fields for the given schema node.
    *
    * @param node       the schema to render
    * @param spec       the full parsed spec (for $ref resolution)
    * @param path       the JSON path for this field within the form state
    * @param stateVar   the root form state
    * @param depth      current recursion depth (to prevent infinite loops)
    * @param isRequired whether this field is required by its parent. Drives two things:
    *                   - Primitive renderers show a red asterisk on required labels and
    *                     auto-expand required collapsible textareas.
    *                   - The oneOf renderer auto-populates the discriminator for required
    *                     unions (needed so nested required unions like `source.format.type`
    *                     are always valid), while optional unions (like
    *                     `preEnrichmentTransformation`) start empty with a "— none —"
    *                     option so the user can choose to leave them unset.
    */
  def render(
    node: SchemaNode,
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int = 0,
    isRequired: Boolean = false,
    // Human-facing label override for this field, supplied by the parent object's
    // UI hints (`labels` map). Takes precedence over the node's own title and the
    // humanized property name wherever a label is emitted.
    labelOverride: Option[String] = None,
  ): HtmlElement =
    if (depth >= MaxRenderDepth)
      div(cls := "form-text text-body-secondary", "(nested schema limit reached)")
    else {
      val resolved = node.ref match {
        case Some(_) => OpenApiParser.resolveNode(node, spec.schemas)
        case None =>
          node.allOf match {
            case Some(_) => OpenApiParser.resolveNode(node, spec.schemas)
            case None => node
          }
      }
      // The resolved node carries its originating schema name (populated by the
      // parser and preserved through resolveRef / copy), so UI hints lookup works
      // regardless of whether a ref was followed or the caller already resolved.
      renderResolved(resolved, spec, path, stateVar, depth, isRequired, resolved.schemaName, labelOverride)
    }

  private def renderResolved(
    node: SchemaNode,
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
    isRequired: Boolean,
    schemaName: Option[String],
    labelOverride: Option[String],
  ): HtmlElement = {
    def obj =
      if (node.properties.forall(_.isEmpty)) div() // no fields to render
      else if (isRequired) renderObject(node, spec, path, stateVar, depth, schemaName)
      else renderOptionalObject(node, spec, path, stateVar, depth, schemaName, labelOverride)
    SchemaAnalysis.discriminatedUnion(node, spec) match {
      case Some(union) =>
        renderOneOfWithDiscriminator(node, union, spec, path, stateVar, depth, isRequired, labelOverride)
      case None =>
        node.oneOf match {
          case Some(variants) if variants.nonEmpty =>
            renderOneOfWithoutDiscriminator(node, variants, spec, path, stateVar, depth)
          case _ =>
            node.typ match {
              case Some("object") => obj
              case Some("array") => renderArray(node, spec, path, stateVar, depth)
              case Some("string") if node.`enum`.exists(_.nonEmpty) => renderEnum(node, path, stateVar, isRequired)
              case Some("string") => renderString(node, path, stateVar, isRequired)
              case Some("integer") | Some("number") => renderNumber(node, path, stateVar, isRequired)
              case Some("boolean") => renderBoolean(node, path, stateVar, isRequired)
              case _ if node.properties.exists(_.nonEmpty) => obj
              case _ => renderString(node, path, stateVar, isRequired) // fallback
            }
        }
    }
  }

  /** Wrap an optional plain-object field in an enable/disable toggle. Children render
    * only when enabled, and their primitive defaults only populate state while the
    * toggle is on. Flipping off removes the whole sub-tree from form state, so the
    * field is absent from the submitted JSON — which is what "optional" means.
    *
    * Without this, any object whose children have defaults (like `resultEnrichment`'s
    * `parameter`, `parallelism`, etc.) would materialize in state as a partial object,
    * and the backend would reject because the required child (e.g. `query`) is missing.
    */
  private def renderOptionalObject(
    node: SchemaNode,
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
    schemaName: Option[String],
    labelOverride: Option[String],
  ): HtmlElement = {
    // `distinct` is critical: without it this signal re-emits on every stateVar change
    // (including every keystroke in any descendant input), which rebuilds the child
    // subtree and causes inputs to lose focus mid-typing.
    val enabledSignal =
      stateVar.signal.map(j => SchemaFormState.getAt(j, path).exists(_.asObject.isDefined)).distinct
    // Label precedence: explicit parent-hint override > schema title > humanized
    // property name. This lets a `CypherQuery` used as `resultEnrichment` read as
    // "Enrichment Query" without renaming the shared schema globally.
    val labelText = labelOverride
      .orElse(node.title)
      .getOrElse(SchemaFormState.humanizeFieldName(path.lastOption.getOrElse("")))
    val checkboxId = s"toggle-${path.mkString("-")}"

    div(
      cls := "mb-2",
      div(
        cls := "form-check form-switch",
        input(
          cls := "form-check-input",
          typ := "checkbox",
          role := "switch",
          idAttr := checkboxId,
          controlled(
            checked <-- enabledSignal,
            onClick.mapToChecked --> { isChecked =>
              if (isChecked) stateVar.update(SchemaFormState.setAt(_, path, Json.obj()))
              else stateVar.update(SchemaFormState.removeAt(_, path))
            },
          ),
        ),
        label(
          cls := "form-check-label small fw-semibold",
          forId := checkboxId,
          s"Include $labelText",
        ),
        renderDescription(node),
      ),
      child <-- enabledSignal.map { enabled =>
        // Same left-border rail as array items, so visually grouped children read
        // consistently across both nesting patterns.
        if (enabled)
          div(
            cls := "mt-2 ps-3 border-start border-secondary-subtle",
            renderObject(node, spec, path, stateVar, depth, schemaName),
          )
        else emptyNode
      },
    )
  }

  // --- Primitive renderers (compact: form-control-sm, mb-2, inline placeholders) ---

  /** True when a field looks like a query/code editor that should be a collapsible textarea. */
  private def isQueryField(node: SchemaNode, path: List[String]): Boolean = {
    val fieldName = path.lastOption.getOrElse("").toLowerCase
    fieldName == "query" || node.format.contains("cypher")
  }

  /** Short description suitable for a placeholder, or None if too long / absent. */
  private def placeholderDesc(node: SchemaNode): Option[String] =
    node.description.filter(_.length <= MaxPlaceholderLen)

  private def renderString(
    node: SchemaNode,
    path: List[String],
    stateVar: Var[Json],
    isRequired: Boolean,
  ): HtmlElement = {
    val currentValue = stateVar.signal.map(json => SchemaFormState.getAt(json, path).flatMap(_.asString).getOrElse(""))
    val defaultValue = node.default.flatMap(_.asString).getOrElse("")
    val isQuery = isQueryField(node, path)
    val inlineDesc = if (isQuery) None else placeholderDesc(node)
    val showFormText = !isQuery && inlineDesc.isEmpty

    // Eagerly initialize default value at construction time (not onMount)
    if (defaultValue.nonEmpty) {
      val current = SchemaFormState.getAt(stateVar.now(), path)
      if (current.isEmpty || current.contains(Json.Null))
        stateVar.update(SchemaFormState.setAt(_, path, Json.fromString(defaultValue)))
    }

    div(
      cls := "mb-2",
      if (isQuery) renderCollapsibleTextarea(node, path, currentValue, stateVar, isRequired)
      else
        div(
          renderLabel(node, path, isRequired),
          input(
            cls := "form-control form-control-sm",
            typ := "text",
            inlineDesc.map(d => placeholder := d),
            controlled(
              value <-- currentValue,
              onInput.mapToValue --> { v =>
                stateVar.update(SchemaFormState.setAt(_, path, Json.fromString(v)))
              },
            ),
          ),
          if (showFormText) renderDescription(node) else emptyMod,
        ),
    )
  }

  /** Render a collapsible textarea for query/code fields. Shows a compact header that
    * expands to reveal a monospace textarea when clicked.
    */
  private def renderCollapsibleTextarea(
    node: SchemaNode,
    path: List[String],
    currentValue: Signal[String],
    stateVar: Var[Json],
    isRequired: Boolean,
  ): HtmlElement = {
    val expanded = Var(isRequired)
    val fieldLabel = node.title.getOrElse[String](SchemaFormState.humanizeFieldName(path.lastOption.getOrElse("")))

    div(
      // Header — always visible, click to toggle
      div(
        cls := "d-flex align-items-center cursor-pointer border rounded px-2 py-1 bg-body-tertiary",
        styleAttr := "cursor: pointer",
        onClick --> { _ => expanded.update(!_) },
        i(
          cls <-- expanded.signal.map(e => if (e) "cil-chevron-bottom me-2" else "cil-chevron-right me-2"),
        ),
        span(cls := "small fw-semibold", fieldLabel),
        if (isRequired) span(cls := "text-danger ms-1 small", "*") else emptyNode,
        // Show a preview of the value when collapsed
        child <-- expanded.signal.combineWith(currentValue).map {
          case (false, v) if v.nonEmpty =>
            span(cls := "ms-2 text-body-secondary small text-truncate", styleAttr := "max-width: 300px", v)
          case (false, _) =>
            span(cls := "ms-2 text-body-secondary small fst-italic", "Click to edit")
          case _ => span(display := "none")
        },
      ),
      // Textarea — shown when expanded
      div(
        display <-- expanded.signal.map(if (_) "block" else "none"),
        cls := "mt-1",
        textArea(
          cls := "form-control form-control-sm",
          rows := 4,
          styleAttr := "font-family: monospace; font-size: 0.8rem",
          placeholder := node.description.getOrElse(""),
          controlled(
            value <-- currentValue,
            onInput.mapToValue --> { v =>
              stateVar.update(SchemaFormState.setAt(_, path, Json.fromString(v)))
            },
          ),
        ),
      ),
    )
  }

  private def renderNumber(
    node: SchemaNode,
    path: List[String],
    stateVar: Var[Json],
    isRequired: Boolean,
  ): HtmlElement = {
    val currentValue = stateVar.signal.map(json =>
      SchemaFormState
        .getAt(json, path)
        .flatMap(j => j.asNumber.map(_.toDouble.toString))
        .getOrElse(""),
    )
    val defaultValue = node.default.flatMap(_.asNumber).map(_.toDouble.toString).getOrElse("")
    val inlineDesc = placeholderDesc(node)

    if (defaultValue.nonEmpty) {
      val current = SchemaFormState.getAt(stateVar.now(), path)
      if (current.isEmpty || current.contains(Json.Null))
        node.default.foreach(d => stateVar.update(SchemaFormState.setAt(_, path, d)))
    }

    div(
      cls := "mb-2",
      renderLabel(node, path, isRequired),
      input(
        cls := "form-control form-control-sm",
        typ := "number",
        inlineDesc.map(d => placeholder := d),
        inContext(ctx => onWheel --> { _ => ctx.ref.blur() }),
        controlled(
          value <-- currentValue,
          onInput.mapToValue --> { v =>
            val json = v.toLongOption
              .map(Json.fromLong)
              .orElse(v.toDoubleOption.map(Json.fromDoubleOrNull))
              .getOrElse(Json.Null)
            stateVar.update(SchemaFormState.setAt(_, path, json))
          },
        ),
      ),
      if (inlineDesc.isEmpty) renderDescription(node) else emptyMod,
    )
  }

  private def renderBoolean(
    node: SchemaNode,
    path: List[String],
    stateVar: Var[Json],
    isRequired: Boolean,
  ): HtmlElement = {
    val currentValue =
      stateVar.signal.map(json => SchemaFormState.getAt(json, path).flatMap(_.asBoolean).getOrElse(false))
    val defaultValue = node.default.flatMap(_.asBoolean).getOrElse(false)

    {
      val current = SchemaFormState.getAt(stateVar.now(), path)
      if (current.isEmpty || current.contains(Json.Null))
        stateVar.update(SchemaFormState.setAt(_, path, Json.fromBoolean(defaultValue)))
    }

    div(
      cls := "mb-2 form-check form-switch",
      input(
        cls := "form-check-input",
        typ := "checkbox",
        role := "switch",
        idAttr := "field-" + path.mkString("-"),
        controlled(
          checked <-- currentValue,
          onClick.mapToChecked --> { v =>
            stateVar.update(SchemaFormState.setAt(_, path, Json.fromBoolean(v)))
          },
        ),
      ),
      label(
        cls := "form-check-label small",
        forId := "field-" + path.mkString("-"),
        node.title.getOrElse[String](SchemaFormState.humanizeFieldName(path.lastOption.getOrElse(""))),
        if (isRequired) span(cls := "text-danger ms-1", "*") else emptyMod,
      ),
    )
  }

  private def renderEnum(
    node: SchemaNode,
    path: List[String],
    stateVar: Var[Json],
    isRequired: Boolean,
  ): HtmlElement = {
    val options = node.`enum`.getOrElse(Nil).flatMap(_.asString)
    val currentValue = stateVar.signal.map(json => SchemaFormState.getAt(json, path).flatMap(_.asString).getOrElse(""))
    val defaultValue = node.default.flatMap(_.asString).getOrElse(options.headOption.getOrElse(""))

    if (defaultValue.nonEmpty) {
      val current = SchemaFormState.getAt(stateVar.now(), path)
      if (current.isEmpty || current.contains(Json.Null))
        stateVar.update(SchemaFormState.setAt(_, path, Json.fromString(defaultValue)))
    }

    div(
      cls := "mb-2",
      renderLabel(node, path, isRequired),
      select(
        cls := "form-select form-select-sm",
        controlled(
          value <-- currentValue,
          onChange.mapToValue --> { v =>
            stateVar.update(SchemaFormState.setAt(_, path, Json.fromString(v)))
          },
        ),
        options.map(opt => option(value := opt, opt)),
      ),
    )
  }

  // --- Composite renderers ---

  /** True when a schema represents a "simple" leaf field that can sit in a half-width
    * column on large screens. Complex fields (objects, arrays, oneOf, long-text) span
    * the full row so they don't get awkwardly squished.
    */
  def isSimpleField(node: SchemaNode, spec: ParsedSpec): Boolean = {
    val resolved = node.ref match {
      case Some(_) => OpenApiParser.resolveNode(node, spec.schemas)
      case None => node
    }
    val isLeaf = resolved.typ match {
      case Some("string") | Some("integer") | Some("number") | Some("boolean") => true
      case _ => false
    }
    val hasNoChildren = resolved.properties.forall(_.isEmpty) &&
      resolved.oneOf.forall(_.isEmpty) &&
      resolved.allOf.forall(_.isEmpty)
    val isNotLongText = !resolved.description.exists(_.length > 100) &&
      !resolved.format.contains("cypher")
    isLeaf && hasNoChildren && isNotLongText
  }

  private def renderFieldList(
    fields: List[(String, SchemaNode)],
    requiredFields: Set[String],
    labels: Map[String, String],
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
  ): HtmlElement =
    div(
      cls := "row g-2",
      fields.map { case (propName, propSchema) =>
        val colClass = if (isSimpleField(propSchema, spec)) "col-lg-6" else "col-12"
        div(
          cls := colClass,
          render(
            propSchema,
            spec,
            path :+ propName,
            stateVar,
            depth + 1,
            requiredFields.contains(propName),
            labels.get(propName),
          ),
        )
      },
    )

  private def renderObject(
    node: SchemaNode,
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
    schemaName: Option[String],
  ): HtmlElement = {
    val hints = schemaName.map(spec.hints.forSchema).getOrElse(UiHints.empty)
    val allProps = node.properties.getOrElse(Map.empty)
    val requiredFields = node.required.getOrElse(Set.empty)

    // Apply overlay: hide then reorder. Unknown-to-hints fields render last in
    // spec (map) order, so new schema properties appear automatically without
    // requiring an overlay update.
    val visibleProps = allProps.view.filterKeys(!hints.hide.contains(_)).toMap
    val ordered: List[(String, SchemaNode)] =
      if (hints.order.isEmpty) visibleProps.toList
      else {
        val mentionedInOrder = hints.order.flatMap(n => visibleProps.get(n).map(n -> _))
        val mentionedSet = hints.order.toSet
        val rest = visibleProps.toList.filterNot { case (n, _) => mentionedSet.contains(n) }
        mentionedInOrder ++ rest
      }

    val (primary, optional) = ordered.partition { case (name, schema) =>
      val resolved = OpenApiParser.resolveNode(schema, spec.schemas)
      // `promote` forces into primary regardless of required/default.
      hints.promote.contains(name) ||
      (requiredFields.contains(name) && resolved.default.isEmpty)
    }

    if (optional.isEmpty)
      // All fields are required — render flat
      renderFieldList(primary, requiredFields, hints.labels, spec, path, stateVar, depth)
    else {
      // Some or all fields are optional — collapse them into "More Options"
      val advancedExpanded = Var(false)
      div(
        renderFieldList(primary, requiredFields, hints.labels, spec, path, stateVar, depth),
        div(
          cls := "mt-3 pt-2 border-top",
          div(
            cls := "d-flex align-items-center mb-2",
            styleAttr := "cursor: pointer",
            onClick --> { _ => advancedExpanded.update(!_) },
            i(cls <-- advancedExpanded.signal.map(e => if (e) "cil-chevron-bottom me-2" else "cil-chevron-right me-2")),
            span(cls := "small fw-semibold", "More Options"),
          ),
          div(
            display <-- advancedExpanded.signal.map(if (_) "block" else "none"),
            renderFieldList(optional, requiredFields, hints.labels, spec, path, stateVar, depth),
          ),
        ),
      )
    }
  }

  private def renderArray(
    node: SchemaNode,
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
  ): HtmlElement = {
    // We intentionally avoid `splitSeq` here. Keying by positional index causes
    // splitSeq to recycle a deleted item's DOM (and its local Vars — expanded
    // state, union selectors, etc.) for the item that shifts into that index.
    // A stable-key scheme would fix that but adds dual-state bookkeeping that is
    // hard to justify given these arrays are small in practice (Kafka topics,
    // HTTP headers, etc. — typically 1–5 items). Instead, we rebuild all items
    // when the array length changes. `.distinct` ensures keystrokes within fields
    // don't trigger rebuilds, so inputs keep focus during normal editing.
    val arrayLenSignal: Signal[Int] = stateVar.signal
      .map(j => SchemaFormState.getAt(j, path).flatMap(_.asArray).map(_.size).getOrElse(0))
      .distinct

    div(
      cls := "mb-2",
      renderLabel(node, path),
      children <-- arrayLenSignal.map { n =>
        (0 until n).toList.map { idx =>
          div(
            cls := "d-flex align-items-start mb-1",
            // Left-border "rail" visually groups all fields that belong to this array
            // item so the user can tell where one item's inputs end and the next begin.
            div(
              cls := "flex-grow-1 ps-3 border-start border-secondary-subtle",
              node.items match {
                case Some(itemSchema) =>
                  // Array items are always "required" from the renderer's perspective — the
                  // user already committed to the item by clicking "Add item", and removing
                  // it is what the trash icon is for. Without this, object-typed items
                  // would get wrapped in the "Include …" optional-object toggle, which is
                  // redundant with add/remove and would allow a nonsensical empty slot.
                  render(itemSchema, spec, path :+ idx.toString, stateVar, depth + 1, isRequired = true)
                case None =>
                  renderString(SchemaNode(), path :+ idx.toString, stateVar, isRequired = false)
              },
            ),
            button(
              cls := "btn btn-sm btn-ghost-danger ms-1",
              typ := "button",
              i(cls := "cil-trash"),
              onClick --> { _ =>
                stateVar.update { json =>
                  val arr = SchemaFormState.getAt(json, path).flatMap(_.asArray).getOrElse(Vector.empty)
                  SchemaFormState.setAt(json, path, Json.fromValues(arr.patch(idx, Nil, 1)))
                }
              },
            ),
          )
        }
      },
      button(
        cls := "btn btn-sm btn-outline-primary",
        typ := "button",
        i(cls := "cil-plus me-1"),
        "Add item",
        onClick --> { _ =>
          stateVar.update { json =>
            val arr = SchemaFormState.getAt(json, path).flatMap(_.asArray).getOrElse(Vector.empty)
            SchemaFormState.setAt(json, path, Json.fromValues(arr :+ Json.Null))
          }
        },
      ),
    )
  }

  // --- Discriminated union renderers ---

  private def renderOneOfWithDiscriminator(
    parent: SchemaNode,
    union: SchemaAnalysis.DiscriminatedUnion,
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
    isRequired: Boolean,
    labelOverride: Option[String],
  ): HtmlElement = {
    val propertyName = union.propertyName
    val mapping: Map[String, SchemaNode] = union.variantMap

    // Derive the selected discriminator value reactively from stateVar. `distinct` is
    // critical: the raw stateVar signal re-emits on every keystroke in any descendant
    // input, and without dedup the `child <-- selectedDiscSignal.map { ... }` below
    // would rebuild the variant subtree each time and destroy input focus.
    val selectedDiscSignal: Signal[String] = stateVar.signal.map { json =>
      SchemaFormState
        .getAt(json, path :+ propertyName)
        .flatMap(_.asString)
        .filter(mapping.contains)
        .getOrElse("")
    }.distinct

    // Eagerly initialize discriminator for required fields
    if (isRequired) {
      val existing = SchemaFormState.getAt(stateVar.now(), path :+ propertyName).flatMap(_.asString)
      if (existing.isEmpty) {
        mapping.keys.toList.sorted.headOption.foreach { firstValue =>
          val obj = SchemaFormState
            .getAt(stateVar.now(), path)
            .flatMap(_.asObject)
            .getOrElse(io.circe.JsonObject.empty)
          stateVar.update(
            SchemaFormState.setAt(_, path, Json.fromJsonObject(obj.add(propertyName, Json.fromString(firstValue)))),
          )
        }
      }
    }

    /** Remove the whole union value from form state. Used when the user selects
      * "— none —" on an optional union.
      */
    def clearUnion(): Unit =
      stateVar.update(SchemaFormState.removeAt(_, path))

    /** When switching discriminator values, keep only fields that exist in the new
      * variant's schema (plus the discriminator itself). This ensures shared fields
      * like "parameter" and "query" are preserved, while stale fields from the old
      * variant are dropped so the review JSON stays clean.
      */
    def switchDiscriminator(discValue: String): Unit = {
      val newVariantFields: Set[String] = mapping.get(discValue) match {
        case Some(variantSchema) =>
          val resolved = OpenApiParser.resolveNode(variantSchema, spec.schemas, depth)
          resolved.properties.map(_.keySet).getOrElse(Set.empty) + propertyName
        case None => Set(propertyName)
      }
      val existing =
        SchemaFormState.getAt(stateVar.now(), path).flatMap(_.asObject).getOrElse(io.circe.JsonObject.empty)
      val filtered = existing.toMap.view.filterKeys(newVariantFields.contains).toMap
      val next = Json.fromJsonObject(
        io.circe.JsonObject.fromMap(filtered + (propertyName -> Json.fromString(discValue))),
      )
      stateVar.update(SchemaFormState.setAt(_, path, next))
    }

    val typeObserver = Observer[String] { discValue =>
      if (discValue.isEmpty) clearUnion() else switchDiscriminator(discValue)
    }

    // When a union has only one variant, a type selector is pointless \u2014 it either
    // shows a single-option dropdown ("pick the only thing you can pick") or a
    // two-option dropdown with "\u2014 None \u2014" for optional fields. In the required
    // case we omit the selector entirely; in the optional case we replace it
    // with an "Include \u2026" checkbox so the user can still opt out.
    val isSingleVariant = mapping.size == 1
    val singleVariantKey: Option[String] = if (isSingleVariant) mapping.keys.headOption else None
    // Label precedence: explicit parent-hint override > humanized property name.
    val fieldLabel = labelOverride.getOrElse(SchemaFormState.humanizeFieldName(path.lastOption.getOrElse("")))

    val selector: HtmlElement = (isSingleVariant, isRequired) match {
      case (true, true) =>
        // Nothing to pick \u2014 the eager init already wrote the only valid value.
        span(display := "none")
      case (true, false) =>
        // Optional + single variant: a Yes/No checkbox is clearer than a dropdown.
        // Include the variant name in parentheses so the user sees which concrete
        // type they're opting into — matches the humanization used by the
        // dropdown so the two paths read consistently.
        val singleValue = singleVariantKey.getOrElse("")
        val humanizedVariant = SchemaFormState.humanizeFieldName(singleValue)
        val checkboxId = s"toggle-union-${path.mkString("-")}"
        val enabledSignal = selectedDiscSignal.map(_.nonEmpty).distinct
        div(
          cls := "form-check form-switch mb-2",
          input(
            cls := "form-check-input",
            typ := "checkbox",
            role := "switch",
            idAttr := checkboxId,
            controlled(
              checked <-- enabledSignal,
              onClick.mapToChecked --> { isChecked =>
                if (isChecked) switchDiscriminator(singleValue) else clearUnion()
              },
            ),
          ),
          label(
            cls := "form-check-label small fw-semibold",
            forId := checkboxId,
            s"Include $fieldLabel ($humanizedVariant)",
          ),
        )
      case _ =>
        // Two or more choices: regular dropdown.
        select(
          cls := "form-select form-select-sm mb-2",
          controlled(
            value <-- selectedDiscSignal,
            onChange.mapToValue --> typeObserver,
          ),
          if (!isRequired) List(option(value := "", "\u2014 None \u2014")) else Nil,
          mapping.keys.toList.sorted.map(discValue =>
            option(value := discValue, SchemaFormState.humanizeFieldName(discValue)),
          ),
        )
    }

    div(
      cls := "mb-2",
      // In the optional-single-variant case the checkbox provides the label
      // ("Include X"), so omitting the separate field label avoids redundancy.
      if (isSingleVariant && !isRequired) emptyNode
      else renderLabel(parent, path, isRequired),
      selector,
      child <-- selectedDiscSignal.map { discValue =>
        if (discValue.isEmpty) emptyNode
        else
          mapping.get(discValue) match {
            case Some(variantSchema) =>
              val resolved = OpenApiParser.resolveNode(variantSchema, spec.schemas, depth)
              // `copy` preserves schemaName from the resolved node, so UI hints for
              // the variant (order/promote/hide) apply to the rendered variant fields
              // without needing to thread the name explicitly.
              val filteredSchema = resolved.copy(
                properties = resolved.properties.map(_.removed(propertyName)),
              )
              // isRequired = true: the user already selected this variant (or the
              // schema offered only one), so render its fields directly without the
              // optional-object toggle.
              render(filteredSchema, spec, path, stateVar, depth + 1, isRequired = true)
            case None =>
              div(cls := "text-body-secondary small", "Select a type to configure")
          }
      },
    )
  }

  private def renderOneOfWithoutDiscriminator(
    parent: SchemaNode,
    variants: List[SchemaNode],
    spec: ParsedSpec,
    path: List[String],
    stateVar: Var[Json],
    depth: Int,
  ): HtmlElement = {
    val namedVariants: List[(String, SchemaNode)] = variants.zipWithIndex.map { case (v, idx) =>
      val name = v.title
        .orElse(v.ref.map(_.split("/").last))
        .getOrElse(s"Option ${idx + 1}")
      name -> v
    }

    // If the parent schema has a default value, find which variant matches its JSON type
    // (e.g. default: false → boolean variant, default: ["a"] → array variant).
    val defaultVariantName: Option[String] = parent.default.flatMap { defVal =>
      val jsonType =
        if (defVal.isBoolean) Some("boolean")
        else if (defVal.isString) Some("string")
        else if (defVal.isNumber) Some("number")
        else if (defVal.isArray) Some("array")
        else if (defVal.isObject) Some("object")
        else None
      jsonType.flatMap(jt => namedVariants.find(_._2.typ.contains(jt)).map(_._1))
    }
    val selectedVar = Var(defaultVariantName.orElse(namedVariants.headOption.map(_._1)).getOrElse(""))

    div(
      cls := "mb-2",
      renderLabel(parent, path),
      select(
        cls := "form-select form-select-sm mb-2",
        controlled(
          value <-- selectedVar.signal,
          onChange.mapToValue --> selectedVar,
        ),
        namedVariants.map { case (name, _) =>
          option(value := name, SchemaFormState.humanizeFieldName(name))
        },
      ),
      child <-- selectedVar.signal.map { selected =>
        namedVariants.find(_._1 == selected) match {
          case Some((_, schema)) => render(schema, spec, path, stateVar, depth + 1)
          case None => div()
        }
      },
    )
  }

  // --- Helpers ---

  private def renderLabel(node: SchemaNode, path: List[String], isRequired: Boolean = false): HtmlElement = {
    // Prefer the property name from the path (e.g. "delimiter") over the schema title
    // (e.g. "CsvCharacter"), since the property name is more meaningful to the user.
    // Fall back to title when there's no path context (e.g. top-level render).
    val text: String = path.lastOption
      .map(SchemaFormState.humanizeFieldName)
      .getOrElse(node.title.getOrElse(""))
    label(
      cls := "form-label small fw-semibold mb-1",
      text,
      if (isRequired) span(cls := "text-danger ms-1", "*") else emptyMod,
    )
  }

  /** Short descriptions render inline in full; longer ones get split into a brief inline
    * lede + the remainder tucked into a native `<details>` disclosure. Threshold is just
    * long enough to comfortably fit a full sentence without pushing later form fields
    * off the first screen.
    */
  private val MaxInlineDescLen = 180

  /** Split a description into `(short, optional long)`. Short descriptions return the
    * whole text as short. Long ones are split on the first paragraph break (`\n\n`)
    * when one appears early enough; otherwise on the first sentence boundary within the
    * threshold; otherwise hard-cut at the threshold. The goal is to surface the useful
    * one-line summary while keeping the elaboration one click away.
    */
  private def splitDescription(desc: String): (String, Option[String]) = {
    val cleaned = desc.trim
    if (cleaned.length <= MaxInlineDescLen) (cleaned, None)
    else {
      val paragraphBreak = cleaned.indexOf("\n\n")
      val splitIdx =
        if (paragraphBreak > 0 && paragraphBreak <= MaxInlineDescLen * 2) paragraphBreak
        else {
          val firstSentence = cleaned.indexOf(". ", 40)
          if (firstSentence > 0 && firstSentence <= MaxInlineDescLen) firstSentence + 1
          else MaxInlineDescLen
        }
      val short = cleaned.substring(0, splitIdx).trim
      val long = cleaned.substring(splitIdx).trim
      if (long.isEmpty) (short, None) else (short, Some(long))
    }
  }

  private def renderDescription(node: SchemaNode): Modifier[HtmlElement] =
    node.description.filter(_.nonEmpty) match {
      case None => emptyMod
      case Some(desc) =>
        // Only the lede is shown. The remainder from `splitDescription` is dropped on
        // purpose: schema descriptions sometimes include ASCII diagrams and long prose
        // that render poorly inline without monospace + more polish than we want to
        // invest right now. Revisit if/when we add structured rendering for long docs.
        val (short, _) = splitDescription(desc)
        div(cls := "form-text small mt-0", short)
    }

  private val emptyMod: Modifier[HtmlElement] = Modifier.empty
}

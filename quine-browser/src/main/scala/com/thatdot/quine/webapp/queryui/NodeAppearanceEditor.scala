package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.syntax._

import com.thatdot.quine.routes._
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.{ApiJsonPreview, PredicateBuilder}

sealed trait NodeAppearanceEditorMode
object NodeAppearanceEditorMode {
  final case class Creating(prefillNodes: Seq[UiNode[String]]) extends NodeAppearanceEditorMode
  final case class Editing(index: Int) extends NodeAppearanceEditorMode
}

sealed trait LabelMode
object LabelMode {
  case object None extends LabelMode
  case object Constant extends LabelMode
  case object Property extends LabelMode
}

object NodeAppearanceEditor {

  def apply(
    mode: NodeAppearanceEditorMode,
    initialValue: Option[UiNodeAppearance],
    currentNodes: Signal[Seq[UiNode[String]]],
    onSave: Seq[UiNodeAppearance] => Unit,
    onDelete: Option[() => Unit],
    onCancel: () => Unit,
  ): HtmlElement = {

    val isMultiNode = mode match {
      case NodeAppearanceEditorMode.Creating(nodes) if nodes.size > 1 => true
      case _ => false
    }

    val (initialPredicate, initialSize, initialIcon, initialColor, initialLabel) = initialValue match {
      case Some(app) => (app.predicate, app.size, app.icon, app.color, app.label)
      case None =>
        mode match {
          case NodeAppearanceEditorMode.Creating(nodes) if nodes.size == 1 =>
            val node = nodes.head
            val pred = UiNodePredicate(
              propertyKeys = Vector.empty,
              knownValues = Map.empty,
              dbLabel = if (node.label.nonEmpty) Some(node.label) else None,
            )
            (pred, None: Option[Double], None: Option[String], None: Option[String], None: Option[UiNodeLabel])
          case _ =>
            (
              UiNodePredicate.every,
              None: Option[Double],
              None: Option[String],
              None: Option[String],
              None: Option[UiNodeLabel],
            )
        }
    }

    val predicateVar = Var(initialPredicate)
    val sizeVar = Var(initialSize.map(_.toString).getOrElse(""))
    val iconVar = Var(initialIcon.getOrElse(""))
    val colorVar = Var(initialColor.getOrElse("#97c2fc"))

    val labelModeVar: Var[LabelMode] = Var(initialLabel match {
      case Some(_: UiNodeLabel.Constant) => LabelMode.Constant
      case Some(_: UiNodeLabel.Property) => LabelMode.Property
      case None => LabelMode.None
    })
    val labelConstantVar = Var(initialLabel.collect { case UiNodeLabel.Constant(v) => v }.getOrElse(""))
    val labelPropertyKeyVar = Var(initialLabel.collect { case UiNodeLabel.Property(k, _) => k }.getOrElse(""))
    val isEditing = mode.isInstanceOf[NodeAppearanceEditorMode.Editing]
    val canSave: Signal[Boolean] = sizeVar.signal
      .combineWith(iconVar.signal, colorVar.signal, labelModeVar.signal)
      .map { case (size, icon, color, labelMode) =>
        isEditing || size.trim.nonEmpty || icon.trim.nonEmpty || color != "#97c2fc" || labelMode != LabelMode.None
      }

    val labelPropertyPrefixVar = Var(
      initialLabel.collect { case UiNodeLabel.Property(_, pfx) => pfx }.flatten.getOrElse(""),
    )

    val jsonPreview: Signal[Json] = predicateVar.signal
      .combineWith(sizeVar.signal, iconVar.signal, colorVar.signal)
      .combineWith(
        labelModeVar.signal,
        labelConstantVar.signal,
        labelPropertyKeyVar.signal,
        labelPropertyPrefixVar.signal,
      )
      .map { case (pred, size, icon, color, labelMode, constVal, propKey, propPrefix) =>
        val labelJson = labelMode match {
          case LabelMode.None => Json.Null
          case LabelMode.Constant =>
            Json.obj("type" -> Json.fromString("Constant"), "value" -> Json.fromString(constVal))
          case LabelMode.Property =>
            val base = Json.obj(
              "type" -> Json.fromString("Property"),
              "key" -> Json.fromString(propKey),
            )
            if (propPrefix.nonEmpty)
              base.deepMerge(Json.obj("prefix" -> Json.fromString(propPrefix)))
            else base
        }

        Json.obj(
          "predicate" -> Json.obj(
            "dbLabel" -> pred.dbLabel.fold(Json.Null)(Json.fromString(_)),
            "propertyKeys" -> pred.propertyKeys.asJson,
            "knownValues" -> pred.knownValues.asJson,
          ),
          "size" -> size.toDoubleOption.fold(Json.Null)(Json.fromDoubleOrNull),
          "icon" -> (if (icon.nonEmpty) Json.fromString(icon) else Json.Null),
          "color" -> Json.fromString(color),
          "label" -> labelJson,
        )
      }

    div(
      cls := Styles.editorForm,
      // Predicate
      if (isMultiNode) {
        val nodeCount = mode match {
          case NodeAppearanceEditorMode.Creating(nodes) => nodes.size
          case _ => 0
        }
        div(
          cls := Styles.editorField,
          span(cls := Styles.editorFieldLabel, "Applies to"),
          div(cls := Styles.predicateMatchCount, s"$nodeCount selected nodes"),
        )
      } else
        div(
          cls := Styles.editorField,
          span(cls := Styles.editorFieldLabel, "Appears on"),
          PredicateBuilder(predicateVar, currentNodes),
        ),
      // Size
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Size (pixels)"),
        input(
          cls := Styles.editorInput,
          typ := "number",
          placeholder := "30",
          controlled(
            value <-- sizeVar.signal,
            onInput.mapToValue --> sizeVar.writer,
          ),
        ),
      ),
      // Icon
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Icon (ionicons v2 character)"),
        input(
          cls := Styles.editorInput,
          typ := "text",
          fontFamily := "Ionicons",
          fontSize := "1.2em",
          controlled(
            value <-- iconVar.signal,
            onInput.mapToValue --> iconVar.writer,
          ),
        ),
      ),
      // Color
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Color"),
        div(
          display := "flex",
          alignItems := "center",
          gap := "8px",
          input(
            typ := "color",
            controlled(
              value <-- colorVar.signal,
              onInput.mapToValue --> colorVar.writer,
            ),
          ),
          input(
            cls := Styles.editorInput,
            typ := "text",
            placeholder := "#97c2fc",
            width := "100px",
            controlled(
              value <-- colorVar.signal,
              onInput.mapToValue --> colorVar.writer,
            ),
          ),
          span(
            cls := Styles.colorSwatch,
            background <-- colorVar.signal.map(c => if (c.nonEmpty) c else "#97c2fc"),
          ),
        ),
      ),
      // Label
      div(
        cls := Styles.editorField,
        span(cls := Styles.editorFieldLabel, "Label"),
        div(
          display := "flex",
          gap := "6px",
          button(
            cls <-- labelModeVar.signal.map(m =>
              if (m == LabelMode.None) Styles.editorToggleActive else Styles.editorToggle,
            ),
            "None",
            onClick --> (_ => labelModeVar.set(LabelMode.None)),
          ),
          button(
            cls <-- labelModeVar.signal.map(m =>
              if (m == LabelMode.Constant) Styles.editorToggleActive else Styles.editorToggle,
            ),
            "Constant",
            onClick --> (_ => labelModeVar.set(LabelMode.Constant)),
          ),
          button(
            cls <-- labelModeVar.signal.map(m =>
              if (m == LabelMode.Property) Styles.editorToggleActive else Styles.editorToggle,
            ),
            "Property",
            onClick --> (_ => labelModeVar.set(LabelMode.Property)),
          ),
        ),
        child <-- labelModeVar.signal.map {
          case LabelMode.Constant =>
            div(
              marginTop := "6px",
              input(
                cls := Styles.editorInput,
                typ := "text",
                placeholder := "Fixed label text",
                controlled(
                  value <-- labelConstantVar.signal,
                  onInput.mapToValue --> labelConstantVar.writer,
                ),
              ),
            )
          case LabelMode.Property =>
            div(
              display := "flex",
              flexDirection := "column",
              gap := "6px",
              marginTop := "6px",
              input(
                cls := Styles.editorInput,
                typ := "text",
                placeholder := "Property key (e.g. name)",
                controlled(
                  value <-- labelPropertyKeyVar.signal,
                  onInput.mapToValue --> labelPropertyKeyVar.writer,
                ),
              ),
              input(
                cls := Styles.editorInput,
                typ := "text",
                placeholder := "Prefix (optional)",
                controlled(
                  value <-- labelPropertyPrefixVar.signal,
                  onInput.mapToValue --> labelPropertyPrefixVar.writer,
                ),
              ),
            )
          case LabelMode.None => emptyNode
        },
      ),
      ApiJsonPreview(jsonPreview),
      // Actions
      div(
        cls := Styles.editorActions,
        button(
          "Save",
          disabled <-- canSave.map(!_),
          onClick --> { _ =>
            val sizeOpt = sizeVar.now().toDoubleOption
            val iconOpt = if (iconVar.now().trim.nonEmpty) Some(iconVar.now().trim) else None
            val colorOpt = if (colorVar.now().trim.nonEmpty) Some(colorVar.now().trim) else None
            val labelOpt = labelModeVar.now() match {
              case LabelMode.None => None
              case LabelMode.Constant => Some(UiNodeLabel.Constant(labelConstantVar.now()))
              case LabelMode.Property =>
                val prefix = if (labelPropertyPrefixVar.now().nonEmpty) Some(labelPropertyPrefixVar.now()) else None
                Some(UiNodeLabel.Property(labelPropertyKeyVar.now(), prefix))
            }
            val appearances = mode match {
              case NodeAppearanceEditorMode.Creating(nodes) if nodes.size > 1 =>
                nodes.map { node =>
                  val pred = UiNodePredicate(
                    propertyKeys = Vector.empty,
                    knownValues = Map.empty,
                    dbLabel = if (node.label.nonEmpty) Some(node.label) else None,
                  )
                  UiNodeAppearance(pred, sizeOpt, iconOpt, colorOpt, labelOpt)
                }
              case _ =>
                Seq(UiNodeAppearance(predicateVar.now(), sizeOpt, iconOpt, colorOpt, labelOpt))
            }
            onSave(appearances)
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

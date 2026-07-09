package com.thatdot.quine.webapp.components

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.routes.{UiNode, UiNodePredicate}
import com.thatdot.quine.v2api.routes.V2UiNodePredicate
import com.thatdot.quine.webapp.Styles

object PredicateBuilder {

  def apply(
    predicateVar: Var[UiNodePredicate],
    nodes: Signal[Seq[UiNode[String]]],
  ): HtmlElement = {

    val newKeyVar = Var("")
    val newKnownKeyVar = Var("")
    val newKnownValueVar = Var("")

    val matchCount: Signal[(Int, Int)] = predicateVar.signal
      .combineWith(nodes)
      .map { case (pred, ns) =>
        val matched = ns.count(pred.matches)
        (matched, ns.size)
      }

    div(
      cls := Styles.predicateBuilder,
      // Label field
      div(
        cls := Styles.predicateField,
        label("Label"),
        input(
          typ := "text",
          placeholder := "e.g. Person (optional)",
          cls := Styles.predicateInput,
          controlled(
            value <-- predicateVar.signal.map(_.dbLabel.getOrElse("")),
            onInput.mapToValue --> { v =>
              val lbl = if (v.trim.isEmpty) None else Some(v.trim)
              predicateVar.update(_.copy(dbLabel = lbl))
            },
          ),
        ),
      ),
      // Property keys
      div(
        cls := Styles.predicateField,
        label("Required property keys"),
        div(
          cls := Styles.predicateChips,
          children <-- predicateVar.signal
            .map(_.propertyKeys)
            .map(_.map { key =>
              span(
                cls := Styles.predicateChip,
                key,
                button(
                  cls := Styles.predicateChipRemove,
                  "×",
                  onClick --> { _ =>
                    predicateVar.update(p => p.copy(propertyKeys = p.propertyKeys.filter(_ != key)))
                  },
                ),
              )
            }),
        ),
        div(
          cls := Styles.predicateAddRow,
          input(
            typ := "text",
            placeholder := "property key",
            cls := Styles.predicateInput,
            controlled(
              value <-- newKeyVar.signal,
              onInput.mapToValue --> newKeyVar.writer,
            ),
            onKeyDown.filter(_.key == "Enter") --> { _ =>
              val k = newKeyVar.now().trim
              if (k.nonEmpty) {
                predicateVar.update(p =>
                  if (p.propertyKeys.contains(k)) p
                  else p.copy(propertyKeys = p.propertyKeys :+ k),
                )
                newKeyVar.set("")
              }
            },
          ),
          button(
            "+",
            onClick --> { _ =>
              val k = newKeyVar.now().trim
              if (k.nonEmpty) {
                predicateVar.update(p =>
                  if (p.propertyKeys.contains(k)) p
                  else p.copy(propertyKeys = p.propertyKeys :+ k),
                )
                newKeyVar.set("")
              }
            },
          ),
        ),
      ),
      // Known values
      div(
        cls := Styles.predicateField,
        label("Known property values"),
        div(
          children <-- predicateVar.signal
            .map(_.knownValues.toVector)
            .map(_.map { case (key, value) =>
              div(
                cls := Styles.predicateKvRow,
                span(cls := Styles.predicateKvKey, key),
                span(" = "),
                span(cls := Styles.predicateKvValue, value.noSpaces),
                button(
                  cls := Styles.predicateChipRemove,
                  "×",
                  onClick --> { _ =>
                    predicateVar.update(p => p.copy(knownValues = p.knownValues - key))
                  },
                ),
              )
            }),
        ),
        div(
          cls := Styles.predicateAddRow,
          input(
            typ := "text",
            placeholder := "key",
            cls := Styles.predicateInputSmall,
            controlled(
              value <-- newKnownKeyVar.signal,
              onInput.mapToValue --> newKnownKeyVar.writer,
            ),
          ),
          span(" = "),
          input(
            typ := "text",
            placeholder := "value (JSON)",
            cls := Styles.predicateInputSmall,
            controlled(
              value <-- newKnownValueVar.signal,
              onInput.mapToValue --> newKnownValueVar.writer,
            ),
            onKeyDown.filter(_.key == "Enter") --> { _ =>
              addKnownValue(predicateVar, newKnownKeyVar, newKnownValueVar)
            },
          ),
          button(
            "+",
            onClick --> { _ =>
              addKnownValue(predicateVar, newKnownKeyVar, newKnownValueVar)
            },
          ),
        ),
      ),
      // Match count
      div(
        cls := Styles.predicateMatchCount,
        span(cls := Styles.predicateMatchDot),
        child.text <-- matchCount.map { case (matched, total) =>
          if (matched == total) s"Matches all $total nodes on canvas"
          else s"Matches $matched of $total nodes on canvas"
        },
      ),
    )
  }

  private def addKnownValue(
    predicateVar: Var[UiNodePredicate],
    keyVar: Var[String],
    valueVar: Var[String],
  ): Unit = {
    val k = keyVar.now().trim
    val v = valueVar.now().trim
    if (k.nonEmpty && v.nonEmpty) {
      val jsonVal = io.circe.parser.parse(v).getOrElse(Json.fromString(v))
      predicateVar.update(p => p.copy(knownValues = p.knownValues + (k -> jsonVal)))
      keyVar.set("")
      valueVar.set("")
    }
  }

  def v2(
    predicateVar: Var[V2UiNodePredicate],
    nodes: Signal[Seq[UiNode[String]]],
  ): HtmlElement = {

    val newKeyVar = Var("")
    val newKnownKeyVar = Var("")
    val newKnownValueVar = Var("")

    val matchCount: Signal[(Int, Int)] = predicateVar.signal
      .combineWith(nodes)
      .map { case (pred, ns) =>
        val matched = ns.count(pred.matches)
        (matched, ns.size)
      }

    div(
      cls := Styles.predicateBuilder,
      div(
        cls := Styles.predicateField,
        label("Label"),
        input(
          typ := "text",
          placeholder := "e.g. Person (optional)",
          cls := Styles.predicateInput,
          controlled(
            value <-- predicateVar.signal.map(_.dbLabel.getOrElse("")),
            onInput.mapToValue --> { v =>
              val lbl = if (v.trim.isEmpty) None else Some(v.trim)
              predicateVar.update(_.copy(dbLabel = lbl))
            },
          ),
        ),
      ),
      div(
        cls := Styles.predicateField,
        label("Required property keys"),
        div(
          cls := Styles.predicateChips,
          children <-- predicateVar.signal
            .map(_.propertyKeys)
            .map(_.map { key =>
              span(
                cls := Styles.predicateChip,
                key,
                button(
                  cls := Styles.predicateChipRemove,
                  "×",
                  onClick --> { _ =>
                    predicateVar.update(p => p.copy(propertyKeys = p.propertyKeys.filter(_ != key)))
                  },
                ),
              )
            }),
        ),
        div(
          cls := Styles.predicateAddRow,
          input(
            typ := "text",
            placeholder := "property key",
            cls := Styles.predicateInput,
            controlled(
              value <-- newKeyVar.signal,
              onInput.mapToValue --> newKeyVar.writer,
            ),
            onKeyDown.filter(_.key == "Enter") --> { _ =>
              val k = newKeyVar.now().trim
              if (k.nonEmpty) {
                predicateVar.update(p =>
                  if (p.propertyKeys.contains(k)) p
                  else p.copy(propertyKeys = p.propertyKeys :+ k),
                )
                newKeyVar.set("")
              }
            },
          ),
          button(
            "+",
            onClick --> { _ =>
              val k = newKeyVar.now().trim
              if (k.nonEmpty) {
                predicateVar.update(p =>
                  if (p.propertyKeys.contains(k)) p
                  else p.copy(propertyKeys = p.propertyKeys :+ k),
                )
                newKeyVar.set("")
              }
            },
          ),
        ),
      ),
      div(
        cls := Styles.predicateField,
        label("Known property values"),
        div(
          children <-- predicateVar.signal
            .map(_.knownValues.toVector)
            .map(_.map { case (key, value) =>
              div(
                cls := Styles.predicateKvRow,
                span(cls := Styles.predicateKvKey, key),
                span(" = "),
                span(cls := Styles.predicateKvValue, value.noSpaces),
                button(
                  cls := Styles.predicateChipRemove,
                  "×",
                  onClick --> { _ =>
                    predicateVar.update(p => p.copy(knownValues = p.knownValues - key))
                  },
                ),
              )
            }),
        ),
        div(
          cls := Styles.predicateAddRow,
          input(
            typ := "text",
            placeholder := "key",
            cls := Styles.predicateInputSmall,
            controlled(
              value <-- newKnownKeyVar.signal,
              onInput.mapToValue --> newKnownKeyVar.writer,
            ),
          ),
          span(" = "),
          input(
            typ := "text",
            placeholder := "value (JSON)",
            cls := Styles.predicateInputSmall,
            controlled(
              value <-- newKnownValueVar.signal,
              onInput.mapToValue --> newKnownValueVar.writer,
            ),
            onKeyDown.filter(_.key == "Enter") --> { _ =>
              addKnownValueV2(predicateVar, newKnownKeyVar, newKnownValueVar)
            },
          ),
          button(
            "+",
            onClick --> { _ =>
              addKnownValueV2(predicateVar, newKnownKeyVar, newKnownValueVar)
            },
          ),
        ),
      ),
      div(
        cls := Styles.predicateMatchCount,
        span(cls := Styles.predicateMatchDot),
        child.text <-- matchCount.map { case (matched, total) =>
          if (matched == total) s"Matches all $total nodes on canvas"
          else s"Matches $matched of $total nodes on canvas"
        },
      ),
    )
  }

  private def addKnownValueV2(
    predicateVar: Var[V2UiNodePredicate],
    keyVar: Var[String],
    valueVar: Var[String],
  ): Unit = {
    val k = keyVar.now().trim
    val v = valueVar.now().trim
    if (k.nonEmpty && v.nonEmpty) {
      val jsonVal = io.circe.parser.parse(v).getOrElse(Json.fromString(v))
      predicateVar.update(p => p.copy(knownValues = p.knownValues + (k -> jsonVal)))
      keyVar.set("")
      valueVar.set("")
    }
  }
}

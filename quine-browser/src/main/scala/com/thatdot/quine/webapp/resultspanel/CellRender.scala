package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import io.circe.Printer.noSpaces
import io.circe.{Json, JsonObject}

import com.thatdot.quine.webapp.Styles

/** Renders one JSON cell value, kept one line tall: scalars truncate with a tooltip,
  * arrays become inline chips, and objects get a meaningful preview — nodes show their
  * labels + id, relationships their type, and other objects an inline key:value preview.
  * The full value is always available in the row drawer (click the row).
  */
object CellRender {

  def value(value: Json): HtmlElement = value.fold(
    jsonNull = span(cls := Styles.cellNull, "null"),
    jsonBoolean = bool => scalarText(bool.toString),
    jsonNumber = num => span(cls := Styles.cellNumber, num.toString),
    jsonString = str => scalarText(str),
    jsonArray = arr => span(arr.map(arrayChip)),
    jsonObject = obj => objectCell(obj),
  )

  private def scalarText(text: String): HtmlElement =
    span(cls := Styles.cellTruncate, title := text, text)

  private def arrayChip(element: Json): HtmlElement = {
    val label = element.asString.getOrElse(noSpaces.print(element))
    span(cls := Styles.cellChip, title := label, label)
  }

  private def objectCell(obj: JsonObject): HtmlElement = GraphValue.classify(obj) match {
    case GraphValue.Node(id, labels, props) => nodeCell(id, labels, props)
    case GraphValue.Relationship(name, props) => relationshipCell(name, props)
    case GraphValue.Plain(plain) => genericObject(plain)
  }

  /** Labels as chips + an inline property preview that truncates to the column width
    * (widen the column to see more). The raw id is on hover; full detail in the drawer.
    */
  private def nodeCell(id: Option[String], labels: Vector[String], props: Option[JsonObject]): HtmlElement =
    span(
      cls := Styles.cellNode,
      title := id.getOrElse(""),
      labels.map(label => span(cls := Styles.cellLabelChip, label)),
      props.map(p => span(cls := Styles.cellNodeProps, keyValuePreview(p))),
    )

  private def relationshipCell(name: String, props: Option[JsonObject]): HtmlElement =
    span(
      cls := Styles.cellNode,
      span(cls := Styles.cellRelChip, name),
      props.map(p => span(cls := Styles.cellNodeProps, keyValuePreview(p))),
    )

  /** Inline `key: value, …` preview for a plain object; the cell truncates to fit. */
  private def genericObject(obj: JsonObject): HtmlElement =
    span(cls := Styles.cellTruncate, title := noSpaces.print(Json.fromJsonObject(obj)), keyValuePreview(obj))

  private def keyValuePreview(obj: JsonObject): String =
    if (obj.isEmpty) "{ }"
    else obj.toIterable.map { case (k, v) => s"$k: ${compact(v)}" }.mkString(", ")

  private def compact(value: Json): String = value.asString.getOrElse(noSpaces.print(value))
}

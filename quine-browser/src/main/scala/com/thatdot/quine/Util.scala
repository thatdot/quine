package com.thatdot.quine

import scala.scalajs.js

import org.scalajs.dom.intl.NumberFormat
import slinky.core.facade.ReactElement
import slinky.web.html.pre

object Util {

  /** Turn a map into a JS object */
  def toJsObject(data: Map[String, js.Any]): js.Object =
    js.Dynamic.literal.applyDynamic("apply")(data.toSeq: _*)

  /** Best effort to escape HTML in a string
    *
    * @see https://stackoverflow.com/a/6234804/3072788
    * @param unsafeString string possible containing HTML
    * @param exclusions entities to not escape (this should almost always be empty)
    * @return string in which HTML entities are escaped
    */
  def escapeHtml(unsafeString: String, exclusions: Set[String] = Set.empty): String =
    List("&" -> "&amp;", "<" -> "&lt;", ">" -> "&gt;", "\"" -> "&quot;", "'" -> "&#039;")
      .filter { case (raw, _) => !exclusions.contains(raw) }
      .foldLeft(unsafeString) { case (acc, (raw, entity)) => acc.replace(raw, entity) }

  /** "gently" pretty-print a JSON value: prints in compact form unless an object is close enough to the root of the
    * provided value
    * @param value
    * @return
    */
  def renderJsonResultValue(value: ujson.Value): ReactElement = {
    // use pretty printing when there is an object within the top 2 levels
    val indent = value match {
      case ujson.Obj(_) => true // root entity is an object
      case ujson.Arr(xs) => xs.exists(_.objOpt.nonEmpty) //root entity is an array containing an object
      case _ => false
    }
    if (indent) pre(ujson.write(value, 2))
    else ujson.write(value, -1)
  }

  val UploadIcon = "ion-android-upload"
  val ExplorerIcon = "ion-search"
  val ResultsIcon = "ion-stats-bars"
  val DocumentationIcon = "ion-document-text"
  val DashboardIcon = "ion-speedometer"

  private val nf = new NumberFormat()

  /** Format a number using the browser's language-sensitive number formatting.
    *
    * For example, the number 654321.987
    *
    *   * looks like `654,321.987` in `en-US`
    *   * looks like `654 321,987` in `fr-FR`
    *
    * @param number number to format
    * @return formatted number
    */
  def formatNum(number: Number): String = nf.format(number.doubleValue)
}

package com.thatdot.quine.webapp.resultspanel

import scala.collection.mutable
import scala.scalajs.js

import io.circe.Json
import io.circe.Printer.{noSpaces, spaces2}
import org.scalajs.dom

import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.queryui.DownloadUtils

/** "Data out" for a tabular result: JSON / CSV serialization, clipboard copy, and
  * self-naming file downloads. Operates on whatever rows it is handed (the caller
  * passes the already filtered/sorted result).
  */
object ResultsExport {

  /** Rows as an array of `{ column: value }` objects — the row-oriented shape most
    * tools expect, pretty-printed. Column names become object keys; duplicate column
    * names (e.g. two `AS v` columns, or `RETURN n, n`) are de-duplicated as `v`,
    * `v_2`, … so the shape stays a stable array of objects and no column is dropped.
    */
  def toJson(result: CypherQueryResult): String = {
    val keys = uniqueKeys(result.columns)
    spaces2.print(Json.fromValues(result.results.map(row => Json.fromFields(keys.zip(row)))))
  }

  /** Column names as collision-free object keys: the first occurrence keeps its name;
    * each later duplicate takes the lowest free `_2`, `_3`, … suffix.
    */
  private def uniqueKeys(columns: Seq[String]): Seq[String] = {
    val seen = mutable.HashSet.empty[String]
    columns.map { name =>
      if (seen.add(name)) name
      else {
        var n = 2
        while (!seen.add(s"${name}_$n")) n += 1
        s"${name}_$n"
      }
    }
  }

  /** CSV. By default nested cells become quoted JSON strings so every row keeps the
    * same columns (lossless). With `flatten`, object cells expand into dotted columns
    * (`properties.address.city`) — the column set becomes the union across rows.
    */
  def toCsv(result: CypherQueryResult, flatten: Boolean): String =
    if (flatten) flattenedCsv(result) else flatCsv(result)

  private def flatCsv(result: CypherQueryResult): String = {
    val header = result.columns.map(csvField).mkString(",")
    val rows = result.results.map(row => row.map(cell => csvField(rawCell(cell))).mkString(","))
    (header +: rows).mkString("\r\n")
  }

  private def flattenedCsv(result: CypherQueryResult): String = {
    val rowMaps = result.results.map(row => flattenRow(result.columns, row))
    // Union of all paths, preserving first-seen order.
    val paths = mutable.LinkedHashSet.empty[String]
    rowMaps.foreach(_.keysIterator.foreach(paths += _))
    val ordered = paths.toVector
    val header = ordered.map(csvField).mkString(",")
    val rows = rowMaps.map(m => ordered.map(p => csvField(m.getOrElse(p, ""))).mkString(","))
    (header +: rows).mkString("\r\n")
  }

  private def flattenRow(columns: Seq[String], row: Seq[Json]): mutable.LinkedHashMap[String, String] = {
    val acc = mutable.LinkedHashMap.empty[String, String]
    columns.zip(row).foreach { case (col, value) => flattenValue(col, value, acc) }
    acc
  }

  private def flattenValue(prefix: String, value: Json, acc: mutable.LinkedHashMap[String, String]): Unit =
    value.asObject match {
      case Some(obj) if obj.nonEmpty =>
        obj.toIterable.foreach { case (k, v) => flattenValue(s"$prefix.$k", v, acc) }
      case _ => acc(prefix) = rawCell(value)
    }

  /** A cell's raw (un-escaped) string: strings verbatim, everything else compact JSON. */
  private def rawCell(value: Json): String = value.asString.getOrElse(noSpaces.print(value))

  /** RFC-4180 CSV field escaping. */
  private def csvField(s: String): String =
    if (s.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r')) "\"" + s.replace("\"", "\"\"") + "\""
    else s

  def copyToClipboard(text: String): Unit = {
    val clipboard = dom.window.navigator.asInstanceOf[js.Dynamic].clipboard
    if (!js.isUndefined(clipboard)) clipboard.writeText(text)
    ()
  }

  def download(content: String, ext: String, mimeType: String): Unit =
    DownloadUtils.downloadFile(content, fileName(ext), mimeType)

  /** `quine-results-YYYYMMDD-HHMM.<ext>`. */
  def fileName(ext: String): String = {
    val d = new js.Date()
    def p2(n: Double): String = {
      val i = n.toInt
      if (i < 10) s"0$i" else i.toString
    }
    val ymd = s"${d.getFullYear().toInt}${p2(d.getMonth() + 1)}${p2(d.getDate())}"
    val hm = s"${p2(d.getHours())}${p2(d.getMinutes())}"
    s"quine-results-$ymd-$hm.$ext"
  }
}

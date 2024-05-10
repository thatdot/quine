package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.annotation.nowarn

import org.pegdown.PegDownProcessor

import com.thatdot.quine.app.ingest.serialization.{CypherParseProtobuf, CypherToProtobuf}
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.compiler.cypher.CypherStandingWiretap
import com.thatdot.quine.graph.cypher.{BuiltinFunc, Func, Proc, UserDefinedFunction, UserDefinedProcedure}

object GenerateCypherTables extends App {

  val (builtinFuncsPath, userDefinedFuncsPath, userDefinedProcsPath): (Path, Path, Path) = args match {
    case Array(stringPath1, stringPath2, stringPath3) =>
      (Paths.get(stringPath1), Paths.get(stringPath2), Paths.get(stringPath3))
    case _ =>
      println(s"GenerateCypherTables expected three path arguments but got: ${args.mkString(",")}")
      sys.exit(1)
  }

  val processor = new PegDownProcessor(Integer.MAX_VALUE)

  type Html = String

  /* Note: this is _not_ foolproof sanitization, but it doesn't need to be since it runs on very
   * controlled inputs and only we run it.
   *
   * @param raw HTML to sanitize
   */
  def escapeHtml(unsafeString: String): Html =
    List("&" -> "&amp;", "<" -> "&lt;", ">" -> "&gt;", "\"" -> "&quot;", "'" -> "&#039;")
      .foldLeft(unsafeString) { case (acc, (raw, entity)) => acc.replace(raw, entity) }

  /** HTML table documenting builtin functions
    *
    * @param funcs functions to document
    * @return HTML for table
    */
  def builtinFunctionTable(funcs: Iterable[BuiltinFunc]): Html = {
    val builder = new StringBuilder("<table>")

    // Header
    builder ++= "<thead><tr>"
    builder ++= List("Name", "Signature", "Description").map(h => s"<th>$h</th>").mkString
    builder ++= "</tr></thead>"

    // Body
    builder ++= "<tbody>"
    for (func <- funcs) {
      builder ++= "<tr>"
      builder ++= s"<td><code>${escapeHtml(func.name)}</code></td>"
      builder ++= s"<td><code>${escapeHtml(func.name + func.signature)}</code></td>"
      builder ++= s"<td>${processor.markdownToHtml(func.description)}</td>"
      builder ++= "</tr>"
    }
    builder ++= "</tbody>"

    builder ++= "</table>"

    builder.result()
  }

  /** HTML table documenting user-defined functions
    *
    * @param funcs functions to document
    * @return HTML for table
    */
  def userDefinedFunctionTable(funcs: Iterable[UserDefinedFunction]): Html = {
    val builder = new StringBuilder("<table>")

    // Header
    builder ++= "<thead><tr>"
    builder ++= List("Name", "Signature", "Description").map(h => s"<th>$h</th>").mkString
    builder ++= "</tr></thead>"

    // Body
    builder ++= "<tbody>"
    for (func <- funcs) {
      var firstRow: Boolean = true
      for (sig <- func.signatures) {
        builder ++= "<tr>"
        if (firstRow) {
          builder ++= s"""<td rowspan="${func.signatures.length}"><code>${escapeHtml(func.name)}</code></td>"""
          firstRow = false
        }
        builder ++= s"<td><code>${escapeHtml(sig.pretty(func.name))}</code></td>"
        builder ++= s"<td>${processor.markdownToHtml(sig.description)}</td>"
        builder ++= "</tr>"
      }
    }
    builder ++= "</tbody>"

    builder ++= "</table>"

    builder.result()
  }

  /** HTML table documenting procedures
    *
    * @param udps procedures to document
    * @return HTML for table
    */
  def userDefinedProcedureTable(udps: Iterable[UserDefinedProcedure]): Html = {
    val builder = new StringBuilder("<table>")

    // Header
    builder ++= "<thead><tr>"
    builder ++= List("Name", "Signature", "Description", "Mode").map(h => s"<th>$h</th>").mkString
    builder ++= "</tr></thead>"

    // Body
    builder ++= "<tbody>"
    for (udp <- udps) {
      builder ++= "<tr>"
      builder ++= s"<td><code>${escapeHtml(udp.name)}</code></td>"
      builder ++= s"<td><code>${escapeHtml(udp.signature.pretty(udp.name))}</code></td>"
      builder ++= s"<td>${processor.markdownToHtml(udp.signature.description)}</td>"
      builder ++= s"<td>${if (udp.canContainUpdates) "WRITE" else "READ"}</td>"
      builder ++= "</tr>"
    }
    builder ++= "</tbody>"

    builder ++= "</table>"

    builder.result()
  }

  // Initialize `resolveCalls` and `resolveFunctions`
  com.thatdot.quine.compiler.cypher.resolveCalls
  com.thatdot.quine.compiler.cypher.resolveFunctions

  val paths: List[(Path, String)] = List(
    builtinFuncsPath -> builtinFunctionTable(Func.builtinFunctions.sortBy(_.name)),
    userDefinedFuncsPath -> userDefinedFunctionTable(Func.userDefinedFunctions.values.toList.sortBy(_.name)),
    userDefinedProcsPath -> userDefinedProcedureTable(
      new CypherParseProtobuf(ProtobufSchemaCache.Blocking: @nowarn) ::
      new CypherToProtobuf(ProtobufSchemaCache.Blocking: @nowarn) ::
      (new CypherStandingWiretap((_, _) => None) ::
      Proc.userDefinedProcedures.values.toList).sortBy(_.name)
    )
  )

  for ((outputPath, outputString) <- paths) {
    Files.createDirectories(outputPath.getParent())
    Files.write(
      outputPath,
      outputString.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.CREATE
    )
  }
}

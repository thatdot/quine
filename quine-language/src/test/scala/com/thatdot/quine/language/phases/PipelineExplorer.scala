package com.thatdot.quine.language.phases

import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{PropertyAccess, SymbolTable}
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, MaterializationPhase, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.prettyprint._

/** Interactive pipeline explorer — paste into `sbt quine-language/console` to
  * step through the compiler phases one at a time and inspect intermediate results.
  *
  * Usage:
  * {{{
  * sbt "quine-language/console"
  * // then at the Scala REPL:
  * import com.thatdot.quine.language.phases.PipelineExplorer._
  * explore("MATCH (a:Person)-[r:KNOWS]->(b) WHERE a.name = 'Alice' RETURN a.name, b.age")
  * }}}
  *
  * Each call prints a clearly-labelled section for each phase showing the
  * rewritten AST and accumulated state (symbol table, type entries,
  * property access mapping, diagnostics).
  */
object PipelineExplorer extends SymbolAnalysisInstances {

  import UpgradeModule._
  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid

  // ── pipeline fragments ──────────────────────────────────────────────

  private val lexAndParse =
    LexerPhase andThen ParserPhase

  private val throughSA =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

  private val throughTC =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase()

  private val throughMat =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase() andThen MaterializationPhase

  // ── helpers ─────────────────────────────────────────────────────────

  private def banner(title: String): Unit = {
    val bar = "═" * 72
    println(s"\n╔$bar╗")
    println(s"║ $title${" " * (bar.length - title.length - 1)}║")
    println(s"╚$bar╝")
  }

  private def section(label: String, body: String): Unit = {
    println(s"\n── $label ${"─" * (68 - label.length)}")
    println(body)
  }

  private def prettyTable(table: SymbolTable): String = table.pretty

  private def prettyPropertyAccesses(entries: List[PropertyAccess]): String =
    if (entries.isEmpty) "(none)"
    else
      entries
        .map(pa => s"  synthId=#${pa.synthId}  onBinding=#${pa.onBinding}  property=${pa.property.name}")
        .mkString("\n")

  // ── public API ──────────────────────────────────────────────────────

  /** Run all four phase-stops and print annotated output for each. */
  def explore(cypher: String): Unit = {
    println(s"\nQuery: $cypher")

    // 1) Parse only
    banner("PHASE 1: Lexer → Parser")
    val (parseState, parseResult) =
      lexAndParse.process(cypher).value.run(LexerState(Nil)).value
    parseResult match {
      case Some(q) => section("AST (identifiers still as names)", q.pretty)
      case None => section("PARSE FAILED", parseState.diagnostics.map(_.toString).mkString("\n"))
    }

    // 2) Through Symbol Analysis
    banner("PHASE 2: + Symbol Analysis  (alpha-renaming)")
    val (saState, saResult) =
      throughSA.process(cypher).value.run(LexerState(Nil)).value
    saResult match {
      case Some(q) =>
        section("AST (identifiers rewritten to #N)", q.pretty)
        section("Symbol table — bindings", prettyTable(saState.symbolTable))
        if (saState.diagnostics.nonEmpty)
          section("Diagnostics", saState.diagnostics.map(diagnosticPrettyPrint.pretty(_)).mkString("\n"))
      case None => section("SA FAILED", saState.diagnostics.map(_.toString).mkString("\n"))
    }

    // 3) Through Type Checking
    banner("PHASE 3: + Type Checking  (type annotations)")
    val (tcState, tcResult) =
      throughTC.process(cypher).value.run(LexerState(Nil)).value
    tcResult match {
      case Some(q) =>
        section("AST (expressions annotated with types)", q.pretty)
        section(
          "Symbol table — type entries", {
            val typeVars = tcState.symbolTable.typeVars
            if (typeVars.isEmpty) "(none)"
            else
              typeVars
                .map(te => s"  id=${te.identifier}  ty=${te.ty}")
                .mkString("\n")
          },
        )
        section(
          "Type environment (resolved bindings)",
          if (tcState.typeEnv.isEmpty) "(none)"
          else
            tcState.typeEnv.toList
              .sortBy(_._1.name)
              .map { case (k, v) => s"  $k → $v" }
              .mkString("\n"),
        )
        if (tcState.diagnostics.nonEmpty)
          section("Diagnostics", tcState.diagnostics.map(diagnosticPrettyPrint.pretty(_)).mkString("\n"))
      case None => section("TC FAILED", tcState.diagnostics.map(_.toString).mkString("\n"))
    }

    // 4) Through Materialization
    banner("PHASE 4: + Materialization  (field access → synthetic IDs)")
    val (matState, matResult) =
      throughMat.process(cypher).value.run(LexerState(Nil)).value
    matResult match {
      case Some(q) =>
        section("AST (field accesses on graph elements rewritten)", q.pretty)
        section("Property access mapping", prettyPropertyAccesses(matState.propertyAccessMapping.entries))
        section("Symbol table — bindings", prettyTable(matState.symbolTable))
        if (matState.diagnostics.nonEmpty)
          section("Diagnostics", matState.diagnostics.map(diagnosticPrettyPrint.pretty(_)).mkString("\n"))
      case None => section("MATERIALIZATION FAILED", matState.diagnostics.map(_.toString).mkString("\n"))
    }

    println()
  }
}

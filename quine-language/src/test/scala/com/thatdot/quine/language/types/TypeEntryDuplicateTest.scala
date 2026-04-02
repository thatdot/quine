package com.thatdot.quine.language.types

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}
import com.thatdot.quine.language.phases.{Phase, TypeCheckingPhase, TypeCheckingState}

/** Tests that the type checker does not produce duplicate TypeEntry records
  * for the same binding. Duplicate entries with first-match lookup semantics
  * means a later (shadowing) entry could silently disagree with the
  * authoritative entry from the defining site.
  */
class TypeEntryDuplicateTest extends munit.FunSuite {

  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid
  import com.thatdot.quine.language.phases.UpgradeModule._

  private val pipeline: Phase[LexerState, TypeCheckingState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase()

  def run(query: String): (TypeCheckingState, Option[Query]) =
    pipeline.process(query).value.run(LexerState(Nil)).value

  def getErrors(diagnostics: List[Diagnostic]): List[Diagnostic] =
    diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }

  test("each binding ID should have at most one TypeEntry") {
    // WITH n AS x aliases a node; RETURN x re-projects the same binding.
    // The TC should not produce two TypeEntry records for #2 (x).
    val (state, maybeQuery) = run("MATCH (n) WITH n AS x RETURN x")

    assert(maybeQuery.isDefined, "Should parse and type check")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors: ${state.diagnostics}")

    val entriesById = state.symbolTable.typeVars.groupBy(_.identifier)
    val duplicates = entriesById.filter(_._2.size > 1)

    assert(
      duplicates.isEmpty,
      s"No binding should have multiple TypeEntry records, but found duplicates:\n" +
      duplicates
        .map { case (id, entries) =>
          s"  id=$id has ${entries.size} entries: ${entries.map(_.ty).mkString(", ")}"
        }
        .mkString("\n"),
    )
  }

  test("re-projected binding does not accumulate TypeEntries") {
    // More complex: node goes through two WITH aliases, each re-projection
    // should not add another entry for the same binding.
    val (state, maybeQuery) = run("MATCH (n) WITH n AS x WITH x AS y RETURN y")

    assert(maybeQuery.isDefined, "Should parse and type check")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors: ${state.diagnostics}")

    val entriesById = state.symbolTable.typeVars.groupBy(_.identifier)
    val duplicates = entriesById.filter(_._2.size > 1)

    assert(
      duplicates.isEmpty,
      s"No binding should have multiple TypeEntry records, but found duplicates:\n" +
      duplicates
        .map { case (id, entries) =>
          s"  id=$id has ${entries.size} entries: ${entries.map(_.ty).mkString(", ")}"
        }
        .mkString("\n"),
    )
  }
}

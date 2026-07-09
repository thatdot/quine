package com.thatdot.quine.cypher.phases

import cats.data.{IndexedState, OptionT}
import org.antlr.v4.runtime.CommonTokenStream

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.parsing.CypherParser
import com.thatdot.quine.cypher.visitors.ast.QueryVisitor
import com.thatdot.quine.cypher.{CollectingErrorListener, UnsupportedCypherFeature}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.Phase.PhaseEffect
import com.thatdot.quine.language.phases.{CompilerPhase, CompilerState}

case class ParserState(diagnostics: List[Diagnostic], cypherText: String) extends CompilerState

object ParserPhase
    extends CompilerPhase.SimpleCompilerPhase[
      ParserState,
      CommonTokenStream,
      Query,
    ] {
  override def process(
    tokenStream: CommonTokenStream,
  ): PhaseEffect[ParserState, ParserState, Query] =
    OptionT {
      IndexedState { parserState =>
        val errorListener = new CollectingErrorListener

        val parser = new CypherParser(tokenStream)
        parser.removeErrorListeners()

        parser.addErrorListener(errorListener)

        val tree = parser.oC_Query()

        // A visitor throws UnsupportedCypherFeature for a parseable-but-unsupported construct (see
        // that type). Record it as a SymbolAnalysisError (the query parsed) so it becomes a
        // reported diagnostic rather than an exception escaping the analysis request.
        val (maybeQuery, unsupportedDiagnostics): (Option[Query], List[Diagnostic]) =
          try (QueryVisitor.visitOC_Query(tree), Nil)
          catch {
            case UnsupportedCypherFeature(featureMessage) =>
              (None, List(Diagnostic.SymbolAnalysisError(featureMessage)))
          }
        val diagnostics = errorListener.errors.toList ::: unsupportedDiagnostics

        (
          parserState.copy(
            diagnostics = parserState.diagnostics ::: diagnostics,
          ),
          maybeQuery,
        )
      }
    }
}

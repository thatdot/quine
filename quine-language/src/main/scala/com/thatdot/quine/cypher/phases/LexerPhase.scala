package com.thatdot.quine.cypher.phases

import cats.data.{IndexedState, OptionT}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.CollectingErrorListener
import com.thatdot.quine.cypher.parsing.CypherLexer
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.Phase.PhaseEffect
import com.thatdot.quine.language.phases.{CompilerPhase, CompilerState}

case class LexerState(diagnostics: List[Diagnostic], cypherText: String = "") extends CompilerState

object LexerPhase
    extends CompilerPhase.SimpleCompilerPhase[
      LexerState,
      String,
      CommonTokenStream,
    ] {
  override def process(
    cypherText: String,
  ): PhaseEffect[LexerState, LexerState, CommonTokenStream] =
    OptionT {
      IndexedState { lexerState =>
        val errorListener = new CollectingErrorListener
        val ts =
          try {
            val input = CharStreams.fromString(cypherText)
            val lexer = new CypherLexer(input)

            lexer.removeErrorListeners()
            lexer.addErrorListener(errorListener)

            Some(new CommonTokenStream(lexer))
          } catch {
            case _: Exception => Option.empty[CommonTokenStream]
          }
        LexerState(
          diagnostics = lexerState.diagnostics ::: errorListener.errors.toList,
          cypherText = cypherText,
        ) -> ts
      }
    }
}

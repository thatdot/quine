package com.thatdot.quine.language.diagnostic

sealed trait Diagnostic {
  val message: String

  /** Whether this diagnostic is advisory (a warning) rather than an error the compiler pipeline
    * rejects the query for. The LSP layer maps it to the protocol severity; declaring it on the
    * sealed trait makes the classification exhaustive — a new variant must choose a severity.
    */
  def isWarning: Boolean
}

object Diagnostic {

  /** A syntax error reported by the ANTLR lexer or parser.
    *
    * Positions follow ANTLR conventions: [[line]] and [[endLine]] are 1-based,
    * while [[char]] and [[endChar]] are 0-based columns measured in Unicode
    * code points (the unit used by ANTLR's `CodePointCharStream`). The end
    * position is exclusive, marking the first position after the offending
    * token; an error at end of input has a zero-width range
    * ([[endChar]] == [[char]]).
    */
  final case class ParseError(line: Int, char: Int, message: String, endLine: Int, endChar: Int) extends Diagnostic {

    /** Alias for [[char]], which cannot be called from Java (`char` is a reserved word there). */
    def charPositionInLine: Int = char

    def isWarning: Boolean = false
  }

  object ParseError {

    /** Builds a [[ParseError]] whose range spans a single code point at the error position. */
    def apply(line: Int, char: Int, message: String): ParseError =
      ParseError(line, char, message, endLine = line, endChar = char + 1)
  }

  case class SymbolAnalysisWarning(message: String) extends Diagnostic {
    def isWarning: Boolean = true
  }
  case class SymbolAnalysisError(message: String) extends Diagnostic {
    def isWarning: Boolean = false
  }
  case class TypeCheckError(message: String) extends Diagnostic {
    def isWarning: Boolean = false
  }
}

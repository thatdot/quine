package com.thatdot.quine.language.diagnostic

sealed trait Diagnostic {
  val message: String
}

object Diagnostic {
  case class ParseError(line: Int, char: Int, message: String) extends Diagnostic
  case class SymbolAnalysisWarning(message: String) extends Diagnostic
  case class SymbolAnalysisError(message: String) extends Diagnostic
  case class TypeCheckError(message: String) extends Diagnostic
}

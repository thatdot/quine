package com.thatdot.quine.language.diagnostic

import com.thatdot.quine.language.diagnostic.Diagnostic._

class DiagnosticTest extends munit.FunSuite {

  test("ParseError creation and properties") {
    val error = ParseError(line = 5, char = 12, message = "Unexpected token")

    assertEquals(error.line, 5)
    assertEquals(error.char, 12)
    assertEquals(error.message, "Unexpected token")
  }

  test("SymbolAnalysisWarning creation and properties") {
    val warning = SymbolAnalysisWarning(message = "Unused variable 'x'")

    assertEquals(warning.message, "Unused variable 'x'")
  }

  test("TypeCheckError creation and properties") {
    val error = TypeCheckError(message = "Type mismatch: expected Integer, got String")

    assertEquals(error.message, "Type mismatch: expected Integer, got String")
  }

  test("diagnostic polymorphism") {
    val diagnostics: List[Diagnostic] = List(
      ParseError(1, 0, "Parse error"),
      SymbolAnalysisWarning("Symbol warning"),
      TypeCheckError("Type error"),
    )

    assertEquals(diagnostics.length, 3)

    val messages = diagnostics.map(_.message)
    assert(messages.contains("Parse error"))
    assert(messages.contains("Symbol warning"))
    assert(messages.contains("Type error"))
  }

  test("ParseError with edge case positions") {
    val errorAtStart = ParseError(line = 1, char = 0, message = "Error at start")
    assertEquals(errorAtStart.line, 1)
    assertEquals(errorAtStart.char, 0)

    val errorAtLargePosition = ParseError(line = 1000, char = 999, message = "Error at large position")
    assertEquals(errorAtLargePosition.line, 1000)
    assertEquals(errorAtLargePosition.char, 999)
  }

  test("empty and whitespace-only messages") {
    val emptyMessage = ParseError(1, 0, "")
    assertEquals(emptyMessage.message, "")

    val whitespaceMessage = SymbolAnalysisWarning("   ")
    assertEquals(whitespaceMessage.message, "   ")
  }

  test("very long error messages") {
    val longMessage = "A" * 1000
    val error = TypeCheckError(longMessage)

    assertEquals(error.message.length, 1000)
    assertEquals(error.message, longMessage)
  }

  test("special characters in error messages") {
    val specialChars = "Error with special chars: \n\t\\\"'$@#%"
    val error = ParseError(1, 0, specialChars)

    assertEquals(error.message, specialChars)
  }

  test("diagnostic equality") {
    val error1 = ParseError(5, 10, "Test error")
    val error2 = ParseError(5, 10, "Test error")
    val error3 = ParseError(5, 11, "Test error")
    val error4 = ParseError(5, 10, "Different error")

    assertEquals(error1, error2)
    assertNotEquals(error1, error3)
    assertNotEquals(error1, error4)
  }

  test("different diagnostic types are not equal") {
    val parseError: Diagnostic = ParseError(1, 0, "Error")
    val symbolWarning: Diagnostic = SymbolAnalysisWarning("Error")
    val typeError: Diagnostic = TypeCheckError("Error")

    assertNotEquals(parseError, symbolWarning)
    assertNotEquals(parseError, typeError)
    assertNotEquals(symbolWarning, typeError)
  }

  test("diagnostic toString representation") {
    val parseError = ParseError(5, 12, "Unexpected token")
    val toString = parseError.toString

    // Should contain key information
    assert(toString.contains("5"))
    assert(toString.contains("12"))
    assert(toString.contains("Unexpected token"))
  }

  test("diagnostic collection operations") {
    val errors = List(
      ParseError(1, 0, "Error 1"),
      ParseError(2, 0, "Error 2"),
      SymbolAnalysisWarning("Warning 1"),
    )

    val parseErrors = errors.collect { case p: ParseError => p }
    assertEquals(parseErrors.length, 2)

    val warnings = errors.collect { case w: SymbolAnalysisWarning => w }
    assertEquals(warnings.length, 1)

    val typeErrors = errors.collect { case t: TypeCheckError => t }
    assertEquals(typeErrors.length, 0)
  }

  test("diagnostic filtering by severity") {
    val diagnostics = List(
      ParseError(1, 0, "Critical parse error"),
      SymbolAnalysisWarning("Minor warning"),
      TypeCheckError("Type mismatch error"),
      SymbolAnalysisWarning("Another warning"),
    )

    // Assume ParseError and TypeCheckError are "errors", SymbolAnalysisWarning is "warning"
    val errors = diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }
    assertEquals(errors.length, 2)

    val warnings = diagnostics.filter {
      case _: SymbolAnalysisWarning => true
      case _ => false
    }
    assertEquals(warnings.length, 2)
  }

  test("diagnostic message extraction") {
    val diagnostics = List(
      ParseError(1, 0, "Syntax error"),
      SymbolAnalysisWarning("Unused variable"),
      TypeCheckError("Type mismatch"),
    )

    val messages = diagnostics.map(_.message)
    assertEquals(messages, List("Syntax error", "Unused variable", "Type mismatch"))
  }

  test("diagnostic with multiline messages") {
    val multilineMessage = """Error occurred:
                             |  Line 1 has issue
                             |  Line 2 has another issue""".stripMargin

    val error = TypeCheckError(multilineMessage)
    assertEquals(error.message, multilineMessage)

    val lines = error.message.split("\n")
    assert(lines.length >= 3)
  }
}

package com.thatdot.quine.language.prettyprint

import munit.FunSuite

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, MaterializationPhase, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.phases.{Phase, TypeCheckingPhase, TypeCheckingState, UpgradeModule}

import UpgradeModule._

class PrettyPrintTest extends FunSuite {
  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid

  val pipeline: Phase[LexerState, TypeCheckingState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase() andThen MaterializationPhase

  def parseQuery(query: String): (TypeCheckingState, Option[Query]) =
    pipeline.process(query).value.run(LexerState(Nil)).value

  test("pretty print simple MATCH query") {
    val (state, maybeAst) = parseQuery("MATCH (n:Person) RETURN n.name")

    assert(maybeAst.isDefined, "Should parse successfully")
    // After SA + TC + canonicalization: n.name → Ident(#3), RETURN projection = #2
    // Type annotations are now present (e.g., ": ?field_name_1")
    assertEquals(
      maybeAst.get.pretty,
      """|SinglepartQuery(
         |  parts = [
         |    MATCH [
         |      (#1:Person) @[6-15]
         |    ] @[0-15]
         |  ],
         |  bindings = [
         |    Ident(#3) @[25-29] : ?field_name_1 AS #2 @[24-29]
         |  ]
         |) @[0-29]""".stripMargin,
    )
  }

  test("pretty print symbol table") {
    val (state, _) = parseQuery("MATCH (n:Person) RETURN n.name")

    val prettyTable = state.symbolTable.pretty
    assert(prettyTable.contains("BindingEntry"), "Should have BindingEntry")
    assert(prettyTable.contains("'n"), "Should have binding for 'n")
    // Type checker now populates typeVars
    assert(state.symbolTable.typeVars.nonEmpty, "Should have type entries after type checking")
    // Property access mapping is separate from symbol table after materialization
    assert(state.propertyAccessMapping.nonEmpty, "Should have property access mapping")
    val pa = state.propertyAccessMapping.entries.head
    assertEquals(pa.onBinding, 1, "Should reference node binding 1")
    assertEquals(pa.property, Symbol("name"), "Should reference property 'name")
  }

  test("pretty print complex query with multiple patterns") {
    val (state, maybeAst) =
      parseQuery("MATCH (n:Person)-[r:KNOWS]->(m:Person) WHERE n.age > 30 RETURN n, m, r")

    assert(maybeAst.isDefined, "Should parse successfully")
    // After SA + TC + canonicalization: n.age → Ident(#4), type annotations present
    val prettyAst = maybeAst.get.pretty
    assert(prettyAst.contains("Ident(#4)"), "n.age should be rewritten to Ident(#4)")
    assert(prettyAst.contains("#1:Person"), "Node n should have Person label")
    assert(prettyAst.contains("#2:KNOWS"), "Edge r should have KNOWS type")
    assert(prettyAst.contains(": Edge"), "Edge binding r should have Edge type annotation")

    // Verify symbol table references contain expected entries
    val prettyTable = state.symbolTable.pretty
    assert(prettyTable.contains("BindingEntry"), "Should have BindingEntry")
    assert(prettyTable.contains("'r"), "Should have binding for 'r")
    assert(state.symbolTable.typeVars.nonEmpty, "Should have type entries after type checking")
    // Property access mapping is separate from symbol table
    assert(state.propertyAccessMapping.nonEmpty, "Should have property access mapping for n.age")
    assert(
      state.propertyAccessMapping.entries.exists(_.property == Symbol("age")),
      "Property access mapping should reference 'age",
    )
  }

  test("pretty print diagnostics") {
    val diag: com.thatdot.quine.language.diagnostic.Diagnostic =
      com.thatdot.quine.language.diagnostic.Diagnostic.ParseError(1, 5, "unexpected token")
    assertEquals(diag.pretty, "ParseError(1:5): unexpected token")
  }

  test("pretty print types") {
    import com.thatdot.quine.language.types.{Type, Constraint}
    import cats.data.NonEmptyList

    assertEquals((Type.PrimitiveType.Integer: Type).pretty, "Integer")
    assertEquals((Type.PrimitiveType.String: Type).pretty, "String")
    assertEquals((Type.PrimitiveType.Boolean: Type).pretty, "Boolean")
    assertEquals((Type.Any: Type).pretty, "Any")
    assertEquals((Type.Null: Type).pretty, "Null")
    assertEquals((Type.Error: Type).pretty, "Error")

    val listType: Type = Type.TypeConstructor(Symbol("List"), NonEmptyList.of(Type.PrimitiveType.Integer))
    assertEquals(listType.pretty, "List[Integer]")

    val typeVar: Type = Type.TypeVariable(Symbol("x"), Constraint.Numeric)
    assertEquals(typeVar.pretty, "?x: Numeric")
  }

  test("pretty print source locations") {
    import com.thatdot.quine.language.ast.Source

    assertEquals((Source.TextSource(10, 25): Source).pretty, "@[10-25]")
    assertEquals((Source.NoSource: Source).pretty, "@[?]")
  }

  test("pretty print operators") {
    import com.thatdot.quine.language.ast.Operator

    assertEquals((Operator.Plus: Operator).pretty, "+")
    assertEquals((Operator.Minus: Operator).pretty, "-")
    assertEquals((Operator.And: Operator).pretty, "AND")
    assertEquals((Operator.Or: Operator).pretty, "OR")
    assertEquals((Operator.Equals: Operator).pretty, "=")
    assertEquals((Operator.NotEquals: Operator).pretty, "<>")
    assertEquals((Operator.LessThan: Operator).pretty, "<")
    assertEquals((Operator.GreaterThan: Operator).pretty, ">")
  }

  test("pretty print values") {
    import com.thatdot.quine.language.ast.Value

    assertEquals((Value.Null: Value).pretty, "null")
    assertEquals((Value.True: Value).pretty, "true")
    assertEquals((Value.False: Value).pretty, "false")
    assertEquals((Value.Integer(42): Value).pretty, "42")
    assertEquals((Value.Real(3.14): Value).pretty, "3.14")
    assertEquals((Value.Text("hello"): Value).pretty, "\"hello\"")
  }

  test("state with errors has diagnostics") {
    val (state, _) = parseQuery("MATCH (n) RETURN undefined_var")

    assert(
      state.diagnostics.exists(_.toString.contains("Undefined variable 'undefined_var'")),
      s"Should have undefined variable error, got: ${state.diagnostics}",
    )
  }

  test("Doc rendering with indentation") {
    import Doc._

    val doc = concat(
      text("outer("),
      nest(1, concat(line, text("inner"))),
      line,
      text(")"),
    )

    assertEquals(render(doc, 2), "outer(\n  inner\n)")
  }
}

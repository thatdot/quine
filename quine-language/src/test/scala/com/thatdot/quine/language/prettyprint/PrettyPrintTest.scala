package com.thatdot.quine.language.prettyprint

import munit.FunSuite

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase, SymbolAnalysisState}
import com.thatdot.quine.language.phases.Phase
import com.thatdot.quine.language.phases.UpgradeModule._

class PrettyPrintTest extends FunSuite {
  val pipeline: Phase[LexerState, SymbolAnalysisState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

  def parseQuery(query: String): (com.thatdot.quine.cypher.phases.SymbolAnalysisState, Option[Query]) =
    pipeline.process(query).value.run(LexerState(Nil)).value

  test("pretty print simple MATCH query") {
    val (state, maybeAst) = parseQuery("MATCH (n:Person) RETURN n.name")

    assert(maybeAst.isDefined, "Should parse successfully")
    assertEquals(
      maybeAst.get.pretty,
      """|SinglepartQuery(
         |  parts = [
         |    MATCH [
         |      (#1:Person) @[6-15]
         |    ] @[0-15]
         |  ],
         |  bindings = [
         |    FieldAccess(
         |      of = Ident(#1) @[24-24],
         |      field = 'name
         |    ) @[25-29] AS #2 @[24-29]
         |  ]
         |) @[0-29]""".stripMargin,
    )
  }

  test("pretty print symbol table") {
    val (state, _) = parseQuery("MATCH (n:Person) RETURN n.name")

    assertEquals(
      state.symbolTable.pretty,
      s"""|SymbolTable(
          |  references = [
          |    ExpressionEntry(
          |      id = 2,
          |      exp = FieldAccess(
          |        of = Ident(#1) @[24-24],
          |        field = 'name
          |      ) @[25-29],
          |      source = @[24-29]
          |    ),
          |    QuineToCypherIdEntry(
          |      id = 2,
          |      cypherName = 'n.name,
          |      source = @[24-29]
          |    ),
          |    NodeEntry(
          |      id = 1,
          |      labels = Set(
          |        'Person
          |      ),
          |      source = @[6-15]
          |    ),
          |    QuineToCypherIdEntry(
          |      id = 1,
          |      cypherName = 'n,
          |      source = @[6-15]
          |    )
          |  ],
          |  typeVars = [
          |    ${""}
          |  ]
          |)""".stripMargin,
    )
  }

  test("pretty print complex query with multiple patterns") {
    val (state, maybeAst) =
      parseQuery("MATCH (n:Person)-[r:KNOWS]->(m:Person) WHERE n.age > 30 RETURN n, m, r")

    assert(maybeAst.isDefined, "Should parse successfully")
    assertEquals(
      maybeAst.get.pretty,
      """|SinglepartQuery(
         |  parts = [
         |    MATCH [
         |      (#1:Person) @[6-15] -[#2:KNOWS]-> @[16-27] (#3:Person) @[28-37] @[6-37]
         |    ]
         |    WHERE BinOp(
         |      op = >,
         |      lhs = FieldAccess(
         |        of = Ident(#1) @[45-45],
         |        field = 'age
         |      ) @[46-49],
         |      rhs = 30 @[53-54]
         |    ) @[45-54] @[0-54]
         |  ],
         |  bindings = [
         |    Ident(#1) @[63-63] AS #1 @[63-63],
         |    Ident(#3) @[66-66] AS #3 @[66-66],
         |    Ident(#2) @[69-69] AS #2 @[69-69]
         |  ]
         |) @[0-69]""".stripMargin,
    )

    assertEquals(
      state.symbolTable.pretty,
      s"""|SymbolTable(
          |  references = [
          |    ExpressionEntry(
          |      id = 2,
          |      exp = Ident(#2) @[69-69],
          |      source = @[69-69]
          |    ),
          |    ExpressionEntry(
          |      id = 3,
          |      exp = Ident(#3) @[66-66],
          |      source = @[66-66]
          |    ),
          |    ExpressionEntry(
          |      id = 1,
          |      exp = Ident(#1) @[63-63],
          |      source = @[63-63]
          |    ),
          |    NodeEntry(
          |      id = 3,
          |      labels = Set(
          |        'Person
          |      ),
          |      source = @[28-37]
          |    ),
          |    QuineToCypherIdEntry(
          |      id = 3,
          |      cypherName = 'm,
          |      source = @[28-37]
          |    ),
          |    EdgeEntry(
          |      id = 2,
          |      edgeType = 'KNOWS,
          |      direction = Right,
          |      source = @[16-27]
          |    ),
          |    QuineToCypherIdEntry(
          |      id = 2,
          |      cypherName = 'r,
          |      source = @[16-27]
          |    ),
          |    NodeEntry(
          |      id = 1,
          |      labels = Set(
          |        'Person
          |      ),
          |      source = @[6-15]
          |    ),
          |    QuineToCypherIdEntry(
          |      id = 1,
          |      cypherName = 'n,
          |      source = @[6-15]
          |    )
          |  ],
          |  typeVars = [
          |    ${""}
          |  ]
          |)""".stripMargin,
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

  test("pretty print state with errors") {
    val (state, _) = parseQuery("MATCH (n) RETURN undefined_var")

    assertEquals(
      state.pretty,
      s"""|SymbolAnalysisState(
          |  diagnostics = [
          |    SymbolAnalysisError: Undefined variable 'undefined_var' at TextSource(17,29)
          |  ],
          |  symbolTable = SymbolTable(
          |    references = [
          |      ExpressionEntry(
          |        id = 3,
          |        exp = Ident(#2) @[17-29],
          |        source = @[17-29]
          |      ),
          |      QuineToCypherIdEntry(
          |        id = 3,
          |        cypherName = 'undefined_var,
          |        source = @[17-29]
          |      ),
          |      NodeEntry(
          |        id = 1,
          |        labels = Set(),
          |        source = @[6-8]
          |      ),
          |      QuineToCypherIdEntry(
          |        id = 1,
          |        cypherName = 'n,
          |        source = @[6-8]
          |      )
          |    ],
          |    typeVars = [
          |      ${""}
          |    ]
          |  ),
          |  cypherText = "MATCH (n) RETURN undefined_var"
          |)""".stripMargin,
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

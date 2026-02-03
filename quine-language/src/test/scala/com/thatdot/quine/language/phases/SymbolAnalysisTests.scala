package com.thatdot.quine.language.phases

import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart.ReadingClausePart
import com.thatdot.quine.cypher.ast.ReadingClause.{FromPatterns, FromSubquery, FromUnwind}
import com.thatdot.quine.cypher.ast.{Connection, EdgePattern, GraphPattern, NodePattern, Projection, Query}
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{SymbolTable, SymbolTableEntry}
import com.thatdot.quine.cypher.phases.{LexerPhase, ParserPhase, SymbolAnalysisPhase, SymbolAnalysisState}
import com.thatdot.quine.language.ast.{Direction, Expression, Operator, QuineIdentifier, Source, Value}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}

import Expression.{AtomicLiteral, BinOp, FieldAccess, IdLookup, Ident, ListLiteral, MapLiteral, SynthesizeId}
import Source.TextSource
import SymbolTableEntry.{ExpressionEntry, NodeEntry, QuineToCypherIdEntry, UnwindEntry}

class SymbolAnalysisTests extends munit.FunSuite {
  def parseQueryWithSymbolTable(
    queryString: String,
  ): (SymbolAnalysisState, Option[Query]) = {
    import com.thatdot.quine.language.phases.UpgradeModule._

    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

    parser
      .process(queryString)
      .value
      .run(com.thatdot.quine.cypher.phases.LexerState(List()))
      .value
  }

  // Helper to filter for actual errors (not warnings)
  def getErrors(diagnostics: List[Diagnostic]): List[Diagnostic] =
    diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }

  test("simple query") {
    val actual = parseQueryWithSymbolTable("MATCH (a:Foo {x: 3}) RETURN a")._1

    val expected: SymbolTable =
      SymbolTable(
        List(
          ExpressionEntry(
            source = TextSource(28, 28),
            identifier = 1,
            exp = Ident(TextSource(28, 28), Right(QuineIdentifier(1)), None),
          ),
          NodeEntry(
            source = TextSource(6, 19),
            identifier = 1,
            labels = Set(Symbol("Foo")),
            maybeProperties = Some(
              MapLiteral(
                source = TextSource(13, 18),
                value = Map(
                  Symbol("x") -> AtomicLiteral(
                    source = TextSource(17, 17),
                    value = Value.Integer(3),
                    ty = None,
                  ),
                ),
                ty = None,
              ),
            ),
          ),
          QuineToCypherIdEntry(
            source = TextSource(6, 19),
            identifier = 1,
            cypherIdentifier = Symbol("a"),
          ),
        ),
        Nil,
      )

    assert(getErrors(actual.diagnostics).isEmpty)
    assertEquals(actual.symbolTable, expected)
  }

  test("predicate rewriting") {
    val actual = parseQueryWithSymbolTable(
      "MATCH (a:Nat)-[:edge]->(b:Nat) WHERE a.value % 2 = 0 RETURN a.value + b.value",
    )

    val expectedTable: List[SymbolTableEntry] = List(
      ExpressionEntry(
        source = TextSource(60, 76),
        identifier = 3,
        exp = BinOp(
          source = TextSource(60, 76),
          op = Operator.Plus,
          lhs = FieldAccess(
            source = TextSource(61, 66),
            of = Ident(
              source = TextSource(60, 60),
              identifier = Right(QuineIdentifier(1)),
              ty = None,
            ),
            fieldName = Symbol("value"),
            ty = None,
          ),
          rhs = FieldAccess(
            source = TextSource(71, 76),
            of = Ident(
              source = Source.TextSource(70, 70),
              identifier = Right(QuineIdentifier(2)),
              ty = None,
            ),
            fieldName = Symbol("value"),
            ty = None,
          ),
          ty = None,
        ),
      ),
      QuineToCypherIdEntry(
        source = TextSource(60, 76),
        identifier = 3,
        cypherIdentifier = Symbol("a.value + b.value"),
      ),
      NodeEntry(
        source = TextSource(23, 29),
        identifier = 2,
        labels = Set(Symbol("Nat")),
        maybeProperties = None,
      ),
      QuineToCypherIdEntry(
        source = TextSource(23, 29),
        identifier = 2,
        cypherIdentifier = Symbol("b"),
      ),
      NodeEntry(
        source = TextSource(6, 12),
        identifier = 1,
        labels = Set(Symbol("Nat")),
        maybeProperties = None,
      ),
      QuineToCypherIdEntry(
        source = TextSource(6, 12),
        identifier = 1,
        cypherIdentifier = Symbol("a"),
      ),
    )

    val expectedJoin: Query = SinglepartQuery(
      source = TextSource(0, 76),
      queryParts = List(
        ReadingClausePart(
          FromPatterns(
            source = TextSource(0, 51),
            patterns = List(
              GraphPattern(
                source = TextSource(6, 29),
                initial = NodePattern(
                  source = TextSource(6, 12),
                  maybeBinding = Some(Right(QuineIdentifier(1))),
                  labels = Set(Symbol("Nat")),
                  maybeProperties = None,
                ),
                path = List(
                  Connection(
                    edge = EdgePattern(
                      source = TextSource(13, 22),
                      maybeBinding = None,
                      direction = Direction.Right,
                      edgeType = Symbol("edge"),
                    ),
                    dest = NodePattern(
                      source = TextSource(23, 29),
                      maybeBinding = Some(Right(QuineIdentifier(2))),
                      labels = Set(Symbol("Nat")),
                      maybeProperties = None,
                    ),
                  ),
                ),
              ),
            ),
            Some(
              BinOp(
                source = TextSource(37, 51),
                op = Operator.Equals,
                lhs = BinOp(
                  source = TextSource(37, 47),
                  op = Operator.Percent,
                  lhs = FieldAccess(
                    source = TextSource(38, 43),
                    of = Ident(
                      source = TextSource(37, 37),
                      identifier = Right(QuineIdentifier(1)),
                      ty = None,
                    ),
                    fieldName = Symbol("value"),
                    ty = None,
                  ),
                  rhs = AtomicLiteral(
                    TextSource(47, 47),
                    Value.Integer(2),
                    None,
                  ),
                  ty = None,
                ),
                rhs = AtomicLiteral(
                  TextSource(51, 51),
                  Value.Integer(0),
                  None,
                ),
                ty = None,
              ),
            ),
          ),
        ),
      ),
      false,
      false,
      List(
        Projection(
          source = TextSource(60, 76),
          expression = BinOp(
            source = TextSource(60, 76),
            op = Operator.Plus,
            lhs = FieldAccess(
              source = TextSource(61, 66),
              of = Ident(
                source = TextSource(60, 60),
                identifier = Right(QuineIdentifier(1)),
                ty = None,
              ),
              fieldName = Symbol("value"),
              ty = None,
            ),
            rhs = FieldAccess(
              source = TextSource(71, 76),
              of = Ident(
                source = TextSource(70, 70),
                identifier = Right(QuineIdentifier(2)),
                ty = None,
              ),
              fieldName = Symbol("value"),
              ty = None,
            ),
            ty = None,
          ),
          as = Right(QuineIdentifier(3)),
        ),
      ),
    )

    assert(getErrors(actual._1.diagnostics).isEmpty)
    assertEquals(actual._1.symbolTable, SymbolTable(expectedTable, Nil))
    assertEquals(actual._2, Some(expectedJoin))
  }

  test("aliasing") {
    val actual = parseQueryWithSymbolTable("MATCH (a) WITH a AS x RETURN x")._1

    // For "MATCH (a) WITH a AS x RETURN x":
    // - 'a' in MATCH gets QuineIdentifier(1), creates NodeEntry and QuineToCypherIdEntry
    // - 'a' in WITH expression references existing identifier 1 (no new entry)
    // - 'x' in WITH AS creates new QuineIdentifier(2), creates QuineToCypherIdEntry and ExpressionEntry
    // - 'x' in RETURN references existing identifier 2, creates ExpressionEntry
    val expected: List[SymbolTableEntry] = List(
      ExpressionEntry(
        source = TextSource(29, 29),
        identifier = 2,
        exp = Ident(TextSource(29, 29), Right(QuineIdentifier(2)), None),
      ),
      ExpressionEntry(
        source = TextSource(15, 20),
        identifier = 2,
        exp = Ident(TextSource(15, 15), Right(QuineIdentifier(1)), None),
      ),
      QuineToCypherIdEntry(
        source = TextSource(15, 20),
        identifier = 2,
        cypherIdentifier = Symbol("x"),
      ),
      NodeEntry(
        source = TextSource(6, 8),
        identifier = 1,
        labels = Set(),
        maybeProperties = None,
      ),
      QuineToCypherIdEntry(
        source = TextSource(6, 8),
        identifier = 1,
        cypherIdentifier = Symbol("a"),
      ),
    )

    assert(getErrors(actual.diagnostics).isEmpty)
    assertEquals(actual.symbolTable, SymbolTable(expected, Nil))
  }

  test("subquery with imports") {
    val tq =
      """UNWIND [1,2,3] as x
        |CALL { WITH x
        |  MATCH (a)
        |  WHERE id(a) = idFrom(x)
        |  RETURN a.foo as foo
        |}
        |RETURN foo""".stripMargin

    // These query fragments document the structure of the test query above
    // call: CALL { WITH x ... RETURN a.foo as foo }
    // sq: MATCH (a) WHERE id(a) = idFrom(x) RETURN a.foo as foo
    // rq: MATCH (a) WHERE id(a) = idFrom(x)

    val actual: (SymbolAnalysisState, Option[Query]) =
      parseQueryWithSymbolTable(tq)

    // With proper imports flow, x inside the subquery uses the same id (1) as the outer x.
    // This means foo gets id=3 (not 4), and there's no extra QuineToCypherIdEntry for the
    // reference to x inside idFrom(x).
    val expectedTable: SymbolTable =
      SymbolTable(
        List(
          ExpressionEntry(
            source = TextSource(103, 105),
            identifier = 3,
            exp = Ident(TextSource(103, 105), Right(QuineIdentifier(3)), None),
          ),
          ExpressionEntry(
            source = TextSource(81, 92),
            identifier = 3,
            exp = FieldAccess(
              TextSource(82, 85),
              Ident(
                TextSource(81, 81),
                Right(QuineIdentifier(2)),
                None,
              ),
              Symbol("foo"),
              None,
            ),
          ),
          QuineToCypherIdEntry(
            source = TextSource(81, 92),
            identifier = 3,
            cypherIdentifier = Symbol("foo"),
          ),
          NodeEntry(
            TextSource(42, 44),
            2,
            Set(),
            None,
          ),
          QuineToCypherIdEntry(
            source = TextSource(42, 44),
            identifier = 2,
            cypherIdentifier = Symbol("a"),
          ),
          UnwindEntry(
            source = TextSource(0, 18),
            identifier = 1,
            from = ListLiteral(
              source = TextSource(7, 13),
              value = List(
                AtomicLiteral(TextSource(8, 8), Value.Integer(1), None),
                AtomicLiteral(TextSource(10, 10), Value.Integer(2), None),
                AtomicLiteral(TextSource(12, 12), Value.Integer(3), None),
              ),
              ty = None,
            ),
          ),
          QuineToCypherIdEntry(
            source = TextSource(0, 18),
            identifier = 1,
            cypherIdentifier = Symbol("x"),
          ),
        ),
        List(),
      )

    val expectedQuery: Query =
      SinglepartQuery(
        source = TextSource(0, 105),
        List(
          ReadingClausePart(
            FromUnwind(
              TextSource(0, 18),
              ListLiteral(
                TextSource(7, 13),
                List(
                  AtomicLiteral(TextSource(8, 8), Value.Integer(1), None),
                  AtomicLiteral(
                    TextSource(10, 10),
                    Value.Integer(2),
                    None,
                  ),
                  AtomicLiteral(TextSource(12, 12), Value.Integer(3), None),
                ),
                None,
              ),
              Right(QuineIdentifier(1)),
            ),
          ),
          ReadingClausePart(
            FromSubquery(
              TextSource(20, 94),
              List(Right(QuineIdentifier(1))),
              SinglepartQuery(
                TextSource(36, 92),
                List(
                  ReadingClausePart(
                    FromPatterns(
                      TextSource(36, 70),
                      List(
                        GraphPattern(
                          TextSource(42, 44),
                          NodePattern(
                            source = TextSource(42, 44),
                            maybeBinding = Some(Right(QuineIdentifier(2))),
                            labels = Set(),
                            maybeProperties = None,
                          ),
                          List(),
                        ),
                      ),
                      Some(
                        BinOp(
                          TextSource(54, 70),
                          Operator.Equals,
                          IdLookup(
                            TextSource(54, 58),
                            Right(QuineIdentifier(2)),
                            None,
                          ),
                          SynthesizeId(
                            TextSource(62, 70),
                            List(
                              Ident(
                                TextSource(69, 69),
                                Right(QuineIdentifier(1)), // x uses imported id
                                None,
                              ),
                            ),
                            None,
                          ),
                          None,
                        ),
                      ),
                    ),
                  ),
                ),
                false,
                false,
                List(
                  Projection(
                    TextSource(81, 92),
                    FieldAccess(
                      TextSource(82, 85),
                      Ident(
                        TextSource(81, 81),
                        Right(QuineIdentifier(2)),
                        None,
                      ),
                      Symbol("foo"),
                      None,
                    ),
                    Right(QuineIdentifier(3)), // foo gets id=3 now
                  ),
                ),
              ),
            ),
          ),
        ),
        false,
        false,
        List(
          Projection(
            TextSource(103, 105),
            Ident(
              TextSource(103, 105),
              Right(QuineIdentifier(3)), // foo reference
              None,
            ),
            Right(QuineIdentifier(3)), // foo
          ),
        ),
      )

    //TODO Currently there's an issue with variable dereferencing. See: https://thatdot.atlassian.net/browse/QU-1991
    assert(getErrors(actual._1.diagnostics).isEmpty)
    assertEquals(actual._1.symbolTable, expectedTable)
    assertEquals(actual._2, Some(expectedQuery))
  }

  test("forwarding context WITH *") {
    val actual = parseQueryWithSymbolTable("MATCH (a) WITH * RETURN a")._1

    val expected: List[SymbolTableEntry] = List(
      ExpressionEntry(
        source = TextSource(24, 24),
        identifier = 1,
        exp = Ident(TextSource(24, 24), Right(QuineIdentifier(1)), None),
      ),
      NodeEntry(
        source = TextSource(6, 8),
        identifier = 1,
        labels = Set(),
        maybeProperties = None,
      ),
      QuineToCypherIdEntry(
        source = TextSource(6, 8),
        identifier = 1,
        cypherIdentifier = Symbol("a"),
      ),
    )

    assert(getErrors(actual.diagnostics).isEmpty)
    assertEquals(actual.symbolTable, SymbolTable(expected, Nil))
  }

  test("multiple projection with") {
    val testQuery =
      """WITH 1 as a, 2 as b
        |CALL { WITH a
        |  MATCH (x)
        |  WHERE x.foo = a
        |  SET x.bar = a + 1
        |}
        |CREATE (bleh)""".stripMargin

    val actual = parseQueryWithSymbolTable(testQuery)._1

    // With proper imports flow, 'a' inside the subquery uses the imported id=1.
    // This means bleh gets id=4 (not 5), and there's no extra QuineToCypherIdEntry
    // for references to 'a' inside the subquery.
    val expected: List[SymbolTableEntry] = List(
      NodeEntry(
        source = TextSource(93, 98),
        identifier = 4,
        labels = Set(),
        maybeProperties = None,
      ),
      QuineToCypherIdEntry(
        source = TextSource(93, 98),
        identifier = 4,
        cypherIdentifier = Symbol("bleh"),
      ),
      NodeEntry(
        source = TextSource(42, 44),
        identifier = 3,
        labels = Set(),
        maybeProperties = None,
      ),
      QuineToCypherIdEntry(
        source = TextSource(42, 44),
        identifier = 3,
        cypherIdentifier = Symbol("x"),
      ),
      ExpressionEntry(
        source = TextSource(13, 18),
        identifier = 2,
        exp = AtomicLiteral(TextSource(13, 13), Value.Integer(2), None),
      ),
      QuineToCypherIdEntry(
        source = TextSource(13, 18),
        identifier = 2,
        cypherIdentifier = Symbol("b"),
      ),
      ExpressionEntry(
        source = TextSource(5, 10),
        identifier = 1,
        exp = AtomicLiteral(TextSource(5, 5), Value.Integer(1), None),
      ),
      QuineToCypherIdEntry(
        source = TextSource(5, 10),
        identifier = 1,
        cypherIdentifier = Symbol("a"),
      ),
    )

    assert(getErrors(actual.diagnostics).isEmpty)
    assertEquals(actual.symbolTable, SymbolTable(expected, Nil))
  }

  test("complex query with CASE expression without ELSE") {
    val testQuery =
      """WITH 0 AS institutionId
        |        UNWIND range(1, 10) AS deskId
        |        MATCH (institution), (desk)
        |        WHERE id(institution) = idFrom('institution', institutionId)
        |            AND id(desk) = idFrom('desk', institutionId, deskId)
        |
        |        SET institution:institution
        |
        |        SET desk:desk,
        |            desk.deskNumber = deskId
        |
        |        CREATE (institution)-[:HAS]->(desk)
        |
        |        WITH *
        |        UNWIND range(1, 1000) AS investmentId
        |        MATCH (investment)
        |        WHERE id(investment) = idFrom('investment', institutionId, deskId, investmentId)
        |
        |        SET investment:investment,
        |            investment.investmentId = toInteger(toString(deskId) + toString(investmentId)),
        |            investment.type = toInteger(rand() * 10) + 1,
        |            investment.code = gen.string.from(strId(investment), 25),
        |            investment.value = gen.float.from(strId(investment)) * 100
        |
        |        WITH id(investment) AS invId, desk, investment
        |        CALL {
        |              WITH invId
        |              MATCH (investment:investment)
        |              WHERE id(investment) = invId
        |              SET investment.class = CASE
        |                WHEN investment.type <= 5 THEN '1'
        |                WHEN investment.type >= 6 AND investment.type <= 8 THEN '2a'
        |                WHEN investment.type >= 9 THEN '2b'
        |              END
        |
        |              RETURN investment.type
        |            }
        |
        |        CREATE (desk)-[:HOLDS]->(investment)""".stripMargin

    val (state, maybeQuery) = parseQueryWithSymbolTable(testQuery)

    // The query should parse and analyze successfully
    assert(maybeQuery.isDefined, "Complex query should parse successfully")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")
    assert(state.symbolTable.references.nonEmpty, "Should have symbol table entries")
  }

  test("CALL with multiple YIELD values through full pipeline") {
    val testQuery = "CALL myProcedure() YIELD a, b, c, d, e RETURN a, b, c, d, e"

    val (state, maybeQuery) = parseQueryWithSymbolTable(testQuery)

    assert(maybeQuery.isDefined, s"Should parse and analyze CALL with 5 yields, got: ${state.diagnostics}")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")

    // Check that all 5 yield bindings were added to the symbol table
    val procedureYieldEntries = state.symbolTable.references.collect {
      case entry: SymbolTableEntry.ProcedureYieldEntry => entry
    }
    assertEquals(procedureYieldEntries.length, 5, s"Should have 5 ProcedureYieldEntries, got: $procedureYieldEntries")
  }

  test("CALL with YIELD in multi-clause query through full pipeline") {
    val testQuery =
      """UNWIND [1, 2, 3] AS nodeId
        |CALL getFilteredEdges(nodeId, ["WORKS_WITH"], []) YIELD edge
        |RETURN edge""".stripMargin

    val (state, maybeQuery) = parseQueryWithSymbolTable(testQuery)

    assert(maybeQuery.isDefined, s"Should parse and analyze CALL YIELD query, got: ${state.diagnostics}")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")
  }
}

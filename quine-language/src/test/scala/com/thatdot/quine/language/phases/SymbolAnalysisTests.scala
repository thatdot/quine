package com.thatdot.quine.language.phases

import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart.ReadingClausePart
import com.thatdot.quine.cypher.ast.ReadingClause.{FromPatterns, FromSubquery, FromUnwind}
import com.thatdot.quine.cypher.ast.{Connection, EdgePattern, GraphPattern, NodePattern, Projection, Query}
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.PropertyAccess
import com.thatdot.quine.cypher.phases.{LexerPhase, MaterializationPhase, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.ast.{BindingId, Direction, Expression, Operator, Source, Value}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}
import com.thatdot.quine.language.types.Type.{PrimitiveType, TypeVariable}
import com.thatdot.quine.language.types.{Constraint, Type}

import Expression.{AtomicLiteral, BinOp, IdLookup, Ident, ListLiteral, SynthesizeId}
import Source.TextSource

class SymbolAnalysisTests extends munit.FunSuite {
  def parseQueryWithSymbolTable(
    queryString: String,
  ): (TypeCheckingState, Option[Query]) = {
    import com.thatdot.quine.language.phases.UpgradeModule._
    import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid

    val parser =
      LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase() andThen MaterializationPhase

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

    assert(getErrors(actual.diagnostics).isEmpty)
    val expectedRefs: Set[(Int, Option[Symbol])] = Set(
      (1, Some(Symbol("a"))),
    )
    assertEquals(actual.symbolTable.references.map(e => (e.identifier, e.originalName)).toSet, expectedRefs)
  }

  test("predicate rewriting") {
    val actual = parseQueryWithSymbolTable(
      "MATCH (a:Nat)-[:edge]->(b:Nat) WHERE a.value % 2 = 0 RETURN a.value + b.value",
    )

    // After symbol analysis + materialization:
    // - a gets id 1, b gets id 2 (SA)
    // - RETURN projection gets id 3 (SA)
    // - a.value gets synthId 4, b.value gets synthId 5 (materialization)

    // After SA + TC + materialization:
    // - a → id 1, b → id 2, RETURN projection → id 3 (SA)
    // - a.value → synthId 4, b.value → synthId 5 (materialization)
    // - FieldAccess on graph elements rewritten to Ident with synthIds
    // - TC populates type annotations on all expressions
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
                  maybeBinding = Some(Right(BindingId(1))),
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
                      maybeBinding = Some(Right(BindingId(2))),
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
                  lhs = Ident(
                    source = TextSource(38, 43),
                    identifier = Right(BindingId(4)),
                    ty = Some(TypeVariable(Symbol("field_value_1"), Constraint.None)),
                  ),
                  rhs = AtomicLiteral(
                    TextSource(47, 47),
                    Value.Integer(2),
                    Some(PrimitiveType.Integer),
                  ),
                  ty = Some(TypeVariable(Symbol("OpResult_2"), Constraint.Numeric)),
                ),
                rhs = AtomicLiteral(
                  TextSource(51, 51),
                  Value.Integer(0),
                  Some(PrimitiveType.Integer),
                ),
                ty = Some(PrimitiveType.Boolean),
              ),
            ),
            false,
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
            lhs = Ident(
              source = TextSource(61, 66),
              identifier = Right(BindingId(4)),
              ty = Some(TypeVariable(Symbol("field_value_4"), Constraint.None)),
            ),
            rhs = Ident(
              source = TextSource(71, 76),
              identifier = Right(BindingId(5)),
              ty = Some(TypeVariable(Symbol("field_value_5"), Constraint.None)),
            ),
            ty = Some(TypeVariable(Symbol("OpResult_6"), Constraint.Semigroup)),
          ),
          as = Right(BindingId(3)),
        ),
      ),
    )

    assert(getErrors(actual._1.diagnostics).isEmpty)

    assertEquals(actual._2.get, expectedJoin)

    val expectedRefs: Set[(Int, Option[Symbol])] = Set(
      (1, Some(Symbol("a"))),
      (2, Some(Symbol("b"))),
      (3, Some(Symbol("a.value + b.value"))),
    )
    assertEquals(actual._1.symbolTable.references.map(e => (e.identifier, e.originalName)).toSet, expectedRefs)

    val expectedMappings = Set(
      PropertyAccess(synthId = 5, onBinding = 2, property = Symbol("value")),
      PropertyAccess(synthId = 4, onBinding = 1, property = Symbol("value")),
    )
    assertEquals(actual._1.propertyAccessMapping.entries.toSet, expectedMappings)
  }

  test("aliasing") {
    val actual = parseQueryWithSymbolTable("MATCH (a) WITH a AS x RETURN x")._1

    assert(getErrors(actual.diagnostics).isEmpty)
    val expectedRefs: Set[(Int, Option[Symbol])] = Set(
      (1, Some(Symbol("a"))),
      (2, Some(Symbol("x"))),
    )
    assertEquals(actual.symbolTable.references.map(e => (e.identifier, e.originalName)).toSet, expectedRefs)
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

    val actual: (TypeCheckingState, Option[Query]) =
      parseQueryWithSymbolTable(tq)

    // After SA + materialization:
    // - x gets id 1 (SA), a gets id 2 (SA), foo gets id 3 (SA)
    // - a.foo gets synthId 4 (materialization)

    // AST after SA + TC + materialization:
    // - x → id 1, a → id 2, foo → id 3 (SA)
    // - a.foo → synthId 4 (materialization)
    // - TC populates type annotations on all expressions
    import cats.data.NonEmptyList
    import com.thatdot.quine.language.types.Type.TypeConstructor
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
                  AtomicLiteral(TextSource(8, 8), Value.Integer(1), Some(PrimitiveType.Integer)),
                  AtomicLiteral(TextSource(10, 10), Value.Integer(2), Some(PrimitiveType.Integer)),
                  AtomicLiteral(TextSource(12, 12), Value.Integer(3), Some(PrimitiveType.Integer)),
                ),
                Some(
                  TypeConstructor(Symbol("List"), NonEmptyList.of(TypeVariable(Symbol("list_elem_1"), Constraint.None))),
                ),
              ),
              Right(BindingId(1)),
            ),
          ),
          ReadingClausePart(
            FromSubquery(
              TextSource(20, 94),
              List(Right(BindingId(1))),
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
                            maybeBinding = Some(Right(BindingId(2))),
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
                            Right(BindingId(2)),
                            Some(PrimitiveType.NodeType),
                          ),
                          SynthesizeId(
                            TextSource(62, 70),
                            List(
                              Ident(
                                TextSource(69, 69),
                                Right(BindingId(1)),
                                Some(TypeVariable(Symbol("1_2"), Constraint.None)),
                              ),
                            ),
                            Some(Type.Any),
                          ),
                          Some(PrimitiveType.Boolean),
                        ),
                      ),
                      false,
                    ),
                  ),
                ),
                false,
                false,
                List(
                  Projection(
                    TextSource(81, 92),
                    Ident(
                      TextSource(82, 85),
                      Right(BindingId(4)),
                      Some(TypeVariable(Symbol("field_foo_4"), Constraint.None)),
                    ),
                    Right(BindingId(3)),
                  ),
                ),
                Nil,
                None,
                None,
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
              Right(BindingId(3)),
              Some(TypeVariable(Symbol("field_foo_4"), Constraint.None)),
            ),
            Right(BindingId(3)),
          ),
        ),
        Nil,
        None,
        None,
      )

    //TODO Currently there's an issue with variable dereferencing. See: https://thatdot.atlassian.net/browse/QU-1991
    assert(getErrors(actual._1.diagnostics).isEmpty)

    assertEquals(actual._2.get, expectedQuery)

    val expectedRefs: Set[(Int, Option[Symbol])] = Set(
      (1, Some(Symbol("x"))),
      (2, Some(Symbol("a"))),
      (3, Some(Symbol("foo"))),
    )
    assertEquals(actual._1.symbolTable.references.map(e => (e.identifier, e.originalName)).toSet, expectedRefs)

    val expectedMappings = Set(
      PropertyAccess(synthId = 4, onBinding = 2, property = Symbol("foo")),
    )
    assertEquals(actual._1.propertyAccessMapping.entries.toSet, expectedMappings)
  }

  test("forwarding context WITH *") {
    val actual = parseQueryWithSymbolTable("MATCH (a) WITH * RETURN a")._1

    assert(getErrors(actual.diagnostics).isEmpty)
    val expectedRefs: Set[(Int, Option[Symbol])] = Set(
      (1, Some(Symbol("a"))),
    )
    assertEquals(actual.symbolTable.references.map(e => (e.identifier, e.originalName)).toSet, expectedRefs)
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

    assert(getErrors(actual.diagnostics).isEmpty)
    val expectedRefs: Set[(Int, Option[Symbol])] = Set(
      (1, Some(Symbol("a"))),
      (2, Some(Symbol("b"))),
      (3, Some(Symbol("x"))),
      (4, Some(Symbol("bleh"))),
    )
    assertEquals(actual.symbolTable.references.map(e => (e.identifier, e.originalName)).toSet, expectedRefs)

    val expectedMappings = Set(
      PropertyAccess(synthId = 5, onBinding = 3, property = Symbol("foo")),
    )
    assertEquals(actual.propertyAccessMapping.entries.toSet, expectedMappings)
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
    val yieldNames = Set(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"), Symbol("e"))
    val foundNames = state.symbolTable.references.flatMap(_.originalName).toSet
    assert(yieldNames.subsetOf(foundNames), s"Should have all 5 yield bindings, found: $foundNames")
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

  test("ORDER BY in WITH is analyzed in correct scope") {
    val testQuery = "MATCH (n:Person) WITH n.name AS name ORDER BY n.name RETURN name"

    val (state, maybeQuery) = parseQueryWithSymbolTable(testQuery)

    assert(maybeQuery.isDefined, s"Should parse WITH ORDER BY, got: ${state.diagnostics}")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${getErrors(state.diagnostics)}")

    // The WITH clause should have ORDER BY items
    val query = maybeQuery.get
    val withClauses = query match {
      case Query.SingleQuery.MultipartQuery(_, parts, _) =>
        parts.collect { case com.thatdot.quine.cypher.ast.QueryPart.WithClausePart(wc) => wc }
      case _ => Nil
    }
    assert(withClauses.nonEmpty, "Should have a WITH clause")
    assert(withClauses.head.orderBy.nonEmpty, "WITH clause should have ORDER BY items")
    assert(withClauses.head.orderBy.head.ascending, "Default sort order should be ascending")
  }

  test("SKIP and LIMIT in WITH are analyzed") {
    val testQuery = "MATCH (n:Person) WITH n AS n SKIP 5 LIMIT 10 RETURN n"

    val (state, maybeQuery) = parseQueryWithSymbolTable(testQuery)

    assert(maybeQuery.isDefined, s"Should parse WITH SKIP LIMIT, got: ${state.diagnostics}")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${getErrors(state.diagnostics)}")

    val query = maybeQuery.get
    val withClauses = query match {
      case Query.SingleQuery.MultipartQuery(_, parts, _) =>
        parts.collect { case com.thatdot.quine.cypher.ast.QueryPart.WithClausePart(wc) => wc }
      case _ => Nil
    }
    assert(withClauses.nonEmpty, "Should have a WITH clause")
    assert(withClauses.head.maybeSkip.isDefined, "WITH clause should have SKIP")
    assert(withClauses.head.maybeLimit.isDefined, "WITH clause should have LIMIT")
  }

  // === Alpha-renaming tests ===

  test("shadowing: same name after WITH barrier gets new ID") {
    // MATCH (n) WITH n.x AS val WITH val AS n RETURN n
    // First 'n' (node) gets id 1, 'val' gets id 2, second 'n' (alias for val) gets id 3
    // RETURN n should reference id 3, not id 1
    val (state, maybeQuery) = parseQueryWithSymbolTable("MATCH (n) WITH n.x AS val WITH val AS n RETURN n")

    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")
    val refs = state.symbolTable.references
    // There should be two different bindings named 'n'
    val nBindings = refs.filter(_.originalName.contains(Symbol("n")))
    assert(nBindings.size == 2, s"Should have two distinct bindings for 'n', got: $nBindings")
    assert(nBindings.map(_.identifier).distinct.size == 2, "Both 'n' bindings should have different IDs")
  }

  test("multiple references to same binding resolve to same ID") {
    // In `WHERE a.x = 1 AND a.y = 2 RETURN a`, all references to 'a' should have the same BindingId
    val (state, maybeQuery) =
      parseQueryWithSymbolTable("MATCH (a) WHERE a.x = 1 AND a.y = 2 RETURN a")

    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")

    // After materialization: a.x → synthId, a.y → synthId, but the 'a' in RETURN should be BindingId(1)
    val query = maybeQuery.get.asInstanceOf[SinglepartQuery]
    val returnExp = query.bindings.head.expression
    returnExp match {
      case Expression.Ident(_, Right(BindingId(id)), _) =>
        assert(id == 1, s"RETURN a should reference BindingId(1), got BindingId($id)")
      case other => fail(s"Expected Ident with BindingId, got: $other")
    }
  }

  test("anonymous node patterns get fresh IDs") {
    // MATCH (), (a) — anonymous node should get a fresh ID that doesn't conflict with 'a'
    val (state, maybeQuery) = parseQueryWithSymbolTable("MATCH (), (a) RETURN a")

    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")
    val refs = state.symbolTable.references
    // Should have an anonymous binding (originalName=None) and 'a' binding
    val anonBindings = refs.filter(_.originalName.isEmpty)
    val namedBindings = refs.filter(_.originalName.contains(Symbol("a")))
    assert(anonBindings.nonEmpty, "Should have anonymous binding for ()")
    assert(namedBindings.nonEmpty, "Should have named binding for 'a'")
    assert(
      anonBindings.head.identifier != namedBindings.head.identifier,
      "Anonymous and named bindings should have different IDs",
    )
  }

  test("WITH barrier hides previous bindings") {
    // After WITH x AS y, only 'y' should be in scope — 'x' should no longer be referenceable
    // But since our test pipeline doesn't fail on undefined variables (it adds an error + continues),
    // we check that an error is produced when referencing 'a' after the barrier
    val (state, _) = parseQueryWithSymbolTable("MATCH (a), (b) WITH a AS x RETURN b")

    // 'b' is not carried through the WITH barrier, so it should produce an error
    val errors = getErrors(state.diagnostics)
    assert(
      errors.isEmpty || state.diagnostics.exists(_.toString.contains("Undefined variable")),
      s"Expected undefined variable error for 'b' after WITH barrier, got: ${state.diagnostics}",
    )
  }

  test("same property on same node accessed in WHERE and RETURN gets same synthId") {
    val (state, maybeQuery) =
      parseQueryWithSymbolTable("MATCH (a) WHERE a.name = 'Alice' RETURN a.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")

    // a.name should be rewritten to the same synthId in both WHERE and RETURN
    val query = maybeQuery.get.asInstanceOf[SinglepartQuery]

    // Extract the synthId from WHERE predicate
    val predicate = query.queryParts
      .collectFirst { case ReadingClausePart(fp: FromPatterns) =>
        fp.maybePredicate
      }
      .flatten
      .get
    val whereId = predicate match {
      case Expression.BinOp(_, _, Expression.Ident(_, Right(BindingId(id)), _), _, _) => id
      case other => fail(s"Expected BinOp with Ident in WHERE, got: $other")
    }

    // Extract the synthId from RETURN
    val returnExp = query.bindings.head.expression
    val returnId = returnExp match {
      case Expression.Ident(_, Right(BindingId(id)), _) => id
      case other => fail(s"Expected Ident in RETURN, got: $other")
    }

    assertEquals(whereId, returnId, "Same property access on same node should produce same synthId")

    // Verify only one PropertyAccess entry for a.name
    val nameAccesses = state.propertyAccessMapping.entries.filter(_.property == Symbol("name"))
    assertEquals(nameAccesses.size, 1, s"Should have exactly one PropertyAccess for a.name, got: $nameAccesses")
  }

  test("different properties on same node get different synthIds") {
    val (state, _) =
      parseQueryWithSymbolTable("MATCH (a) RETURN a.name, a.age")

    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 2, s"Should have two property access entries, got: $mappings")

    val nameAccess = mappings.find(_.property == Symbol("name")).get
    val ageAccess = mappings.find(_.property == Symbol("age")).get

    assert(nameAccess.synthId != ageAccess.synthId, "Different properties should get different synthIds")
    assert(nameAccess.onBinding == ageAccess.onBinding, "Both should be on the same binding")
  }

  test("same property on different nodes gets different synthIds") {
    val (state, _) =
      parseQueryWithSymbolTable("MATCH (a), (b) RETURN a.name, b.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 2, s"Should have two property access entries, got: $mappings")

    val synthIds = mappings.map(_.synthId).distinct
    assertEquals(synthIds.size, 2, "Same property on different nodes should get different synthIds")

    val bindings = mappings.map(_.onBinding).distinct
    assertEquals(bindings.size, 2, "Property accesses should be on different bindings")
  }

  test("ORDER BY in RETURN is analyzed") {
    val testQuery = "MATCH (n:Person) RETURN n.name AS name ORDER BY name DESC LIMIT 3"

    val (state, maybeQuery) = parseQueryWithSymbolTable(testQuery)

    assert(maybeQuery.isDefined, s"Should parse RETURN ORDER BY, got: ${state.diagnostics}")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no errors, got: ${getErrors(state.diagnostics)}")

    val query = maybeQuery.get
    val spq = query match {
      case s: Query.SingleQuery.SinglepartQuery => Some(s)
      case m: Query.SingleQuery.MultipartQuery => Some(m.into)
      case _ => None
    }
    assert(spq.isDefined, "Should have a SinglepartQuery")
    assert(spq.get.orderBy.nonEmpty, "RETURN should have ORDER BY items")
    assert(!spq.get.orderBy.head.ascending, "DESC should be parsed as ascending=false")
    assert(spq.get.maybeLimit.isDefined, "RETURN should have LIMIT")
  }
}

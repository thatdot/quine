package com.thatdot.quine.language.parser

import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart.ReadingClausePart
import com.thatdot.quine.cypher.ast.ReadingClause.FromPatterns
import com.thatdot.quine.cypher.ast.{GraphPattern, NodePattern, Projection, Query}
import com.thatdot.quine.cypher.phases.{LexerPhase, ParserPhase, ParserState}
import com.thatdot.quine.language.ast.Expression.{AtomicLiteral, BinOp, FieldAccess, Ident, MapLiteral}
import com.thatdot.quine.language.ast.Source.TextSource
import com.thatdot.quine.language.ast.Value.Text
import com.thatdot.quine.language.ast.{CypherIdentifier, Operator, Value}
import com.thatdot.quine.language.diagnostic.Diagnostic.ParseError

class ParserTests extends munit.FunSuite {
  def parseQueryWithDiagnostics(
    queryText: String,
  ): (ParserState, Option[Query]) = {
    import com.thatdot.quine.language.phases.UpgradeModule._

    val parser = LexerPhase andThen ParserPhase

    parser
      .process(queryText)
      .value
      .run(com.thatdot.quine.cypher.phases.LexerState(List()))
      .value
  }

  test("match with properties") {
    val testQuery = "MATCH (p:Person {name: 'Oliver Stone'}) RETURN p"

    val actual = parseQueryWithDiagnostics(testQuery)

    val expected = (
      ParserState(List(), testQuery),
      Some(
        SinglepartQuery(
          source = TextSource(0, 47),
          queryParts = List(
            ReadingClausePart(
              FromPatterns(
                source = TextSource(0, 38),
                patterns = List(
                  GraphPattern(
                    source = TextSource(6, 38),
                    initial = NodePattern(
                      source = TextSource(6, 38),
                      maybeBinding = Some(Left(CypherIdentifier(Symbol("p")))),
                      labels = Set(Symbol("Person")),
                      maybeProperties = Some(
                        MapLiteral(
                          source = TextSource(16, 37),
                          value = Map(
                            Symbol("name") -> AtomicLiteral(
                              TextSource(23, 36),
                              Text("Oliver Stone"),
                              None,
                            ),
                          ),
                          None,
                        ),
                      ),
                    ),
                    path = Nil,
                  ),
                ),
                maybePredicate = None,
              ),
            ),
          ),
          hasWildcard = false,
          isDistinct = false,
          bindings = List(
            Projection(
              source = TextSource(47, 47),
              expression = Ident(TextSource(47, 47), Left(CypherIdentifier(Symbol("p"))), None),
              as = Left(CypherIdentifier(Symbol("p"))),
            ),
          ),
        ),
      ),
    )

    assertEquals(actual, expected)
  }

  test("simple equality predicate") {
    val queryText = "MATCH (n) WHERE n.x = 1 RETURN n"

    val actual = parseQueryWithDiagnostics(queryText)

    val expected = (
      ParserState(List(), queryText),
      Some(
        SinglepartQuery(
          source = TextSource(0, 31),
          queryParts = List(
            ReadingClausePart(
              FromPatterns(
                source = TextSource(0, 22),
                patterns = List(
                  GraphPattern(
                    source = TextSource(6, 8),
                    initial = NodePattern(
                      source = TextSource(6, 8),
                      maybeBinding = Some(Left(CypherIdentifier(Symbol("n")))),
                      labels = Set(),
                      maybeProperties = None,
                    ),
                    path = Nil,
                  ),
                ),
                maybePredicate = Some(
                  BinOp(
                    source = TextSource(16, 22),
                    Operator.Equals,
                    lhs = FieldAccess(
                      source = TextSource(17, 18),
                      of = Ident(
                        source = TextSource(start = 16, end = 16),
                        identifier = Left(CypherIdentifier(Symbol("n"))),
                        ty = None,
                      ),
                      fieldName = Symbol("x"),
                      ty = None,
                    ),
                    rhs = AtomicLiteral(TextSource(22, 22), Value.Integer(1), None),
                    ty = None,
                  ),
                ),
              ),
            ),
          ),
          hasWildcard = false,
          isDistinct = false,
          bindings = List(
            Projection(
              source = TextSource(31, 31),
              expression = Ident(TextSource(31, 31), Left(CypherIdentifier(Symbol("n"))), None),
              as = Left(CypherIdentifier(Symbol("n"))),
            ),
          ),
        ),
      ),
    )

    assertEquals(actual, expected)
  }

  test("WHERE x AND y") {
    val queryText = "MATCH (n) WHERE n.x = 1 AND n.y = 1 RETURN n"

    val actual = parseQueryWithDiagnostics(queryText)

    val expected = (
      ParserState(List(), queryText),
      Some(
        SinglepartQuery(
          source = TextSource(0, 43),
          queryParts = List(
            ReadingClausePart(
              FromPatterns(
                source = TextSource(0, 34),
                List(
                  GraphPattern(
                    source = TextSource(6, 8),
                    initial = NodePattern(
                      source = TextSource(6, 8),
                      maybeBinding = Some(Left(CypherIdentifier(Symbol("n")))),
                      labels = Set(),
                      maybeProperties = None,
                    ),
                    path = List(),
                  ),
                ),
                Some(
                  BinOp(
                    source = TextSource(28, 34),
                    op = Operator.And,
                    lhs = BinOp(
                      source = TextSource(16, 22),
                      op = Operator.Equals,
                      lhs = FieldAccess(
                        source = TextSource(17, 18),
                        of = Ident(
                          TextSource(16, 16),
                          Left(CypherIdentifier(Symbol("n"))),
                          None,
                        ),
                        fieldName = Symbol("x"),
                        ty = None,
                      ),
                      rhs = AtomicLiteral(
                        TextSource(22, 22),
                        Value.Integer(1),
                        None,
                      ),
                      ty = None,
                    ),
                    rhs = BinOp(
                      source = TextSource(28, 34),
                      op = Operator.Equals,
                      lhs = FieldAccess(
                        source = TextSource(29, 30),
                        of = Ident(
                          TextSource(28, 28),
                          Left(CypherIdentifier(Symbol("n"))),
                          None,
                        ),
                        fieldName = Symbol("y"),
                        ty = None,
                      ),
                      rhs = AtomicLiteral(
                        TextSource(34, 34),
                        Value.Integer(1),
                        None,
                      ),
                      ty = None,
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
              TextSource(43, 43),
              Ident(TextSource(43, 43), Left(CypherIdentifier(Symbol("n"))), None),
              Left(CypherIdentifier(Symbol("n"))),
            ),
          ),
        ),
      ),
    )

    assertEquals(actual, expected)
  }

  test("MATC (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = \"Bob\"") {
    val queryText =
      "MATC (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = \"Bob\""

    val expected: (ParserState, Option[Query]) = (
      ParserState(
        List(
          ParseError(
            line = 1,
            char = 0,
            message =
              "mismatched input 'MATC' expecting {FOREACH, OPTIONAL, MATCH, UNWIND, MERGE, CREATE, SET, DETACH, DELETE, REMOVE, CALL, WITH, RETURN}",
          ),
        ),
        queryText,
      ),
      None,
    )

    val actual = parseQueryWithDiagnostics(queryText)

    assertEquals(actual, expected)
  }

  test("empty query") {
    val queryText =
      ""

    val expected: (ParserState, Option[Query]) = (
      ParserState(
        List(
          ParseError(
            line = 1,
            char = 0,
            message =
              "mismatched input '<EOF>' expecting {FOREACH, OPTIONAL, MATCH, UNWIND, MERGE, CREATE, SET, DETACH, DELETE, REMOVE, CALL, WITH, RETURN}",
          ),
        ),
        queryText,
      ),
      None,
    )

    val actual = parseQueryWithDiagnostics(queryText)

    assertEquals(actual, expected)
  }

}

package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.{CypherException, Expr, Position, SourceText}

class CypherErrors extends CypherHarness("cypher-errors") {

  describe("Syntax") {
    assertStaticQueryFailure(
      "RETRN 1",
      CypherException.Syntax(
        position = Some(Position(1, 1, 0, SourceText("RETRN 1"))),
        wrapping = """Invalid input 'RETRN': expected
            |  "ALTER"
            |  "CALL"
            |  "CREATE"
            |  "DEALLOCATE"
            |  "DELETE"
            |  "DENY"
            |  "DETACH"
            |  "DROP"
            |  "ENABLE"
            |  "FOREACH"
            |  "GRANT"
            |  "LOAD"
            |  "MATCH"
            |  "MERGE"
            |  "OPTIONAL"
            |  "REMOVE"
            |  "RENAME"
            |  "RETURN"
            |  "REVOKE"
            |  "SET"
            |  "SHOW"
            |  "START"
            |  "STOP"
            |  "TERMINATE"
            |  "UNWIND"
            |  "USE"
            |  "USING"
            |  "WITH" (line 1, column 1 (offset: 0))""".stripMargin
      )
    )
  }

  describe("Arithmetic") {
    assertQueryExecutionFailure(
      "UNWIND [6, 0] AS p RETURN p / 0",
      CypherException.Arithmetic(
        wrapping = "/ by zero",
        operands = Seq(Expr.Integer(6L), Expr.Integer(0L))
      )
    )

    assertQueryExecutionFailure(
      "UNWIND [-34, 1949] AS p WITH p + 9223372036854775800 AS N RETURN 1",
      CypherException.Arithmetic(
        wrapping = "long overflow",
        operands = Seq(Expr.Integer(1949L), Expr.Integer(9223372036854775800L))
      )
    )
  }

  describe("Compile") {
    val query1 = "FOREACH (p IN [1,3,7] | UNWIND range(9,78) AS N)"
    assertStaticQueryFailure(
      query1,
      CypherException.Compile(
        wrapping = "Invalid use of UNWIND inside FOREACH",
        position = Some(
          Position(1, 25, 24, SourceText(query1))
        )
      )
    )

    val query2 = "CREATE (n)-[*]-(m)"
    assertStaticQueryFailure(
      query2,
      CypherException.Compile(
        wrapping = "Variable length relationships cannot be used in CREATE",
        position = Some(
          Position(1, 11, 10, SourceText(query2))
        )
      )
    )
  }

  describe("Unsupported Cypher features") {
    describe("Variable length path expressions") {
      val query1 = "MATCH p = (n)-[e*]-(m) RETURN *"
      assertStaticQueryFailure(
        query1,
        CypherException.Compile(
          wrapping = "Unknown variable `p`",
          position = Some(
            Position(1, 24, 23, SourceText(query1))
          )
        )
      )

      val query2 = "MATCH p = (bob {name: 'Bob'})-[e:KNOWS*1..3]-(guy:Person) RETURN p"
      assertStaticQueryFailure(
        query2,
        CypherException.Compile(
          wrapping = "Unknown variable `p`",
          position = Some(
            Position(1, 66, 65, SourceText(query2))
          )
        )
      )
    }

    describe("Edge properties") {
      val query = "CREATE (:Account { accId: 1 })-[r:TRANSERS {quantity: 4}]->(:Account { accId: 2 })"
      assertStaticQueryFailure(
        query,
        CypherException.Compile(
          wrapping = "Properties on edges are not yet supported",
          position = Some(
            Position(1, 31, 30, SourceText(query))
          )
        )
      )
    }

    describe("Shortest path matching") {
      val query =
        "MATCH (bob:Person {name: 'Bob'}), (joe:Person {name: 'Joe'}), p = shortestPath((bob)-[*..15]-(joe)) RETURN p"
      assertStaticQueryFailure(
        query,
        CypherException.Compile(
          wrapping = "`shortestPath` planning in graph patterns is not supported",
          position = Some(
            Position(1, 67, 66, SourceText(query))
          )
        )
      )
    }
  }
}

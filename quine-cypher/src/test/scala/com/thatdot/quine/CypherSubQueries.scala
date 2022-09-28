package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherSubQueries extends CypherHarness("cypher-subqueries-tests") {

  describe("nested aliasing aggregation") {
    testQuery(
      "UNWIND [0, 1, 2] AS x CALL { WITH x RETURN x * 10 AS y } RETURN x, y",
      expectedColumns = Vector("x", "y"),
      expectedRows = Seq(
        Vector(Expr.Integer(0L), Expr.Integer(0L)),
        Vector(Expr.Integer(1L), Expr.Integer(10L)),
        Vector(Expr.Integer(2L), Expr.Integer(20L))
      )
    )

    testQuery(
      """UNWIND range(0,2) AS x
        |CALL {
        |  WITH x
        |  UNWIND range(0,x) AS y
        |  CALL {
        |    WITH y
        |    UNWIND range(0,y) AS z
        |    RETURN z
        |  }
        |  RETURN y, z
        |}
        |RETURN x, y, z""".stripMargin,
      expectedColumns = Vector("x", "y", "z"),
      expectedRows = Seq(
        Vector(Expr.Integer(0L), Expr.Integer(0L), Expr.Integer(0L)),
        Vector(Expr.Integer(1L), Expr.Integer(0L), Expr.Integer(0L)),
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(0L)),
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(0L), Expr.Integer(0L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(0L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(0L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(2L))
      )
    )
  }

  describe("scoped aggregation") {
    testQuery(
      """UNWIND range(1,10) AS x
        |CALL {
        |  WITH x
        |  UNWIND range(1, x) AS y
        |  RETURN sum(y) AS sumToX
        |}
        |RETURN *""".stripMargin,
      expectedColumns = Vector("sumToX", "x"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(3L), Expr.Integer(2L)),
        Vector(Expr.Integer(6L), Expr.Integer(3L)),
        Vector(Expr.Integer(10L), Expr.Integer(4L)),
        Vector(Expr.Integer(15L), Expr.Integer(5L)),
        Vector(Expr.Integer(21L), Expr.Integer(6L)),
        Vector(Expr.Integer(28L), Expr.Integer(7L)),
        Vector(Expr.Integer(36L), Expr.Integer(8L)),
        Vector(Expr.Integer(45L), Expr.Integer(9L)),
        Vector(Expr.Integer(55L), Expr.Integer(10L))
      )
    )
  }

  describe("subquery scoping") {
    testQuery(
      "WITH 2 AS y CALL { RETURN 1 AS x } RETURN y",
      expectedColumns = Vector("y"),
      expectedRows = Seq(Vector(Expr.Integer(2L))),
      expectedCannotFail = true
    )

    testQuery(
      "WITH 2 AS y CALL { RETURN 1 AS x UNION ALL RETURN 2 AS x UNION ALL RETURN 3 AS x } RETURN *",
      expectedColumns = Vector("x", "y"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L)),
        Vector(Expr.Integer(3L), Expr.Integer(2L))
      ),
      expectedCannotFail = true
    )

    testQuery(
      """unwind range(1,2) as x
        |unwind range(1,2) as y
        |unwind range(1,2) as z
        |call {
        |  with y, x
        |  return y * x as w
        |}
        |return *
        |""".stripMargin,
      expectedColumns = Vector("w", "x", "y", "z"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(4L), Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(4L), Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(2L))
      )
    )

    testQuery(
      """unwind range(1,2) as x
        |unwind range(1,2) as y
        |unwind range(1,2) as z
        |call {
        |  with y, x
        |  return y * x as w
        |  union
        |  with x, z
        |  return x * z as w
        |} return *
        |""".stripMargin,
      expectedColumns = Vector("w", "x", "y", "z"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(1L), Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(4L), Expr.Integer(2L), Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(4L), Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(1L)),
        Vector(Expr.Integer(4L), Expr.Integer(2L), Expr.Integer(2L), Expr.Integer(2L))
      )
    )
  }
}

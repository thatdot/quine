package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.{CypherException, Expr, Type}

class CypherAggregations extends CypherHarness("cypher-aggregation-tests") {

  describe("`count(*)` aggregation") {
    testQuery(
      "UNWIND [1,2,\"hello\",4.0,null,true] AS n RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(6L)))
    )

    testQuery(
      "RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "UNWIND [] AS n RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )
  }

  describe("`count(...)` aggregation") {
    testQuery(
      "UNWIND [1,2,\"hello\",2,1,null,true] AS n RETURN count(n)",
      expectedColumns = Vector("count(n)"),
      expectedRows = Seq(Vector(Expr.Integer(6L)))
    )

    testQuery(
      "UNWIND [1,2,\"hello\",2,1,null,true] AS n RETURN count(DISTINCT n)",
      expectedColumns = Vector("count(DISTINCT n)"),
      expectedRows = Seq(Vector(Expr.Integer(4L)))
    )

    testQuery(
      "RETURN count(1)",
      expectedColumns = Vector("count(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "RETURN count(null)",
      expectedColumns = Vector("count(null)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN count(null)",
      expectedColumns = Vector("count(null)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )

    testQuery(
      "UNWIND [] AS n RETURN count(n)",
      expectedColumns = Vector("count(n)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )
  }

  describe("`collect(...)` aggregation") {
    testQuery(
      "UNWIND [1,2,\"hello\",2,1,null,true] AS n RETURN collect(n)",
      expectedColumns = Vector("collect(n)"),
      expectedRows = Seq(
        Vector(
          Expr.List(
            Vector(
              Expr.Integer(1L),
              Expr.Integer(2L),
              Expr.Str("hello"),
              Expr.Integer(2L),
              Expr.Integer(1L),
              Expr.True
            )
          )
        )
      )
    )

    testQuery(
      "UNWIND [1,2,\"hello\",2,1,null,true] AS n RETURN collect(DISTINCT n)",
      expectedColumns = Vector("collect(DISTINCT n)"),
      expectedRows = Seq(
        Vector(
          Expr.List(
            Vector(
              Expr.Integer(1L),
              Expr.Integer(2L),
              Expr.Str("hello"),
              Expr.True
            )
          )
        )
      )
    )

    testQuery(
      "RETURN collect(1)",
      expectedColumns = Vector("collect(1)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Integer(1L)))))
    )

    testQuery(
      "RETURN collect(null)",
      expectedColumns = Vector("collect(null)"),
      expectedRows = Seq(Vector(Expr.List(Vector.empty)))
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN collect(null)",
      expectedColumns = Vector("collect(null)"),
      expectedRows = Seq(Vector(Expr.List(Vector.empty)))
    )

    testQuery(
      "UNWIND [] AS n RETURN collect(n)",
      expectedColumns = Vector("collect(n)"),
      expectedRows = Seq(Vector(Expr.List(Vector.empty)))
    )
  }

  describe("`avg(...)` aggregation") {
    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN avg(n)",
      expectedColumns = Vector("avg(n)"),
      expectedRows = Seq(Vector(Expr.Floating(1.8)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN avg(DISTINCT n)",
      expectedColumns = Vector("avg(DISTINCT n)"),
      expectedRows = Seq(Vector(Expr.Floating(2.0)))
    )

    testQuery(
      "UNWIND [1.1,2.5,2.4,1.3,3.1] AS n RETURN avg(n)",
      expectedColumns = Vector("avg(n)"),
      expectedRows = Seq(Vector(Expr.Floating(2.08)))
    )

    testQuery(
      "RETURN avg(1)",
      expectedColumns = Vector("avg(1)"),
      expectedRows = Seq(Vector(Expr.Floating(1)))
    )

    testQuery(
      "RETURN avg(null)",
      expectedColumns = Vector("avg(null)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    interceptQuery(
      "UNWIND [1,2,\"hi\",3] AS N RETURN avg(N)",
      CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = Expr.Str("hi"),
        context = "average of values"
      )
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN avg(null)",
      expectedColumns = Vector("avg(null)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    testQuery(
      "UNWIND [] AS n RETURN avg(n)",
      expectedColumns = Vector("avg(n)"),
      expectedRows = Seq(Vector(Expr.Null))
    )
  }

  describe("`sum(...)` aggregation") {

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN sum(n)",
      expectedColumns = Vector("sum(n)"),
      expectedRows = Seq(Vector(Expr.Integer(9L)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN sum(DISTINCT n)",
      expectedColumns = Vector("sum(DISTINCT n)"),
      expectedRows = Seq(Vector(Expr.Integer(6L)))
    )

    testQuery(
      "UNWIND [1.1,2.5,2.4,1.3,3.1] AS n RETURN sum(n)",
      expectedColumns = Vector("sum(n)"),
      expectedRows = Seq(Vector(Expr.Floating(10.4)))
    )

    testQuery(
      "RETURN sum(1)",
      expectedColumns = Vector("sum(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "RETURN sum(null)",
      expectedColumns = Vector("sum(null)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )

    interceptQuery(
      "UNWIND [1,2,\"hi\",3] AS N RETURN sum(N)",
      CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = Expr.Str("hi"),
        context = "sum of values"
      )
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN sum(null)",
      expectedColumns = Vector("sum(null)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )

    testQuery(
      "UNWIND [] AS n RETURN sum(n)",
      expectedColumns = Vector("sum(n)"),
      expectedRows = Seq(Vector(Expr.Integer(0L)))
    )
  }

  describe("`max(...)` aggregation") {
    testQuery(
      "UNWIND [13, NULL, 44, 33, NULL] AS val RETURN max(val)",
      expectedColumns = Vector("max(val)"),
      expectedRows = Seq(Vector(Expr.Integer(44L)))
    )

    testQuery(
      "UNWIND [1, 'a', NULL , 0.2, 'b', '1', '99'] AS val RETURN max(val)",
      expectedColumns = Vector("max(val)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "UNWIND [1,2,\"hello\",4.0,null,true] AS n RETURN max(n)",
      expectedColumns = Vector("max(n)"),
      expectedRows = Seq(Vector(Expr.Floating(4.0)))
    )

    testQuery(
      "UNWIND [[1, 'a', 89],[1, 2]] AS val RETURN max(val)",
      expectedColumns = Vector("max(val)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Integer(1L), Expr.Integer(2L)))))
    )

    testQuery(
      "RETURN max(1)",
      expectedColumns = Vector("max(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "UNWIND [] AS n RETURN max(n)",
      expectedColumns = Vector("max(n)"),
      expectedRows = Seq(Vector(Expr.Null))
    )
  }

  describe("`min(...)` aggregation") {
    testQuery(
      "UNWIND [13, NULL, 44, 33, NULL] AS val RETURN min(val)",
      expectedColumns = Vector("min(val)"),
      expectedRows = Seq(Vector(Expr.Integer(13L)))
    )

    testQuery(
      "UNWIND [1, 'a', NULL , 0.2, 'b', '1', '99'] AS val RETURN min(val)",
      expectedColumns = Vector("min(val)"),
      expectedRows = Seq(Vector(Expr.Str("1")))
    )

    testQuery(
      "UNWIND [1,2,\"hello\",4.0,null,true] AS n RETURN min(n)",
      expectedColumns = Vector("min(n)"),
      expectedRows = Seq(Vector(Expr.Str("hello")))
    )

    testQuery(
      "UNWIND [[1, 'a', 89],[1, 2]] AS val RETURN min(val)",
      expectedColumns = Vector("min(val)"),
      expectedRows = Seq(
        Vector(Expr.List(Vector(Expr.Integer(1L), Expr.Str("a"), Expr.Integer(89L))))
      )
    )

    testQuery(
      "RETURN min(1)",
      expectedColumns = Vector("min(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "UNWIND [] AS n RETURN min(n)",
      expectedColumns = Vector("min(n)"),
      expectedRows = Seq(Vector(Expr.Null))
    )
  }

  describe("variable scoping") {
    testQuery(
      "UNWIND [1,3,2] AS x WITH count(*) AS cnt, x RETURN x + cnt",
      expectedColumns = Vector("x + cnt"),
      expectedRows = Seq(
        Vector(Expr.Integer(2L)),
        Vector(Expr.Integer(4L)),
        Vector(Expr.Integer(3L))
      ),
      ordered = false
    )

    testQuery(
      "UNWIND [1,3,2] AS x UNWIND [1,2,3] AS y WITH count(*) AS cnt, x + y AS sum WHERE sum = 3 RETURN *",
      expectedColumns = Vector("cnt", "sum"),
      expectedRows = Seq(
        Vector(Expr.Integer(2L), Expr.Integer(3L))
      )
    )

    testQuery(
      "UNWIND [1,3,2] AS x UNWIND [1,2,3] AS y WITH count(*) AS cnt, x + y AS sum ORDER BY cnt WHERE sum <> 3 RETURN *",
      expectedColumns = Vector("cnt", "sum"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(1L), Expr.Integer(6L)),
        Vector(Expr.Integer(2L), Expr.Integer(5L)),
        Vector(Expr.Integer(3L), Expr.Integer(4L))
      )
    )
  }
}

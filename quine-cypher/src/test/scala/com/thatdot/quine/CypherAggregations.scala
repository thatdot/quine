package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.{CypherException, Expr, Type}

class CypherAggregations extends CypherHarness("cypher-aggregation-tests") {

  describe("`count(*)` aggregation") {
    testQuery(
      "UNWIND [1,2,\"hello\",4.0,null,true] AS n RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(6L))),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [] AS n RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCannotFail = true
    )
  }

  describe("`count(...)` aggregation") {
    testQuery(
      "UNWIND [1,2,\"hello\",2,1,null,true] AS n RETURN count(n)",
      expectedColumns = Vector("count(n)"),
      expectedRows = Seq(Vector(Expr.Integer(6L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1,2,\"hello\",2,1,null,true] AS n RETURN count(DISTINCT n)",
      expectedColumns = Vector("count(DISTINCT n)"),
      expectedRows = Seq(Vector(Expr.Integer(4L))),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN count(1)",
      expectedColumns = Vector("count(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN count(null)",
      expectedColumns = Vector("count(null)"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN count(null)",
      expectedColumns = Vector("count(null)"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [] AS n RETURN count(n)",
      expectedColumns = Vector("count(n)"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCannotFail = true
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
      ),
      expectedCannotFail = true
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
      ),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN collect(1)",
      expectedColumns = Vector("collect(1)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Integer(1L))))),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN collect(null)",
      expectedColumns = Vector("collect(null)"),
      expectedRows = Seq(Vector(Expr.List(Vector.empty))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN collect(null)",
      expectedColumns = Vector("collect(null)"),
      expectedRows = Seq(Vector(Expr.List(Vector.empty))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [] AS n RETURN collect(n)",
      expectedColumns = Vector("collect(n)"),
      expectedRows = Seq(Vector(Expr.List(Vector.empty))),
      expectedCannotFail = true
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

    assertQueryExecutionFailure(
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

    assertQueryExecutionFailure(
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
      expectedRows = Seq(Vector(Expr.Integer(44L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1, 'a', NULL , 0.2, 'b', '1', '99'] AS val RETURN max(val)",
      expectedColumns = Vector("max(val)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1,2,\"hello\",4.0,null,true] AS n RETURN max(n)",
      expectedColumns = Vector("max(n)"),
      expectedRows = Seq(Vector(Expr.Floating(4.0))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [[1, 'a', 89],[1, 2]] AS val RETURN max(val)",
      expectedColumns = Vector("max(val)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Integer(1L), Expr.Integer(2L))))),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN max(1)",
      expectedColumns = Vector("max(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [] AS n RETURN max(n)",
      expectedColumns = Vector("max(n)"),
      expectedRows = Seq(Vector(Expr.Null)),
      expectedCannotFail = true
    )
  }

  describe("`min(...)` aggregation") {
    testQuery(
      "UNWIND [13, NULL, 44, 33, NULL] AS val RETURN min(val)",
      expectedColumns = Vector("min(val)"),
      expectedRows = Seq(Vector(Expr.Integer(13L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1, 'a', NULL , 0.2, 'b', '1', '99'] AS val RETURN min(val)",
      expectedColumns = Vector("min(val)"),
      expectedRows = Seq(Vector(Expr.Str("1"))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [1,2,\"hello\",4.0,null,true] AS n RETURN min(n)",
      expectedColumns = Vector("min(n)"),
      expectedRows = Seq(Vector(Expr.Str("hello"))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [[1, 'a', 89],[1, 2]] AS val RETURN min(val)",
      expectedColumns = Vector("min(val)"),
      expectedRows = Seq(
        Vector(Expr.List(Vector(Expr.Integer(1L), Expr.Str("a"), Expr.Integer(89L))))
      ),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN min(1)",
      expectedColumns = Vector("min(1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true
    )

    testQuery(
      "UNWIND [] AS n RETURN min(n)",
      expectedColumns = Vector("min(n)"),
      expectedRows = Seq(Vector(Expr.Null)),
      expectedCannotFail = true
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
        Vector(Expr.Integer(1L), Expr.Integer(6L)),
        Vector(Expr.Integer(1L), Expr.Integer(2L)),
        Vector(Expr.Integer(2L), Expr.Integer(5L)),
        Vector(Expr.Integer(3L), Expr.Integer(4L))
      )
    )
  }

  describe("`stDev(...)` aggregation") {

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN stDev(n)",
      expectedColumns = Vector("stDev(n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.8366600265340756d)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN stDev(DISTINCT n)",
      expectedColumns = Vector("stDev(DISTINCT n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.8366600265340756d)))
    )

    testQuery(
      "UNWIND [1.1,2.5,2.4,1.3,3.1] AS n RETURN stDev(n)",
      expectedColumns = Vector("stDev(n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.8497058314499201d)))
    )

    testQuery(
      "RETURN stDev(1)",
      expectedColumns = Vector("stDev(1)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )

    testQuery(
      "RETURN stDev(null)",
      expectedColumns = Vector("stDev(null)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )

    assertQueryExecutionFailure(
      "UNWIND [1,2,\"hi\",3] AS N RETURN stDev(N)",
      CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = Expr.Str("hi"),
        context = "standard deviation of values"
      )
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN stDev(null)",
      expectedColumns = Vector("stDev(null)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )

    testQuery(
      "UNWIND [] AS n RETURN stDev(n)",
      expectedColumns = Vector("stDev(n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )
  }

  describe("`stDevP(...)` aggregation") {

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN stDevP(n)",
      expectedColumns = Vector("stDevP(n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.7483314773547883d)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN stDevP(DISTINCT n)",
      expectedColumns = Vector("stDevP(DISTINCT n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.7483314773547883d)))
    )

    testQuery(
      "UNWIND [1.1,2.5,2.4,1.3,3.1] AS n RETURN stDevP(n)",
      expectedColumns = Vector("stDevP(n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.76d)))
    )

    testQuery(
      "RETURN stDevP(1)",
      expectedColumns = Vector("stDevP(1)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )

    testQuery(
      "RETURN stDevP(null)",
      expectedColumns = Vector("stDevP(null)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )

    assertQueryExecutionFailure(
      "UNWIND [1,2,\"hi\",3] AS N RETURN stDevP(N)",
      CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = Expr.Str("hi"),
        context = "standard deviation of values"
      )
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN stDevP(null)",
      expectedColumns = Vector("stDevP(null)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )

    testQuery(
      "UNWIND [] AS n RETURN stDevP(n)",
      expectedColumns = Vector("stDevP(n)"),
      expectedRows = Seq(Vector(Expr.Floating(0.0d)))
    )
  }

  describe("`percentileDisc(...)` aggregation") {

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileDisc(n, 0)",
      expectedColumns = Vector("percentileDisc(n, 0)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileDisc(n, 0.1)",
      expectedColumns = Vector("percentileDisc(n, 0.1)"),
      expectedRows = Seq(Vector(Expr.Integer(1L)))
    )

    testQuery(
      "UNWIND [1,2,5,1,3] AS n RETURN percentileDisc(n, 0.45)",
      expectedColumns = Vector("percentileDisc(n, 0.45)"),
      expectedRows = Seq(Vector(Expr.Integer(2L)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileDisc(n, 0.7)",
      expectedColumns = Vector("percentileDisc(n, 0.7)"),
      expectedRows = Seq(Vector(Expr.Integer(2L)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileDisc(n, 1)",
      expectedColumns = Vector("percentileDisc(n, 1)"),
      expectedRows = Seq(Vector(Expr.Integer(3L)))
    )

    testQuery(
      "RETURN percentileDisc(1.1, 0.3)",
      expectedColumns = Vector("percentileDisc(1.1, 0.3)"),
      expectedRows = Seq(Vector(Expr.Floating(1.1d)))
    )

    testQuery(
      "RETURN percentileDisc(null, 0.4)",
      expectedColumns = Vector("percentileDisc(null, 0.4)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    assertQueryExecutionFailure(
      "UNWIND [1,2,\"hi\",3] AS N RETURN percentileDisc(N, 0.2)",
      CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = Expr.Str("hi"),
        context = "percentile of values"
      )
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN percentileDisc(null, 0.2)",
      expectedColumns = Vector("percentileDisc(null, 0.2)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    testQuery(
      "UNWIND [] AS n RETURN percentileDisc(n, 0.45)",
      expectedColumns = Vector("percentileDisc(n, 0.45)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    // Evil use of `N` in the percentile
    testQuery(
      "UNWIND [1,2,3] AS N RETURN percentileDisc(N, N / 3.0)",
      expectedColumns = Vector("percentileDisc(N, N / 3.0)"),
      expectedRows = Seq(Vector(Expr.Integer(2L)))
    )

    assertQueryExecutionFailure(
      "UNWIND [-0.2, 1,2,3] AS N RETURN percentileDisc(N, N)",
      CypherException.Runtime("percentile of values between 0.0 and 1.0")
    )

    assertQueryExecutionFailure(
      "UNWIND [2,2,3] AS N RETURN percentileDisc(N, N)",
      CypherException.Runtime("percentile of values between 0.0 and 1.0")
    )
  }

  describe("`percentileCont(...)` aggregation") {

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileCont(n, 0)",
      expectedColumns = Vector("percentileCont(n, 0)"),
      expectedRows = Seq(Vector(Expr.Floating(1.0d)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileCont(n, 0.1)",
      expectedColumns = Vector("percentileCont(n, 0.1)"),
      expectedRows = Seq(Vector(Expr.Floating(1.0d)))
    )

    testQuery(
      "UNWIND [1,2,5,1,3] AS n RETURN percentileCont(n, 0.45)",
      expectedColumns = Vector("percentileCont(n, 0.45)"),
      expectedRows = Seq(Vector(Expr.Floating(1.8d)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileCont(n, 0.7)",
      expectedColumns = Vector("percentileCont(n, 0.7)"),
      expectedRows = Seq(Vector(Expr.Floating(2.0d)))
    )

    testQuery(
      "UNWIND [1,2,2,1,3] AS n RETURN percentileCont(n, 1)",
      expectedColumns = Vector("percentileCont(n, 1)"),
      expectedRows = Seq(Vector(Expr.Floating(3.0d)))
    )

    testQuery(
      "RETURN percentileCont(1.1, 0.3)",
      expectedColumns = Vector("percentileCont(1.1, 0.3)"),
      expectedRows = Seq(Vector(Expr.Floating(1.1d)))
    )

    testQuery(
      "RETURN percentileCont(null, 0.4)",
      expectedColumns = Vector("percentileCont(null, 0.4)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    assertQueryExecutionFailure(
      "UNWIND [1,2,\"hi\",3] AS N RETURN percentileCont(N, 0.2)",
      CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = Expr.Str("hi"),
        context = "percentile of values"
      )
    )

    testQuery(
      "UNWIND [1,2,3] AS N RETURN percentileCont(null, 0.2)",
      expectedColumns = Vector("percentileCont(null, 0.2)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    testQuery(
      "UNWIND [] AS n RETURN percentileCont(n, 0.45)",
      expectedColumns = Vector("percentileCont(n, 0.45)"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    // Evil use of `N` in the percentile
    testQuery(
      "UNWIND [1,2,3] AS N RETURN percentileCont(N, N / 3.0)",
      expectedColumns = Vector("percentileCont(N, N / 3.0)"),
      expectedRows = Seq(Vector(Expr.Floating(1.6666666666666665d)))
    )

    assertQueryExecutionFailure(
      "UNWIND [-0.2, 1,2,3] AS N RETURN percentileCont(N, N)",
      CypherException.Runtime("percentile of values between 0.0 and 1.0")
    )

    assertQueryExecutionFailure(
      "UNWIND [2,2,3] AS N RETURN percentileCont(N, N)",
      CypherException.Runtime("percentile of values between 0.0 and 1.0")
    )

  }
  describe("DISTINCT projections") {
    testQuery(
      "UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS x RETURN DISTINCT x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(4)),
        Vector(Expr.Integer(5))
      ),
      expectedCannotFail = true
    )

    /** the following 2 queries can't fail, but query compilation loses proof that the SKIP and LIMIT are constant ints
      * so we're forced to set `expectedCannotFail = false`
      */
    testQuery(
      "UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS x RETURN DISTINCT x SKIP 2 LIMIT 2",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(4))
      ),
      expectedCannotFail = false
    )
    testQuery(
      "UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS x RETURN DISTINCT x ORDER BY x DESC SKIP 2 LIMIT 2",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(2))
      ),
      expectedCannotFail = false
    )

    // WITH DISTINCT
    testQuery(
      "UNWIND [1, 1, 2, 2, 3, 3, 4, 4, 5, 5] AS x WITH DISTINCT x AS dX RETURN dX",
      expectedColumns = Vector("dX"),
      expectedRows = Seq(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(4)),
        Vector(Expr.Integer(5))
      ),
      expectedCannotFail = true,
      skip = true // QU-1748
    )
  }

}

package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.{Expr, Parameters, QueryContext, Value}

class CypherEquality extends CypherHarness("cypher-equality-tests") {

  /** Assert that a list of values (passed as a list literal) will ORDER BY into the
    * [ascending] order given in expectedValues
    */
  def testOrdering(listLiteral: String, expectedValues: Seq[Value]): Unit =
    testQuery(
      s"UNWIND $listLiteral AS datum RETURN datum ORDER BY datum",
      Vector("datum"),
      expectedValues.map(Vector(_)),
      expectedCannotFail = true,
      ordered = true
    )

  describe("`IN` list operator") {
    testExpression("1 IN null", Expr.Null)
    testExpression("null IN null", Expr.Null)
    testExpression("null IN []", Expr.False)
    testExpression("null IN [1,2,3,4]", Expr.Null)
    testExpression("null IN [1,null,2]", Expr.Null)
    testExpression("2 IN [1,2,3,4]", Expr.True)
    testExpression("6 IN [1,2,3,4]", Expr.False)
    testExpression("2 IN [1,null,2,3,4]", Expr.True)
    testExpression("6 IN [1,null,2,3,4]", Expr.Null)
    testExpression("[1,2] IN [[1,null,3]]", Expr.False, expectedCannotFail = true)
    testExpression("[1,2] IN [[1,null]]", Expr.Null, expectedCannotFail = true)
    testExpression("[1,2] IN [[1,2]]", Expr.True, expectedCannotFail = true)
  }

  describe("`=` operator") {
    testExpression("1 = 2.0", Expr.False, expectedCannotFail = true)
    testExpression("1 = 1.0", Expr.True, expectedCannotFail = true)
    testExpression("[1] = [1.0]", Expr.True, expectedCannotFail = true)

    // NaN = NaN  ==>  false
    //
    // TODO: review whether `sqrt(-1)` really is NaN? openCypher says not, but Neo4j says so
    testExpression("sqrt(-1) = sqrt(-1)", Expr.False)

    // Infinity = Infinity  ==>  true
    testExpression("1.0/0.0 = 1.0/0.0", Expr.True, expectedCannotFail = true)

    // Infinity = -Infinity  ==> false
    testExpression("1.0/0.0 = -1.0/0.0", Expr.False, expectedCannotFail = true)

    testExpression("null + {}", Expr.Null)
  }

  describe("`IS NULL` and `IS NOT NULL` operators") {
    testExpression(
      "x IS NOT NULL",
      Expr.False,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      queryPreamble = "WITH null AS x RETURN "
    )

    testExpression(
      "x IS NULL",
      Expr.True,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      queryPreamble = "WITH null AS x RETURN "
    )

    testExpression(
      "x IS NOT NULL",
      Expr.True,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      queryPreamble = "WITH 1 AS x RETURN "
    )

    testExpression(
      "x IS NULL",
      Expr.False,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      queryPreamble = "WITH 1 AS x RETURN "
    )
  }

  describe("NaN equality") {
    testExpression("0.0/0.0 = 0.0/0.0", Expr.False, expectedCannotFail = true)
    testExpression("0.0/0.0 <> 0.0/0.0", Expr.True, expectedCannotFail = true)
    testExpression(
      "0.0/0.0 = nan",
      Expr.False,
      expectedIsIdempotent = true,
      expectedCannotFail = true,
      queryPreamble = "WITH 0.0/0.0 AS nan RETURN "
    )
    testExpression(
      "0.0/0.0 <> nan",
      Expr.True,
      expectedIsIdempotent = true,
      queryPreamble = "WITH 0.0/0.0 AS nan RETURN "
    )
    testExpression(
      "nan = nan",
      Expr.False,
      expectedIsIdempotent = true,
      expectedCannotFail = true,
      queryPreamble = "WITH 0.0/0.0 AS nan RETURN "
    )
    testExpression("nan <> nan", Expr.True, expectedIsIdempotent = true, queryPreamble = "WITH 0.0/0.0 AS nan RETURN ")
  }

  describe("infinite equality") {
    testExpression(
      "NOT (n <> n)",
      Expr.True,
      expectedIsIdempotent = true,
      expectedCannotFail = true,
      queryPreamble = "WITH 1.0/0.0 AS n RETURN "
    )
    testExpression(
      "n = n",
      Expr.True,
      expectedIsIdempotent = true,
      expectedCannotFail = true,
      queryPreamble = "WITH 1.0/0.0 AS n RETURN "
    )
  }

  describe("NaN and infinite comparisons") {
    testExpression("1.0/0.0 > 0.0/0.0", Expr.False, expectedCannotFail = true)
    testExpression("1.0/0.0 < 0.0/0.0", Expr.False, expectedCannotFail = true)
    testExpression("-1.0/0.0 < 0.0/0.0", Expr.False, expectedCannotFail = true)
    testExpression("1.0/0.0 = 0.0/0.0", Expr.False, expectedCannotFail = true)
    testExpression("1.0/0.0 <> 0.0/0.0", Expr.True, expectedCannotFail = true)
  }

  describe("NULL equality") {
    testExpression(
      "n = n",
      Expr.Null,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      queryPreamble = "WITH null AS n RETURN "
    )
    testExpression("n <> n", Expr.Null, expectedIsIdempotent = true, queryPreamble = "WITH null AS n RETURN ")
  }

  describe("Lexicographic string comparison") {
    testExpression("'hi' < 'hello'", Expr.False)
    testExpression("'ha' < 'hello'", Expr.True)
    testExpression("'he' < 'hello'", Expr.True)
    testExpression("'hellooooo' < 'hello'", Expr.False)
  }

  describe("Map comparison") {
    testOrdering(
      "[{a: 7}, {b: 1}, {a: 2}, {b: 1, c: 3}, {b: 'cello'}]",
      Seq(
        Expr.Map(Map("a" -> Expr.Integer(2))),
        Expr.Map(Map("a" -> Expr.Integer(7))),
        Expr.Map(Map("b" -> Expr.Str("cello"))),
        Expr.Map(Map("b" -> Expr.Integer(1))),
        Expr.Map(Map("b" -> Expr.Integer(1), "c" -> Expr.Integer(3)))
      )
    )

    /** comparison with <, >, <=, >= on maps doesn't compile, but it can still come up when cypher's static analysis
      * pass falls short -- and it's well (enough) defined, so let's test it!
      */

    it("{a: 7} > {a: 6, b: 7}") {
      val gt = Expr.Greater(
        Expr.Map(
          Map(
            "a" -> Expr.Integer(7)
          )
        ),
        Expr.Map(
          Map(
            "a" -> Expr.Integer(6),
            "b" -> Expr.Integer(7)
          )
        )
      )
      assert(gt.eval(QueryContext.empty)(idProv, Parameters.empty) === Expr.True)
    }

    it("{a: 'six'} < {a: 6}") {
      val lt = Expr.Less(
        Expr.Map(
          Map(
            "a" -> Expr.Str("six")
          )
        ),
        Expr.Map(
          Map(
            "a" -> Expr.Integer(6)
          )
        )
      )
      assert(lt.eval(QueryContext.empty)(idProv, Parameters.empty) === Expr.True)
    }

    it("{a: 1} <= {a: 1, b: null} should return null") {
      val lte = Expr.LessEqual(
        Expr.Map(
          Map(
            "a" -> Expr.Integer(1)
          )
        ),
        Expr.Map(
          Map(
            "a" -> Expr.Integer(1),
            "b" -> Expr.Null
          )
        )
      )
      assert(lte.eval(QueryContext.empty)(idProv, Parameters.empty) === Expr.Null)
    }
  }
}

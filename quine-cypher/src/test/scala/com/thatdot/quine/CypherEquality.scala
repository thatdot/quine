package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherEquality extends CypherHarness("cypher-equality-tests") {

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
    testExpression("0.0/0.0 <> 0.0/0.0", Expr.True)
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
    testExpression("1.0/0.0 <> 0.0/0.0", Expr.True)
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

  describe("Lexicographic tring ordering") {
    testExpression("'hi' < 'hello'", Expr.False)
    testExpression("'ha' < 'hello'", Expr.True)
    testExpression("'he' < 'hello'", Expr.True)
    testExpression("'hellooooo' < 'hello'", Expr.False)
  }
}

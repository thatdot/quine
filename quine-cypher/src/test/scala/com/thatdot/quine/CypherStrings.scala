package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherStrings extends CypherHarness("cypher-string-tests") {

  describe("`STARTS WITH` operator") {
    testExpression("\"hello world\" STARTS WITH \"hell\"", Expr.True)
    testExpression("\"hello world\" STARTS WITH \"llo\"", Expr.False)
    testExpression("\"hello world\" STARTS WITH \"world\"", Expr.False)

    testExpression("\"hello world\" STARTS WITH NULL", Expr.Null)
    testExpression("NULL STARTS WITH \"hell\"", Expr.Null)

    testQuery(
      "UNWIND [1, 'foo'] AS lhs UNWIND [1, 'foo'] AS rhs RETURN lhs STARTS WITH rhs",
      expectedColumns = Vector("lhs STARTS WITH rhs"),
      expectedRows = Seq(
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.True)
      ),
      expectedIsIdempotent = true,
      expectedIsReadOnly = true
    )
  }

  describe("`CONTAINS` operator") {
    testExpression("\"hello world\" CONTAINS \"hell\"", Expr.True)
    testExpression("\"hello world\" CONTAINS \"llo\"", Expr.True)
    testExpression("\"hello world\" CONTAINS \"world\"", Expr.True)

    testExpression("\"hello world\" CONTAINS NULL", Expr.Null)
    testExpression("NULL CONTAINS \"hell\"", Expr.Null)

    testQuery(
      "UNWIND [1, 'foo'] AS lhs UNWIND [1, 'foo'] AS rhs RETURN lhs CONTAINS rhs",
      expectedColumns = Vector("lhs CONTAINS rhs"),
      expectedRows = Seq(
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.True)
      ),
      expectedIsIdempotent = true,
      expectedIsReadOnly = true
    )
  }

  describe("`ENDS WITH` operator") {
    testExpression("\"hello world\" ENDS WITH \"hell\"", Expr.False)
    testExpression("\"hello world\" ENDS WITH \"llo\"", Expr.False)
    testExpression("\"hello world\" ENDS WITH \"world\"", Expr.True)

    testExpression("\"hello world\" ENDS WITH NULL", Expr.Null)
    testExpression("NULL ENDS WITH \"hell\"", Expr.Null)

    testQuery(
      "UNWIND [1, 'foo'] AS lhs UNWIND [1, 'foo'] AS rhs RETURN lhs ENDS WITH rhs",
      expectedColumns = Vector("lhs ENDS WITH rhs"),
      expectedRows = Seq(
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.True)
      ),
      expectedIsIdempotent = true,
      expectedIsReadOnly = true
    )
  }

  describe("`=~` operator") {
    testExpression("\"hello world\" =~ \"he[lo]{1,8} w.*\"", Expr.True)
    testExpression("\"hello world\" =~ \"he[lo]{1,2} w.*\"", Expr.False)
    testExpression("\"hello world\" =~ \"llo\"", Expr.False) // full string match

    testExpression("\"hello world\" =~ NULL", Expr.Null)
    testExpression("NULL =~ \"hell\"", Expr.Null)

    testQuery(
      "UNWIND [1, 'foo'] AS lhs UNWIND [1, 'foo'] AS rhs RETURN lhs =~ rhs",
      expectedColumns = Vector("lhs =~ rhs"),
      expectedRows = Seq(
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.Null),
        Vector(Expr.True)
      ),
      expectedIsIdempotent = true,
      expectedIsReadOnly = true
    )
  }
}

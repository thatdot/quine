package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherLists extends CypherHarness("cypher-list-tests") {

  describe("list literals") {
    testExpression(
      "[0, 1, 2, 2 + 1, 4, 5, 6, 7, 8, 9]",
      Expr.List(Vector.tabulate(10)(i => Expr.Integer(i.toLong)))
    )

    testExpression("[]", Expr.List(Vector.empty))
  }

  describe("`[]` list operator") {
    testExpression("range(0, 10)[3]", Expr.Integer(3))

    testExpression("range(0, 10)[-3]", Expr.Integer(8))

    testExpression(
      "range(0, 10)[0..3]",
      Expr.List((0 until 3).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression(
      "range(0, 10)[0..-5]",
      Expr.List((0 to 5).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression(
      "range(0, 10)[-5..]",
      Expr.List((6 to 10).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression(
      "range(0, 10)[..4]",
      Expr.List((0 until 4).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression("range(0, 10)[15]", Expr.Null)

    testExpression(
      "range(0, 10)[5..15]",
      Expr.List((5 to 10).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression("size(range(0, 10)[0..3])", Expr.Integer(3L))
  }

  describe("list comprehensions (and `filter`/`extract`)") {
    testExpression(
      "[x IN range(0,10) WHERE x % 2 = 0 | x^3]",
      Expr.List(
        Vector(
          Expr.Floating(0.0),
          Expr.Floating(8.0),
          Expr.Floating(64.0),
          Expr.Floating(216.0),
          Expr.Floating(512.0),
          Expr.Floating(1000.0)
        )
      )
    )

    testExpression(
      "[x IN range(0,10) WHERE x % 2 = 0]",
      Expr.List(
        Vector(
          Expr.Integer(0),
          Expr.Integer(2),
          Expr.Integer(4),
          Expr.Integer(6),
          Expr.Integer(8),
          Expr.Integer(10)
        )
      )
    )

    testExpression(
      "[x IN range(0,10) | x^3]",
      Expr.List(
        Vector(
          Expr.Floating(0.0),
          Expr.Floating(1.0),
          Expr.Floating(8.0),
          Expr.Floating(27.0),
          Expr.Floating(64.0),
          Expr.Floating(125.0),
          Expr.Floating(216.0),
          Expr.Floating(343.0),
          Expr.Floating(512.0),
          Expr.Floating(729.0),
          Expr.Floating(1000.0)
        )
      )
    )

    testExpression(
      "filter(x in range(0,10) WHERE x > 3)",
      Expr.List(
        Vector(
          Expr.Integer(4),
          Expr.Integer(5),
          Expr.Integer(6),
          Expr.Integer(7),
          Expr.Integer(8),
          Expr.Integer(9),
          Expr.Integer(10)
        )
      )
    )

    testExpression(
      "extract(x in range(0,10) | x ^ 2)",
      Expr.List(
        Vector(
          Expr.Floating(0.0),
          Expr.Floating(1.0),
          Expr.Floating(4.0),
          Expr.Floating(9.0),
          Expr.Floating(16.0),
          Expr.Floating(25.0),
          Expr.Floating(36.0),
          Expr.Floating(49.0),
          Expr.Floating(64.0),
          Expr.Floating(81.0),
          Expr.Floating(100.0)
        )
      )
    )
  }

  describe("iterable list expressions") {
    describe("`any` list predicate") {
      testExpression("any(x IN [1,2,3,4,5] WHERE x > 2)", Expr.True)
      testExpression("any(x IN [true,null,false,false] WHERE x)", Expr.True)
      testExpression("any(x IN [null,null,false,false] WHERE x)", Expr.Null)
      testExpression("any(x IN [false,false,false,false] WHERE x)", Expr.False)
    }

    describe("`all` list predicate") {
      testExpression("all(x IN [1,2,3,4,5] WHERE x > 2)", Expr.False)
      testExpression("all(x IN [true,null,true,false] WHERE x)", Expr.False)
      testExpression("all(x IN [true,null,true,null] WHERE x)", Expr.Null)
      testExpression("all(x IN [true,true,true,true] WHERE x)", Expr.True)
    }

    describe("`none` list predicate") {
      testExpression("none(x IN [1,2,3,4,5] WHERE x > 2)", Expr.False)
      testExpression("none(x IN [true,null,true,false] WHERE x)", Expr.False)
      testExpression("none(x IN [false,null,false,null] WHERE x)", Expr.Null)
      testExpression("none(x IN [false,false,false,false] WHERE x)", Expr.True)
    }

    describe("`single` list predicate") {
      // more than one match
      testExpression("single(x IN [1,2,3,4,5] WHERE x > 2)", Expr.False)
      testExpression("single(x IN [1,2,3,4,5,null] WHERE x > 2)", Expr.False)

      // less than one match
      testExpression("single(x IN [1,2,3,4,5] WHERE x > 9)", Expr.False)

      // perhaps a match
      testExpression("single(x IN [true,null,null,false] WHERE x)", Expr.Null)
      testExpression("single(x IN [null,null,null,false] WHERE x)", Expr.Null)

      // exactly one match
      testExpression("single(x IN [1,2,3,4,5] WHERE x = 2)", Expr.True)
    }

    describe("`reduce` list") {
      testExpression("reduce(acc = 1, x IN [1,3,6,9] | acc * x)", Expr.Integer(162L))
    }
  }
}

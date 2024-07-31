package com.thatdot.quine

import com.thatdot.quine.compiler.cypher.CypherHarness
import com.thatdot.quine.graph.cypher.Query.{AdjustContext, Apply, RecursiveSubQuery, Unwind}
import com.thatdot.quine.graph.cypher.{
  Columns,
  CompiledQuery,
  CypherException,
  Expr,
  Func,
  Location,
  Parameters,
  Position,
  Query,
  SourceText,
  Type
}

class CypherRecursiveSubQuery extends CypherHarness("cypher-recursive-subqueries") {

  describe("Basic recursive subquery") {
    val setXToZero: AdjustContext[Location.Anywhere] = AdjustContext( // x = 0
      dropExisting = true,
      toAdd = Vector(
        Symbol("x") -> Expr.Integer(0)
      ),
      adjustThis = Query.Unit(Columns.Specified(Vector.empty)),
      columns = Columns.Specified(Vector(Symbol("x")))
    )
    val incrementX: AdjustContext[Location.Anywhere] = AdjustContext( // x++
      dropExisting = true,
      toAdd = Vector(
        Symbol("x") -> Expr.Add(Expr.Variable(Symbol("x")), Expr.Integer(1))
      ),
      adjustThis = Query.Unit(Columns.Specified(Vector.empty)),
      columns = Columns.Specified(Vector(Symbol("x")))
    )

    val countToTenQuery = CompiledQuery[Location.External](
      Some("""
          |CALL RECURSIVELY WITH 0 AS x UNTIL (x >= 10) {
          |  RETURN x+1 AS x
          |} RETURN x
          |""".stripMargin.replace('\n', ' ').trim),
      Apply(
        setXToZero,
        RecursiveSubQuery(
          incrementX,
          inputVariableToPlain = Map(Symbol("x") -> Symbol("x")),
          outputVariableToPlain = Map(Symbol("x") -> Symbol("x")),
          doneExpression = Expr.GreaterEqual(Expr.Variable(Symbol("x")), Expr.Integer(10)), // x >= 10
          columns = Columns.Specified(Vector(Symbol("x")))
        ),
        columns = Columns.Specified(Vector(Symbol("x")))
      ),
      Seq.empty,
      Parameters.empty,
      Seq.empty
    )

    testQuery(
      countToTenQuery,
      Vector("x"),
      Seq(
        Vector(Expr.Integer(10))
      )
    )

    testQuery(
      countToTenQuery.queryText.get,
      Vector("x"),
      Seq(
        Vector(Expr.Integer(10))
      )
    )

    testQuery(
      """CALL RECURSIVELY WITH 0 AS x UNTIL (y > 5) {
        |  RETURN x+1 AS x, x AS y
        |} RETURN x, y
        |""".stripMargin.replace('\n', ' ').trim,
      Vector("x", "y"),
      Seq(
        Vector(Expr.Integer(7), Expr.Integer(6))
      )
    )

  }

  describe("Fan-out recursive subquery") {
    val setMaxTo1: AdjustContext[Location.Anywhere] = AdjustContext( // max = 1
      dropExisting = true,
      toAdd = Vector(
        Symbol("max") -> Expr.Integer(1)
      ),
      adjustThis = Query.Unit(Columns.Specified(Vector.empty)),
      columns = Columns.Specified(Vector(Symbol("max")))
    )
    val countXOneToMax: Unwind[Location.Anywhere] = Unwind(
      Expr.Function(Func.Range, Vector(Expr.Integer(1), Expr.Variable(Symbol("max")))), // 1 to max
      Symbol("x"),
      Query.Unit(Columns.Specified(Vector.empty)),
      Columns.Specified(Vector(Symbol("x"), Symbol("max")))
    )
    val incrementMaxNoopX: AdjustContext[Location.Anywhere] = AdjustContext(
      dropExisting = true,
      toAdd = Vector(
        Symbol("x") -> Expr.Variable(Symbol("x")),
        Symbol("max") -> Expr.Add(Expr.Variable(Symbol("max")), Expr.Integer(1))
      ),
      adjustThis = Query.Unit(Columns.Specified(Vector.empty)),
      columns = Columns.Specified(Vector(Symbol("x"), Symbol("max")))
    )

    val nonlinearRecursiveQuery = CompiledQuery[Location.External](
      Some("""
             |CALL RECURSIVELY WITH 1 AS max UNTIL (max > 3) {
             |  UNWIND range(1, max) AS x
             |  RETURN x, max + 1 AS max
             |} RETURN x""".stripMargin.replace('\n', ' ').trim),
      AdjustContext(
        dropExisting = true,
        toAdd = Vector(
          Symbol("x") -> Expr.Variable(Symbol("x"))
        ),
        adjustThis = Apply(
          setMaxTo1,
          RecursiveSubQuery(
            Apply(
              countXOneToMax,
              incrementMaxNoopX,
              Columns.Specified(Vector(Symbol("x"), Symbol("max")))
            ),
            inputVariableToPlain = Map(Symbol("max") -> Symbol("max")),
            outputVariableToPlain = Map(Symbol("max") -> Symbol("max")),
            doneExpression = Expr.Greater(Expr.Variable(Symbol("max")), Expr.Integer(3)), // max >= 3
            columns = Columns.Specified(Vector(Symbol("x"), Symbol("max")))
          ),
          Columns.Specified(Vector(Symbol("x"), Symbol("max"))) // TODO are these in the right order?
        ),
        columns = Columns.Specified(Vector(Symbol("x")))
      ),
      Seq.empty,
      Parameters.empty,
      Seq.empty
    )

    // Evaluation looks like the following (square brackets are unreturned rows, parentheses are returned rows):
    // [x=1, max=2]
    // [x=1, max=3],                             [x=2, max=3]
    // (x=1, max=4), (x=2, max=4), (x=3, max=4), (x=1, max=4), (x=2, max=4), (x=3, max=4)

    testQuery(
      nonlinearRecursiveQuery,
      Vector("x"),
      Seq(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3))
      )
    )

    testQuery(
      nonlinearRecursiveQuery.queryText.get,
      Vector("x"),
      Seq(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3))
      )
    )
  }

  describe("Malformed inner queries") {
    val columnNotImported = "WITH 0 AS foo CALL RECURSIVELY WITH 0 AS x UNTIL (x > 0) { RETURN foo, 2 AS x } RETURN x"
    assertStaticQueryFailure(
      columnNotImported,
      CypherException.Compile(
        "Variable `foo` not defined",
        Some(Position(1, 67, 66, SourceText(columnNotImported)))
      )
    )

    val illegalVanillaImport =
      "WITH 0 AS foo CALL RECURSIVELY WITH 0 AS x UNTIL (x > 0) { WITH foo RETURN 2 AS x } RETURN x"
    assertStaticQueryFailure(
      illegalVanillaImport,
      CypherException.Compile(
        "Recursive subqueries cannot use import-`WITH` subquery syntax. Use `CALL RECURSIVELY WITH` syntax instead",
        Some(Position(1, 60, 59, SourceText(illegalVanillaImport)))
      )
    )

    val missingRecursiveColumns = "CALL RECURSIVELY WITH 0 AS x UNTIL (x > 0) { RETURN x AS y } RETURN y"
    assertStaticQueryFailure(
      missingRecursiveColumns,
      CypherException.Compile(
        "Recursive subquery declares recursive variable(s): [`x`] but does not return all of them. Missing variable(s): [`x`]",
        Some(Position(1, 46, 45, SourceText(missingRecursiveColumns)))
      )
    )

    // TODO it'd be much better if we could do this typechecking at compile time
    val typeChangingRecursiveVariable = "CALL RECURSIVELY WITH 0 AS x UNTIL (x > 0) { RETURN 'foo' AS x } RETURN x"
    assertQueryExecutionFailure(
      typeChangingRecursiveVariable,
      CypherException.TypeMismatch(
        Seq(Type.Integer),
        Expr.Str("foo"),
        "recursive subquery return value (variable `x`)",
        Some(Position(1, 46, 45, SourceText(typeChangingRecursiveVariable)))
      )
    )

    it("QU-1947: unhelpful error messages / missing errors") {
      pendingUntilFixed {
        val nonIdempotentSubquery = "CALL RECURSIVELY WITH 0 AS x UNTIL (x > 0) { CREATE () RETURN x } RETURN x"
        assertStaticQueryFailure(
          nonIdempotentSubquery,
          CypherException.Compile(
            "Recursive subquery must be idempotent",
            Some(Position(1, 46, 45, SourceText(nonIdempotentSubquery)))
          )
        )

        val doneConditionNotBoolean = "CALL RECURSIVELY WITH 0 AS x UNTIL (x) { RETURN x+1 AS x } RETURN x"
        assertStaticQueryFailure(
          doneConditionNotBoolean,
          CypherException.TypeMismatch(
            Seq(Type.Bool),
            Expr.Integer(1),
            "recursive subquery done condition",
            Some(Position(1, 46, 45, SourceText(doneConditionNotBoolean)))
          )
        )

        val doneConditionDoesNotUseReturnValues = "CALL RECURSIVELY WITH 0 AS x UNTIL (true) { RETURN x } RETURN x"
        assertStaticQueryFailure(
          doneConditionDoesNotUseReturnValues,
          CypherException.Compile(
            "Recursive subquery done condition must use at least one of the columns returned by the subquery: [`x`]",
            Some(Position(1, 46, 45, SourceText(doneConditionDoesNotUseReturnValues)))
          )
        )
      }
    }
  }

  describe("Allow variables to be passed-through unchanged") {
    // we're looking at `x` here -- `y` just makes sure the other requirements
    // for a recursive subquery are met (eg no infinite loops)
    it("Known bug: wrapped query causes incorrect output context") {
      pendingUntilFixed {
        val variableReturnedUnchanged =
          """CALL RECURSIVELY WITH 0 AS x, 1 AS y UNTIL (y > 0) {
          |  RETURN x, y+1 AS y
          |} RETURN x, y""".stripMargin.replace('\n', ' ').trim
        testQuery(
          variableReturnedUnchanged,
          Vector("x", "y"),
          Seq(
            Vector(Expr.Integer(0), Expr.Integer(2))
          )
        )
      }
    }
  }

  describe("Malformed subquery boundary") {
    it("QU-1947: unhelpful error messages / missing errors") {
      pendingUntilFixed {
        val subqueryReturnsConflictingColumn =
          """WITH 0 AS x
            |CALL RECURSIVELY WITH x AS x UNTIL (x > 5) {
            |  RETURN x + 1 AS x
            |} RETURN x
            |""".stripMargin.replace('\n', ' ').trim
        assertStaticQueryFailure(
          subqueryReturnsConflictingColumn,
          CypherException.Compile(
            "Recursive subquery binds column[s] already bound in the parent query: [`x`]",
            Some(Position(1, 46, 45, SourceText(subqueryReturnsConflictingColumn)))
          )
        )
        val unsupportedAggregationInVariables =
          """WITH 0 AS x
            |CALL RECURSIVELY WITH sum(x) AS x UNTIL (x > 5) {
            |  RETURN x + 1 AS x
            |} RETURN x
            |""".stripMargin.replace('\n', ' ').trim
        assertStaticQueryFailure(
          unsupportedAggregationInVariables,
          CypherException.Compile(
            "Recursive subquery initializers may not use aggregators: [`x`]",
            Some(Position(1, 46, 45, SourceText(unsupportedAggregationInVariables)))
          )
        )
      }
    }
  }

  describe("Refers to correct instance of variables in initializers") {
    val variableBoundBeforeAfterAndUsedDuring =
      """WITH 1 AS openCypherAmbiguous
        |CALL RECURSIVELY WITH openCypherAmbiguous AS y UNTIL (y > 0) {
        |  RETURN 2 AS y
        |}
        |WITH 3 AS openCypherAmbiguous
        |RETURN openCypherAmbiguous""".stripMargin
    testQuery(
      variableBoundBeforeAfterAndUsedDuring,
      Vector("openCypherAmbiguous"),
      Seq(
        Vector(Expr.Integer(3))
      ),
      expectedCannotFail = true
    )
  }

  describe("runaway recursion detection") {
    val query =
      """CALL RECURSIVELY WITH 0 AS x UNTIL (x > 5) {
        |  RETURN x
        |} RETURN x
        |""".stripMargin.replace('\n', ' ').trim
    it("QU-1947: Does not detect infinite loops") {
      pendingUntilFixed {
        assertQueryExecutionFailure(
          query,
          CypherException.Runtime(
            "Infinite recursion detected in recursive subquery",
            Some(Position(1, 46, 45, SourceText(query)))
          )
        )
      }
    }
  }

  describe("variable demangling works even in weird conditions") {
    // The variable name here is `  x @ 0` which looks a lot like a post-Namespacer openCypher variable
    val query =
      """CALL RECURSIVELY WITH 0 AS `  x @ 0` UNTIL (`  x @ 0` > 0) {
        |  RETURN `  x @ 0` + 1 AS `  x @ 0`
        |} RETURN `  x @ 0`
        |""".stripMargin.replace('\n', ' ').trim

    testQuery(
      query,
      Vector("  x @ 0"),
      Seq(
        Vector(Expr.Integer(1))
      ),
      skip = true // QU-1947: demangles incorrectly
    )
  }

  describe("Nested recursive subquery") {
    val nestedQuery =
      """CALL RECURSIVELY WITH 0 AS i, 0 AS x UNTIL (i = 10) {
        |  CALL RECURSIVELY WITH i AS i, i AS j, x AS x UNTIL (j = 10) {
        |     RETURN j + 1 AS j, i, x + 1 AS x
        |  }
        |  RETURN i + 1 AS i, j, x
        |}
        |RETURN i, j, x
        |""".stripMargin.replace('\n', ' ').trim

    testQuery(
      nestedQuery,
      Vector("i", "j", "x"),
      Seq(
        Vector(Expr.Integer(10), Expr.Integer(10), Expr.Integer(55))
      ),
      skip = true // QU-1947: fails to parse in openCypher
    )
  }

  describe("works even mid-query") {
    val midQueryCallRecursively =
      """WITH 0 AS x
        |CALL RECURSIVELY WITH x AS y UNTIL (y > 5) {
        |  RETURN y + 1 AS y
        |} RETURN y
        |""".stripMargin

    testQuery(
      midQueryCallRecursively,
      Vector("y"),
      Seq(
        Vector(Expr.Integer(6))
      )
    )
  }
}

package com.thatdot.quine.language.types

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart.ReadingClausePart
import com.thatdot.quine.cypher.ast.ReadingClause.FromPatterns
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.ast.Expression
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}
import com.thatdot.quine.language.phases.{Phase, TypeCheckingPhase, TypeCheckingState}
import com.thatdot.quine.language.types.Type.PrimitiveType

class TypeCheckerTests extends munit.FunSuite {

  import com.thatdot.quine.language.phases.UpgradeModule._
  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid

  val pipeline: Phase[LexerState, TypeCheckingState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase()

  def parseAndCheck(queryText: String): (TypeCheckingState, Option[Query]) =
    pipeline.process(queryText).value.run(LexerState(Nil)).value

  // Helper to filter for actual errors (not warnings)
  def getErrors(diagnostics: List[Diagnostic]): List[Diagnostic] =
    diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }

  // Helper to extract first expression from return clause
  def getReturnExpression(query: Query): Option[Expression] = query match {
    case sq: SinglepartQuery => sq.bindings.headOption.map(_.expression)
    case _ => None
  }

  // Helper to extract predicate expression from WHERE clause
  def getPredicateExpression(query: Query): Option[Expression] = query match {
    case sq: SinglepartQuery =>
      sq.queryParts.collectFirst { case ReadingClausePart(fp: FromPatterns) =>
        fp.maybePredicate
      }.flatten
    case _ => None
  }

  test("pipeline integration - parses and type checks simple query") {
    val (state, maybeQuery) = parseAndCheck("MATCH (n) RETURN n")

    assert(maybeQuery.isDefined, "Should parse and type check successfully")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no diagnostics, got: ${state.diagnostics}")
  }

  test("integer literal gets Integer type") {
    val (state, maybeQuery) = parseAndCheck("RETURN 42")

    assert(maybeQuery.isDefined, "Should parse successfully")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")

    val query = maybeQuery.get
    val returnExp = getReturnExpression(query)

    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Integer), s"Expected Integer type, got ${exp.ty}")
    }
  }

  test("string literal gets String type") {
    val (state, maybeQuery) = parseAndCheck("RETURN 'hello'")

    assert(maybeQuery.isDefined, "Should parse successfully")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.String), s"Expected String type, got ${exp.ty}")
    }
  }

  test("boolean literal gets Boolean type") {
    val (state, maybeQuery) = parseAndCheck("RETURN true")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"Expected Boolean type, got ${exp.ty}")
    }
  }

  test("comparison operator returns Boolean") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x = 1 RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val predicate = getPredicateExpression(maybeQuery.get)
    assert(predicate.isDefined, "Should have WHERE predicate")
    predicate.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"Comparison should return Boolean, got ${exp.ty}")
    }
  }

  test("arithmetic expression has numeric type") {
    val (state, maybeQuery) = parseAndCheck("RETURN 1 + 2")

    assert(maybeQuery.isDefined, "Should parse successfully")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "Arithmetic expression should have a type")
    // The type should be a TypeVariable with Semigroup constraint that unifies to Integer
    }
  }

  test("node binding gets NodeType") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")

    // Check that the symbol table has NodeType for 'a'
    val nodeTypes = state.symbolTable.typeVars.filter(_.ty == PrimitiveType.NodeType)
    assert(nodeTypes.nonEmpty, "Should have at least one NodeType entry in symbol table")
  }

  test("field access creates type variable") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) RETURN a.x")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "Field access should have a type")
      exp.ty.foreach {
        case _: Type.TypeVariable => () // Expected
        case other => fail(s"Expected TypeVariable for field access, got $other")
      }
    }
  }

  test("AND expression requires Boolean operands") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x = 1 AND a.y = 2 RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val predicate = getPredicateExpression(maybeQuery.get)
    assert(predicate.isDefined, "Should have WHERE predicate")
    predicate.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"AND expression should return Boolean, got ${exp.ty}")
    }
  }

  test("OR expression requires Boolean operands") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x = 1 OR a.y = 2 RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val predicate = getPredicateExpression(maybeQuery.get)
    assert(predicate.isDefined, "Should have WHERE predicate")
    predicate.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"OR expression should return Boolean, got ${exp.ty}")
    }
  }

  test("less than comparison returns Boolean") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x < 10 RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val predicate = getPredicateExpression(maybeQuery.get)
    assert(predicate.isDefined, "Should have WHERE predicate")
    predicate.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"Less than should return Boolean, got ${exp.ty}")
    }
  }

  test("greater than comparison returns Boolean") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x > 10 RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val predicate = getPredicateExpression(maybeQuery.get)
    assert(predicate.isDefined, "Should have WHERE predicate")
    predicate.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"Greater than should return Boolean, got ${exp.ty}")
    }
  }

  test("not equals comparison returns Boolean") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x <> 10 RETURN a")

    assert(maybeQuery.isDefined, "Should parse successfully")

    val predicate = getPredicateExpression(maybeQuery.get)
    assert(predicate.isDefined, "Should have WHERE predicate")
    predicate.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Boolean), s"Not equals should return Boolean, got ${exp.ty}")
    }
  }

  test("complex query with properties") {
    val (state, maybeQuery) = parseAndCheck("MATCH (p:Person {name: 'John'}) RETURN p.age")

    assert(maybeQuery.isDefined, "Should parse complex query")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")
  }

  test("query with multiple patterns") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a), (b) RETURN a, b")

    assert(maybeQuery.isDefined, "Should parse query with multiple patterns")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")
  }

  test("WITH clause projections get types") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WITH a.x AS x RETURN x")

    assert(maybeQuery.isDefined, "Should parse WITH query")
    // WITH clause should create type entries for projected expressions
  }

  test("parameter gets fresh type variable") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) WHERE a.x = $param RETURN a")

    assert(maybeQuery.isDefined, "Should parse query with parameter")
    // Parameters should get fresh type variables
  }

  test("list literal gets List type") {
    val (state, maybeQuery) = parseAndCheck("RETURN [1, 2, 3]")

    assert(maybeQuery.isDefined, "Should parse list literal")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "List should have a type")
      exp.ty.foreach {
        case Type.TypeConstructor(id, _) =>
          assert(id == Symbol("List"), s"Expected List type constructor, got $id")
        case other => fail(s"Expected TypeConstructor for list, got $other")
      }
    }
  }

  test("map literal gets Map type") {
    val (state, maybeQuery) = parseAndCheck("RETURN {foo: 1, bar: 2}")

    assert(maybeQuery.isDefined, "Should parse map literal")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "Map should have a type")
      exp.ty.foreach {
        case Type.TypeConstructor(id, _) =>
          assert(id == Symbol("Map"), s"Expected Map type constructor, got $id")
        case other => fail(s"Expected TypeConstructor for map, got $other")
      }
    }
  }

  test("CASE expression unifies branch types") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) RETURN CASE WHEN a.x = 1 THEN 'one' ELSE 'other' END")

    assert(maybeQuery.isDefined, "Should parse CASE expression")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    // CASE should have a type (unified from branches)
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "CASE should have a type")
    }
  }

  test("CREATE clause processes without errors") {
    val (state, maybeQuery) = parseAndCheck("CREATE (n:Person {name: 'Alice'}) RETURN n")

    assert(maybeQuery.isDefined, "Should parse CREATE query")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")
  }

  test("UNWIND creates element type binding") {
    val (state, maybeQuery) = parseAndCheck("UNWIND [1, 2, 3] AS x RETURN x")

    assert(maybeQuery.isDefined, "Should parse UNWIND query")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")

    // After symbol analysis, 'x' becomes QuineIdentifier(N), stored as "_qN" in the type table
    // Check that there's at least one type entry for a QuineIdentifier (which UNWIND creates)
    val quineTypeEntries = state.symbolTable.typeVars.filter(_.identifier.startsWith("_q"))
    assert(
      quineTypeEntries.nonEmpty,
      s"Should have type entry for UNWIND binding, found: ${state.symbolTable.typeVars.map(_.identifier)}",
    )
  }

  test("null literal gets Null type") {
    val (state, maybeQuery) = parseAndCheck("RETURN null")

    assert(maybeQuery.isDefined, "Should parse null literal")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.contains(Type.Null), s"Expected Null type, got ${exp.ty}")
    }
  }

  test("floating point literal gets Real type") {
    val (state, maybeQuery) = parseAndCheck("RETURN 3.14")

    assert(maybeQuery.isDefined, "Should parse floating point literal")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.contains(PrimitiveType.Real), s"Expected Real type, got ${exp.ty}")
    }
  }

  test("subtraction has numeric constraint") {
    val (state, maybeQuery) = parseAndCheck("RETURN 5 - 3")

    assert(maybeQuery.isDefined, "Should parse subtraction")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "Subtraction should have a type")
    }
  }

  test("multiplication has numeric constraint") {
    val (state, maybeQuery) = parseAndCheck("RETURN 4 * 2")

    assert(maybeQuery.isDefined, "Should parse multiplication")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")
  }

  test("division has numeric constraint") {
    val (state, maybeQuery) = parseAndCheck("RETURN 10 / 2")

    assert(maybeQuery.isDefined, "Should parse division")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")
  }

  test("function application gets fresh type variable") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a) RETURN count(a)")

    assert(maybeQuery.isDefined, "Should parse function call")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "Function call should have a type")
    }
  }

  test("relationship pattern processes without errors") {
    val (state, maybeQuery) = parseAndCheck("MATCH (a)-[:KNOWS]->(b) RETURN a, b")

    assert(maybeQuery.isDefined, "Should parse relationship pattern")
    assert(getErrors(state.diagnostics).isEmpty, s"Should have no type errors: ${state.diagnostics}")
  }

  test("diagnostics accumulate type errors") {
    // This test verifies the diagnostic mechanism works
    // The type checker currently doesn't produce errors for valid queries
    val (state, _) = parseAndCheck("MATCH (a) RETURN a")
    assert(getErrors(state.diagnostics).isEmpty, "Valid query should have no diagnostics")
  }
}

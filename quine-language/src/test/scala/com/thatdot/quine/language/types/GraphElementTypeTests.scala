package com.thatdot.quine.language.types

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.ast.Expression
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}
import com.thatdot.quine.language.phases.{Phase, TypeCheckingPhase, TypeCheckingState}
import com.thatdot.quine.language.types.Type.PrimitiveType

/** Tests that graph element bindings (nodes and edges) receive the correct
  * type annotations from the type checking phase, and that these annotations
  * are preserved through aliasing and projections.
  */
class GraphElementTypeTests extends munit.FunSuite {

  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid
  import com.thatdot.quine.language.phases.UpgradeModule._

  private val pipeline: Phase[LexerState, TypeCheckingState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase()

  def run(query: String): (TypeCheckingState, Option[Query]) =
    pipeline.process(query).value.run(LexerState(Nil)).value

  def getErrors(diagnostics: List[Diagnostic]): List[Diagnostic] =
    diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }

  def resolve(ty: Type, env: Map[Symbol, Type]): Type = ty match {
    case Type.TypeVariable(id, _) => env.get(id).map(resolve(_, env)).getOrElse(ty)
    case other => other
  }

  def getReturnExpression(query: Query): Option[Expression] = query match {
    case sq: SinglepartQuery => sq.bindings.headOption.map(_.expression)
    case _ => None
  }

  // --- NodeType assignment ---

  test("single node binding gets NodeType") {
    val (state, _) = run("MATCH (n) RETURN n")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")
    val nodeTypes = state.symbolTable.typeVars.filter(_.ty == PrimitiveType.NodeType)
    assert(nodeTypes.nonEmpty, "Node binding should have a NodeType entry")
  }

  test("multiple nodes in same pattern all get NodeType") {
    val (state, _) = run("MATCH (a), (b), (c) RETURN a, b, c")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")
    val nodeTypes = state.symbolTable.typeVars.filter(_.ty == PrimitiveType.NodeType)
    assert(
      nodeTypes.size >= 3,
      s"a, b, c should each have NodeType, got ${nodeTypes.size} entries: $nodeTypes",
    )
  }

  test("node binding preserves NodeType through WITH alias") {
    val (state, _) = run("MATCH (n) WITH n AS m RETURN m")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val allResolved = state.symbolTable.typeVars.map(e => (e.identifier, resolve(e.ty, state.typeEnv)))
    val nodeEntries = allResolved.filter(_._2 == PrimitiveType.NodeType)
    assert(
      nodeEntries.size >= 2,
      s"Both n and m should resolve to NodeType, got: $allResolved",
    )
  }

  // --- EdgeType assignment ---

  test("edge binding gets EdgeType") {
    val (state, _) = run("MATCH (a)-[r:KNOWS]->(b) RETURN r")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")
    val edgeTypes = state.symbolTable.typeVars.filter(_.ty == PrimitiveType.EdgeType)
    assert(edgeTypes.nonEmpty, "Edge binding should have an EdgeType entry")
  }

  test("edge binding preserves EdgeType through WITH alias") {
    val (state, _) = run("MATCH (a)-[r:KNOWS]->(b) WITH r AS e RETURN e")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val allResolved = state.symbolTable.typeVars.map(e => (e.identifier, resolve(e.ty, state.typeEnv)))
    val edgeEntries = allResolved.filter(_._2 == PrimitiveType.EdgeType)
    assert(
      edgeEntries.size >= 2,
      s"Both r and e should resolve to EdgeType, got: $allResolved",
    )
  }

  // --- Mixed node + edge ---

  test("node and edge in same pattern get distinct types") {
    val (state, _) = run("MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val nodeTypes = state.symbolTable.typeVars.filter(_.ty == PrimitiveType.NodeType)
    val edgeTypes = state.symbolTable.typeVars.filter(_.ty == PrimitiveType.EdgeType)

    assert(nodeTypes.size >= 2, s"a and b should be NodeType, got: $nodeTypes")
    assert(edgeTypes.size >= 1, s"r should be EdgeType, got: $edgeTypes")
  }

  // --- Field access on graph elements ---

  test("field access on node gets TypeVariable, not NodeType") {
    val (state, maybeQuery) = run("MATCH (n:Person) RETURN n.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val returnExp = getReturnExpression(maybeQuery.get)
    assert(returnExp.isDefined, "Should have return expression")
    returnExp.foreach { exp =>
      assert(exp.ty.isDefined, "Field access should have a type annotation")
      exp.ty.foreach {
        case _: Type.TypeVariable => () // correct — field type is statically unknown
        case PrimitiveType.NodeType => fail("Field access should not have NodeType")
        case other => fail(s"Expected TypeVariable, got $other")
      }
    }
  }

  // --- Non-graph bindings ---

  test("UNWIND binding does not get NodeType or EdgeType") {
    val (state, _) = run("UNWIND [1, 2, 3] AS x RETURN x")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val allResolved = state.symbolTable.typeVars.map(e => (e.identifier, resolve(e.ty, state.typeEnv)))
    val graphTypes = allResolved.filter { case (_, ty) =>
      ty == PrimitiveType.NodeType || ty == PrimitiveType.EdgeType
    }
    assert(
      graphTypes.isEmpty,
      s"UNWIND binding should not resolve to a graph element type, got: $allResolved",
    )
  }

  test("WITH expression binding does not get NodeType") {
    val (state, _) = run("WITH 42 AS x RETURN x")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val allResolved = state.symbolTable.typeVars.map(e => (e.identifier, resolve(e.ty, state.typeEnv)))
    val xResolved = allResolved.filter { case (_, ty) => ty == PrimitiveType.Integer }
    assert(xResolved.nonEmpty, s"WITH 42 AS x should resolve x to Integer, got: $allResolved")
  }
}

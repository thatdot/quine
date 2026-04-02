package com.thatdot.quine.language.phases

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart.ReadingClausePart
import com.thatdot.quine.cypher.ast.ReadingClause.FromPatterns
import com.thatdot.quine.cypher.phases.{LexerPhase, MaterializationPhase, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.ast.{BindingId, Expression}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}

/** Tests that the materialization phase correctly rewrites field access
  * expressions on graph element bindings to synthetic identifier lookups,
  * and leaves non-graph field access unchanged.
  */
class MaterializationTests extends munit.FunSuite {

  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid
  import com.thatdot.quine.language.phases.UpgradeModule._

  private val pipeline =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase() andThen MaterializationPhase

  def run(query: String): (TypeCheckingState, Option[Query]) =
    pipeline
      .process(query)
      .value
      .run(com.thatdot.quine.cypher.phases.LexerState(List()))
      .value

  def getErrors(diagnostics: List[Diagnostic]): List[Diagnostic] =
    diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }

  // --- basic rewriting ---

  test("node field access is rewritten to synthetic Ident") {
    val (state, maybeQuery) = run("MATCH (n) RETURN n.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    // RETURN expression should be Ident(synthId), not FieldAccess
    val query = maybeQuery.get.asInstanceOf[SinglepartQuery]
    query.bindings.head.expression match {
      case Expression.Ident(_, Right(BindingId(synthId)), _) =>
        // synthId should be recorded in the property access mapping
        val mapping = state.propertyAccessMapping.entries
          .find(_.synthId == synthId)
        assert(mapping.isDefined, s"SynthId $synthId should appear in property access mapping")
        assertEquals(mapping.get.property, Symbol("name"))
      case other =>
        fail(s"Expected Ident with synthetic BindingId, got: $other")
    }
  }

  // --- deduplication ---

  test("same property on same node accessed twice gets same synthId") {
    val (state, maybeQuery) = run("MATCH (a) WHERE a.name = 'Alice' RETURN a.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val query = maybeQuery.get.asInstanceOf[SinglepartQuery]

    // Extract synthId from WHERE: a.name = 'Alice'
    val predicate = query.queryParts
      .collectFirst { case ReadingClausePart(fp: FromPatterns) =>
        fp.maybePredicate
      }
      .flatten
      .get
    val whereId = predicate match {
      case Expression.BinOp(_, _, Expression.Ident(_, Right(BindingId(id)), _), _, _) => id
      case other => fail(s"Expected BinOp with Ident in WHERE, got: $other")
    }

    // Extract synthId from RETURN
    val returnId = query.bindings.head.expression match {
      case Expression.Ident(_, Right(BindingId(id)), _) => id
      case other => fail(s"Expected Ident in RETURN, got: $other")
    }

    assertEquals(whereId, returnId, "Same property on same node should reuse the same synthId")

    // Only one PropertyAccess entry for a.name
    val nameAccesses = state.propertyAccessMapping.entries.filter(_.property == Symbol("name"))
    assertEquals(nameAccesses.size, 1, s"Should have exactly one PropertyAccess for a.name, got: $nameAccesses")
  }

  // --- distinct synthIds ---

  test("different properties on same node get different synthIds") {
    val (state, _) = run("MATCH (a) RETURN a.name, a.age")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 2, s"Expected 2 property access entries, got: $mappings")

    val nameAccess = mappings.find(_.property == Symbol("name")).get
    val ageAccess = mappings.find(_.property == Symbol("age")).get
    assert(nameAccess.synthId != ageAccess.synthId, "Different properties should get different synthIds")
    assertEquals(nameAccess.onBinding, ageAccess.onBinding, "Both should reference the same binding")
  }

  test("same property on different nodes gets different synthIds") {
    val (state, _) = run("MATCH (a), (b) RETURN a.name, b.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 2, s"Expected 2 property access entries, got: $mappings")
    assertEquals(mappings.map(_.synthId).distinct.size, 2, "Same property on different nodes → different synthIds")
    assertEquals(mappings.map(_.onBinding).distinct.size, 2, "Property accesses should be on different bindings")
  }

  // --- non-graph-element field access ---

  test("field access on map literal is not rewritten") {
    val (state, _) = run("WITH {name: 'Alice'} AS m RETURN m.name")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")
    assert(
      state.propertyAccessMapping.entries.isEmpty,
      s"Map field access should not produce PropertyAccess entries, got: ${state.propertyAccessMapping.entries}",
    )
  }

  // --- edge field access ---

  test("field access on edge binding produces diagnostic") {
    val (state, _) = run("MATCH (a)-[r:KNOWS]->(b) RETURN r.since")

    val edgeErrors = state.diagnostics.collect {
      case Diagnostic.TypeCheckError(msg) if msg.toLowerCase.contains("edge") => msg
    }
    assert(
      edgeErrors.nonEmpty,
      s"Field access on edge binding should produce an error diagnostic, got: ${state.diagnostics}",
    )
  }

  // --- nested expressions ---

  test("field access inside CASE expression is rewritten") {
    val (state, _) = run(
      "MATCH (n) RETURN CASE WHEN n.active = true THEN n.name ELSE 'unknown' END",
    )

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 2, s"Should have entries for n.active and n.name, got: $mappings")
    assert(mappings.exists(_.property == Symbol("active")), "Should have PropertyAccess for n.active")
    assert(mappings.exists(_.property == Symbol("name")), "Should have PropertyAccess for n.name")
  }

  test("field access inside list literal is rewritten") {
    val (state, _) = run("MATCH (n) RETURN [n.x, n.y, n.z]")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 3, s"Should have 3 property access entries, got: $mappings")
    assertEquals(mappings.map(_.property).toSet, Set(Symbol("x"), Symbol("y"), Symbol("z")))
  }

  test("field access inside binary expression is rewritten") {
    val (state, _) = run("MATCH (n) RETURN n.x + n.y")

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    val mappings = state.propertyAccessMapping.entries
    assertEquals(mappings.size, 2, s"Should have entries for n.x and n.y, got: $mappings")
  }

  test("SET target field access is NOT rewritten to synthId") {
    // SET n.name = 'Bob' — the write target should stay as FieldAccess
    val (state, _) = run(
      """MATCH (n)
        |WHERE id(n) = idFrom('test')
        |SET n.name = 'Bob'""".stripMargin,
    )

    assert(getErrors(state.diagnostics).isEmpty, s"Errors: ${state.diagnostics}")

    // The SET target should NOT appear in the property access mapping
    // (property access mapping is only for reads, not writes)
    val nameAccesses = state.propertyAccessMapping.entries.filter(_.property == Symbol("name"))
    assert(
      nameAccesses.isEmpty,
      s"SET target should not produce a PropertyAccess entry, got: $nameAccesses",
    )
  }
}

package com.thatdot.quine.language.phases

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.BindingEntry
import com.thatdot.quine.cypher.phases.{LexerPhase, MaterializationPhase, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.ast.{BindingId, Expression}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}

/** Tests that symbol analysis correctly alpha-renames all identifiers:
  * each binding gets a globally unique BindingId, references resolve to the
  * correct ID, and scoping/shadowing rules are enforced.
  */
class AlphaRenamingTests extends munit.FunSuite {

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

  // --- unique ID assignment ---

  test("each binding gets a distinct ID") {
    val (state, _) = run("MATCH (a), (b), (c) RETURN a, b, c")

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")

    val ids = state.symbolTable.references.collect {
      case BindingEntry(_, id, Some(name)) if Set(Symbol("a"), Symbol("b"), Symbol("c")).contains(name) => id
    }
    assertEquals(ids.distinct.size, 3, s"a, b, c should each have a unique ID, got: $ids")
  }

  test("multiple references to same binding resolve to same ID") {
    val (state, maybeQuery) = run("MATCH (a) WHERE a.x = 1 AND a.y = 2 RETURN a")

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")

    // The RETURN expression for 'a' should still be a plain Ident (not a synthId)
    val query = maybeQuery.get.asInstanceOf[SinglepartQuery]
    val returnIdent = query.bindings.head.expression
    returnIdent match {
      case Expression.Ident(_, Right(BindingId(id)), _) =>
        assertEquals(id, 1, "RETURN a should reference BindingId(1)")
      case other => fail(s"Expected Ident(Right(BindingId(1))), got: $other")
    }
  }

  test("anonymous node patterns get fresh IDs") {
    val (state, _) = run("MATCH (), (a) RETURN a")

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")

    val anonBindings = state.symbolTable.references.filter(_.originalName.isEmpty)
    val namedBindings = state.symbolTable.references.filter(_.originalName.contains(Symbol("a")))

    assert(anonBindings.nonEmpty, "Anonymous node () should still get a binding entry")
    assert(namedBindings.nonEmpty, "Named node (a) should get a binding entry")
    assert(
      anonBindings.head.identifier != namedBindings.head.identifier,
      "Anonymous and named bindings must have different IDs",
    )
  }

  // --- shadowing / barrier semantics ---

  test("shadowing: same name after WITH barrier gets a new ID") {
    // MATCH (n) WITH n.x AS val WITH val AS n RETURN n
    // first 'n' → id 1, 'val' → id 2, second 'n' → id 3
    val (state, _) = run("MATCH (n) WITH n.x AS val WITH val AS n RETURN n")

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")

    val nBindings = state.symbolTable.references.filter(_.originalName.contains(Symbol("n")))
    assertEquals(nBindings.size, 2, s"Should have two distinct bindings for 'n', got: $nBindings")
    assertEquals(
      nBindings.map(_.identifier).distinct.size,
      2,
      "Both 'n' bindings should have different IDs (shadowing)",
    )
  }

  test("WITH barrier hides previous bindings") {
    // After `WITH a AS x`, 'b' is no longer in scope
    val (state, _) = run("MATCH (a), (b) WITH a AS x RETURN b")

    // 'b' should produce an undefined-variable diagnostic
    val undefinedErrors = state.diagnostics.collect {
      case Diagnostic.SymbolAnalysisError(msg) if msg.contains("Undefined variable") => msg
    }
    assert(
      undefinedErrors.nonEmpty,
      s"Expected undefined-variable error for 'b' after WITH barrier, diagnostics: ${state.diagnostics}",
    )
  }

  test("WITH * forwards all bindings") {
    val (state, maybeQuery) = run("MATCH (a), (b) WITH * RETURN a, b")

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")
    // Both a and b should still be reachable
    val names = state.symbolTable.references.flatMap(_.originalName).toSet
    assert(
      names.contains(Symbol("a")) && names.contains(Symbol("b")),
      s"WITH * should forward both a and b, got: $names",
    )
  }

  // --- aliasing ---

  test("alias creates new ID, original ID remains in symbol table") {
    val (state, _) = run("MATCH (a) WITH a AS x RETURN x")

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")

    val aEntry = state.symbolTable.references.find(_.originalName.contains(Symbol("a")))
    val xEntry = state.symbolTable.references.find(_.originalName.contains(Symbol("x")))

    assert(aEntry.isDefined, "Should have binding entry for 'a'")
    assert(xEntry.isDefined, "Should have binding entry for 'x'")
    assert(aEntry.get.identifier != xEntry.get.identifier, "'a' and 'x' should have different IDs")
  }

  // --- subquery scoping ---

  test("subquery imports only listed variables") {
    val (state, _) = run(
      """MATCH (a), (b)
        |CALL { WITH a
        |  MATCH (c)
        |  WHERE id(c) = idFrom(a)
        |  RETURN c
        |}
        |RETURN a, b, c""".stripMargin,
    )

    assert(getErrors(state.diagnostics).isEmpty, s"Unexpected errors: ${state.diagnostics}")

    val names = state.symbolTable.references.flatMap(_.originalName).toSet
    assert(Set(Symbol("a"), Symbol("b"), Symbol("c")).subsetOf(names), s"Should have bindings for a, b, c, got: $names")
  }
}

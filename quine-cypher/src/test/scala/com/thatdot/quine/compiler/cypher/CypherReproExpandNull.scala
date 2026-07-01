package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

/** Regression test for: a relationship pattern used as a predicate (or projection) inside a list
  * comprehension whose loop variable is an endpoint of that pattern.
  *
  * Previously this threw
  *   TypeMismatch "Expected type(s) Node but got value null ... in one extremity of an edge we are
  *   expanding to"
  * when the loop variable was the far endpoint, and silently returned the wrong answer when the
  * outer variable was the anchor. Root cause: the pattern predicate compiled to a synthesized graph
  * fetch whose effect was *hoisted* out of the comprehension's per-element scope, so the loop
  * variable was unbound at runtime.
  *
  * Fixed by lowering a graph-touching list comprehension to a correlated `collect`-aggregation over
  * `UNWIND list AS v`, so `v` is a real query column and the traversal runs correlated with it.
  */
class CypherReproExpandNull extends CypherHarness("cypher-repro-expand-null") {

  // Seed: a -[:DEPLOYED_TO]-> b ; c is an unconnected VM.
  testQuery(
    "CREATE (a:Machine {n:'a'})-[:DEPLOYED_TO]->(b:VM {n:'b'}), (c:VM {n:'c'}) RETURN a.n",
    expectedColumns = Vector("a.n"),
    expectedRows = Seq(Vector(Expr.Str("a"))),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
  )

  describe("pattern predicate NOT referencing the loop variable") {
    // `a` IS deployed to a VM, so the existence predicate holds for every element and all three
    // survive. (Formerly returned [] because the outer binding of `a` was not threaded into the
    // comprehension predicate's runtime context.)
    testQuery(
      "MATCH (a:Machine) RETURN [v IN [1,2,3] WHERE (a)-[:DEPLOYED_TO]->(:VM)] AS xs",
      expectedColumns = Vector("xs"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(3L))))),
      expectedCanContainAllNodeScan = true,
    )
  }

  describe("loop variable as the far endpoint of the pattern") {
    // Only `b` (the VM that `a` is actually deployed to) survives the filter; `c` is unconnected.
    // (Formerly threw TypeMismatch because `v` evaluated to null in the hoisted Expand.)
    testQuery(
      "MATCH (a:Machine) MATCH (b:VM) WITH a, collect(b) AS bs " +
      "RETURN [v IN bs WHERE (a)-[:DEPLOYED_TO]->(v) | v.n] AS xs",
      expectedColumns = Vector("xs"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Str("b"))))),
      expectedCanContainAllNodeScan = true,
    )
  }

  describe("null element in the comprehension's list") {
    // A null flows through UNWIND into the pattern's far endpoint. A null endpoint can't match any
    // edge, so `v = null` is a non-match and is dropped -- only `b` survives (matches Neo4j, which
    // returns ["b"]). (Formerly threw TypeMismatch: the Expand demanded the endpoint resolve to a
    // QuineId and threw on null before ever consulting the edge collection.)
    testQuery(
      "MATCH (a:Machine) MATCH (b:VM {n:'b'}) WITH a, collect(b) + [null] AS bs " +
      "RETURN [v IN bs WHERE (a)-[:DEPLOYED_TO]->(v) | v.n] AS xs",
      expectedColumns = Vector("xs"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Str("b"))))),
      expectedCanContainAllNodeScan = true,
    )
  }

  describe("null-bound endpoint in a top-level MATCH") {
    // A pre-existing interpreter gap, independent of the comprehension lowering: expanding to a
    // null-bound node threw TypeMismatch instead of matching nothing. Cypher (per Neo4j) returns
    // zero rows here. The null-aware Expand fixes both this and the comprehension case above.
    testQuery(
      "WITH null AS x MATCH (a:Machine)-[:DEPLOYED_TO]->(x) RETURN a.n",
      expectedColumns = Vector("a.n"),
      expectedRows = Seq.empty,
    )
  }

  describe("negated pattern (NOT exists) correlated on the loop variable") {
    // `a` is already deployed to its one VM, so the "not yet linked" set is empty. Uses `NOT exists(...)`
    // (the supported boolean form); the bare `NOT (a)-[:R]->(v)` form hits a separate, pre-existing
    // pattern-expression-to-boolean coercion gap unrelated to the binding-scope fix under test here.
    testQuery(
      "MATCH (a:Machine) " +
      "OPTIONAL MATCH (a)-[:DEPLOYED_TO]->(vm) " +
      "WITH a, [v IN collect(DISTINCT vm) WHERE NOT exists((a)-[:DEPLOYED_TO]->(v))] AS vms " +
      "RETURN size(vms) AS n",
      expectedColumns = Vector("n"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCanContainAllNodeScan = true,
    )
  }
}

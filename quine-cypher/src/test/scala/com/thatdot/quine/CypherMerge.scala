package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherMerge extends CypherHarness("cypher-merge-tests") {

  import idProv.ImplicitConverters._

  testQuery(
    "match (n0) return n0",
    expectedColumns = Vector("n0"),
    expectedRows = Seq.empty,
    expectedCannotFail = true,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "merge (n1: Foo { prop1: 'val1' }) return n1",
    expectedColumns = Vector("n1"),
    expectedRows = Seq(
      Vector(Expr.Node(0L, Set(Symbol("Foo")), Map(Symbol("prop1") -> Expr.Str("val1"))))
    ),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "merge (n2: Foo { prop1: 'val1' }) return n2",
    expectedColumns = Vector("n2"),
    expectedRows = Seq(
      Vector(Expr.Node(0L, Set(Symbol("Foo")), Map(Symbol("prop1") -> Expr.Str("val1"))))
    ),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "match (n3) return n3",
    expectedColumns = Vector("n3"),
    expectedCannotFail = true,
    expectedRows = Seq(
      Vector(Expr.Node(0L, Set(Symbol("Foo")), Map(Symbol("prop1") -> Expr.Str("val1"))))
    ),
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "match (n4: Foo)-[:REL]->(m { bar: 'baz' }) return m",
    expectedColumns = Vector("m"),
    expectedRows = Seq.empty,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "merge (n5: Foo)-[:REL]->(m { bar: 'baz' }) return m",
    expectedColumns = Vector("m"),
    expectedRows = Seq(Vector(Expr.Node(2L, Set(), Map(Symbol("bar") -> Expr.Str("baz"))))),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "merge (n6: Foo)-[:REL]->(m { bar: 'baz' }) return m",
    expectedColumns = Vector("m"),
    expectedRows = Seq(Vector(Expr.Node(2L, Set(), Map(Symbol("bar") -> Expr.Str("baz"))))),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "match (n7) return n7",
    expectedColumns = Vector("n7"),
    expectedRows = Seq(
      Vector(Expr.Node(0L, Set(Symbol("Foo")), Map(Symbol("prop1") -> Expr.Str("val1")))),
      Vector(Expr.Node(2L, Set(), Map(Symbol("bar") -> Expr.Str("baz")))),
      Vector(Expr.Node(3L, Set(Symbol("Foo")), Map()))
    ),
    ordered = false,
    expectedCannotFail = true,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "match (n1: Foo) return n1.matched",
    expectedColumns = Vector("n1.matched"),
    expectedRows = Seq(Vector(Expr.Null)),
    ordered = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    """merge (n: Foo)-[:REL]->(m { bar: 'baz' })
      |on create set n.matched = false
      |on match set n.matched = true
      |return null""".stripMargin,
    expectedColumns = Vector("null"),
    expectedRows = Seq(Vector(Expr.Null)),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    """merge (n: Foo)<-[:REL]-(m { bar: 'baz' })
      |on create set n.matched = false
      |on match set n.matched = true
      |return null""".stripMargin,
    expectedColumns = Vector("null"),
    expectedRows = Seq(Vector(Expr.Null)),
    expectedIsReadOnly = false,
    expectedIsIdempotent = false,
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "match (n: Foo) return n.matched",
    expectedColumns = Vector("n.matched"),
    expectedRows = Seq(Vector(Expr.True), Vector(Expr.False), Vector(Expr.Null)),
    ordered = false,
    expectedCanContainAllNodeScan = true
  )
}

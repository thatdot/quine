package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherMutate extends CypherHarness("cypher-mutate-tests") {

  import QuineIdImplicitConversions._

  describe("`CREATE` query clause") {
    testQuery(
      "MATCH (n) RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "CREATE (a:PERSON {name: 'Andrea'}) RETURN a",
      expectedColumns = Vector("a"),
      expectedRows = Seq(
        Vector(Expr.Node(0L, Set(Symbol("PERSON")), Map(Symbol("name") -> Expr.Str("Andrea"))))
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false
    )

    testQuery(
      "CREATE (a {name: 'Bob', age: '43'}) RETURN a",
      expectedColumns = Vector("a"),
      expectedRows = Seq(
        Vector(
          Expr.Node(
            1L,
            Set(),
            Map(Symbol("name") -> Expr.Str("Bob"), Symbol("age") -> Expr.Str("43"))
          )
        )
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false
    )

    testQuery(
      "MATCH (n) RETURN n.name, n.age, labels(n)",
      expectedColumns = Vector("n.name", "n.age", "labels(n)"),
      expectedRows = Seq(
        Vector(Expr.Str("Andrea"), Expr.Null, Expr.List(Vector(Expr.Str("PERSON")))),
        Vector(Expr.Str("Bob"), Expr.Str("43"), Expr.List(Vector.empty))
      ),
      expectedCanContainAllNodeScan = true,
      ordered = false
    )

    testQuery(
      "MATCH (n: PERSON) RETURN n.name",
      expectedColumns = Vector("n.name"),
      expectedRows = Seq(Vector(Expr.Str("Andrea"))),
      ordered = false,
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (a {name: 'Bob'}) SET a: PERSON",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (a: PERSON) RETURN a.name",
      expectedColumns = Vector("a.name"),
      expectedRows = Seq(Vector(Expr.Str("Andrea")), Vector(Expr.Str("Bob"))),
      ordered = false,
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (a), (b) WHERE id(a) < id(b) CREATE (a)-[:FRIENDS]->(b) RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false,
      expectedCanContainAllNodeScan = true
    )
  }

  testQuery(
    "create (:Person { name: 'bob' })-[:LOVES]->(:Person { name: 'sherry' })",
    expectedColumns = Vector.empty,
    expectedRows = Seq.empty,
    expectedIsReadOnly = false,
    expectedIsIdempotent = false
  )

  // See QU-224
  describe("`WHERE` clauses where anchor-like constraints depend on other variables") {
    testQuery(
      "match (n), (m) where id(n) = 33 and id(m) = 34 set n.foo = 34, m.bar = 'hello'",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedCanContainAllNodeScan = false
    )

    testQuery(
      "match (n), (m) where id(n) = m.foo and id(m) = 33 return n.bar",
      expectedColumns = Vector("n.bar"),
      expectedRows = Seq(Vector(Expr.Str("hello"))),
      expectedCanContainAllNodeScan = true
    )
  }

  describe("Don't remove the label property when otherwise overriding all properties") {
    // Set a label and some properties
    testQuery(
      "match (n) where id(n) = 78 set n: Person, n = { name: 'Greta' }",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedIsIdempotent = true
    )

    // Destructively set properties (this causes all previous properties to be removed)
    testQuery(
      "match (n) where id(n) = 78 set n = { name: 'Greta Garbo' }",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedIsIdempotent = true
    )

    // Label should not have been affected
    testQuery(
      "match (n) where id(n) = 78 return labels(n)",
      expectedColumns = Vector("labels(n)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Str("Person"))))),
      expectedIsReadOnly = true,
      expectedIsIdempotent = true
    )
  }

  /* Broken because we assume `set` always returns no rows. That's not true
   * though - it only returns 0 rows when it is the last clause
   */
  testQuery(
    "match (n:Person) set n.is_bob = (n.name = 'bob') return 1",
    expectedColumns = Vector("1"),
    expectedRows = Seq(Vector(Expr.Integer(1L))),
    skip = true
  )

  /* Broken because `set` doesn't actually mutate the context, only the data.
   * This requires a bit of book-keeping for mutating queries.
   */
  testQuery(
    "match (n:Person) set n.is_sherry = (n.name = 'sherry') return n.is_sherry",
    expectedColumns = Vector("n.is_sherry"),
    expectedRows = Seq(Vector(Expr.True), Vector(Expr.False)),
    skip = true
  )

  /* Broken for a subtly different reason that above: the standard Cypher
   * behaviour is to eagerly do all the `set`'s before ever starting the
   * `return`.
   */
  testQuery(
    """match (n:Person)--(m:Person)
      |order by n.name
      |set n.prop = n.name, m.prop = n.name
      |return n.prop""".stripMargin,
    expectedColumns = Vector("n.prop"),
    expectedRows = Seq(Vector(Expr.Str("sherry")), Vector(Expr.Str("sherry"))),
    skip = true
  )
}

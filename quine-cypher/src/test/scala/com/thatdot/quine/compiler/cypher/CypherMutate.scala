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
      expectedCanContainAllNodeScan = true,
    )

    testQuery(
      "CREATE (a:Person {name: 'Andrea'}) RETURN a",
      expectedColumns = Vector("a"),
      expectedRows = Seq(
        Vector(Expr.Node(0L, Set(Symbol("Person")), Map(Symbol("name") -> Expr.Str("Andrea")))),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )

    testQuery(
      "CREATE (a {name: 'Bob', age: '43'}) RETURN a",
      expectedColumns = Vector("a"),
      expectedRows = Seq(
        Vector(
          Expr.Node(
            1L,
            Set(),
            Map(Symbol("name") -> Expr.Str("Bob"), Symbol("age") -> Expr.Str("43")),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )

    testQuery(
      "MATCH (n) RETURN n.name, n.age, labels(n)",
      expectedColumns = Vector("n.name", "n.age", "labels(n)"),
      expectedRows = Seq(
        Vector(Expr.Str("Andrea"), Expr.Null, Expr.List(Vector(Expr.Str("Person")))),
        Vector(Expr.Str("Bob"), Expr.Str("43"), Expr.List(Vector.empty)),
      ),
      expectedCanContainAllNodeScan = true,
      ordered = false,
    )

    testQuery(
      "MATCH (n:Person) RETURN n.name",
      expectedColumns = Vector("n.name"),
      expectedRows = Seq(Vector(Expr.Str("Andrea"))),
      ordered = false,
      expectedCanContainAllNodeScan = true,
    )

    testQuery(
      "MATCH (a {name: 'Bob'}) SET a:Person RETURN labels(a), a.name",
      expectedColumns = Vector("labels(a)", "a.name"),
      expectedRows = Seq(
        Vector(Expr.List(Vector(Expr.Str("Person"))), Expr.Str("Bob")),
      ),
      expectedIsReadOnly = false,
      expectedCanContainAllNodeScan = true,
    )

    testQuery(
      "MATCH (a:Person) RETURN a.name",
      expectedColumns = Vector("a.name"),
      expectedRows = Seq(Vector(Expr.Str("Andrea")), Vector(Expr.Str("Bob"))),
      ordered = false,
      expectedCanContainAllNodeScan = true,
    )

    testQuery(
      "MATCH (a), (b) WHERE id(a) < id(b) CREATE (a)-[:FRIENDS]->(b) RETURN count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false,
      expectedCanContainAllNodeScan = true,
    )

    testQuery(
      "create (:Person { name: 'bob' })-[:LOVES]->(:Person { name: 'sherry' })",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
  }

  // See QU-224
  describe("`WHERE` clauses where anchor-like constraints depend on other variables") {
    testQuery(
      "match (n), (m) where id(n) = 33 and id(m) = 34 set n.foo = 34, m.bar = 'hello'",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedCanContainAllNodeScan = false,
    )

    testQuery(
      "match (n), (m) where id(n) = m.foo and id(m) = 33 return n.bar",
      expectedColumns = Vector("n.bar"),
      expectedRows = Seq(Vector(Expr.Str("hello"))),
      expectedCanContainAllNodeScan = true,
    )
  }

  describe("Special behavior of label mutations") {
    // Set a label and some properties
    testQuery(
      "match (n) where id(n) = 78 set n:Person, n = { name: 'Greta' }",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // Destructively set properties (this causes all previous properties to be removed)
    testQuery(
      "match (n) where id(n) = 78 set n = { name: 'Greta Garbo' }",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // Label should not have been affected
    testQuery(
      "match (n) where id(n) = 78 return labels(n)",
      expectedColumns = Vector("labels(n)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Str("Person"))))),
      expectedIsReadOnly = true,
      expectedIsIdempotent = true,
    )
  }

  describe("`SET` and `REMOVE` query clauses") {
    // SET single property (no history)
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n.p1 = 'p1'
        |RETURN n.p1""".stripMargin,
      expectedColumns = Vector("n.p1"),
      expectedRows = Seq(
        Vector(
          Expr.Str("p1"),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // add a label
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n:Address
        |RETURN labels(n)""".stripMargin,
      expectedColumns = Vector("labels(n)"),
      expectedRows = Seq(
        Vector(
          Expr.List(Expr.Str("Address")),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // SET multiple properties (no history)
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n.p2 = 'p2',
        |    n.p3 = 'p3'
        |RETURN n.p1, n.p2, n.p3""".stripMargin,
      expectedColumns = Vector("n.p1", "n.p2", "n.p3"),
      expectedRows = Seq(
        Vector(
          Expr.Str("p1"),
          Expr.Str("p2"),
          Expr.Str("p3"),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // SET += property map (with history)
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n += {
        | p1: 'p1 updated',
        | p4: 'p4',
        | p5: 'p5',
        | p6: 'p6'
        |}
        |RETURN n.p1, n.p2, n.p3, n.p4, n.p5, n.p6""".stripMargin,
      expectedColumns = Vector("n.p1", "n.p2", "n.p3", "n.p4", "n.p5", "n.p6"),
      expectedRows = Seq(
        Vector(
          Expr.Str("p1 updated"),
          Expr.Str("p2"),
          Expr.Str("p3"),
          Expr.Str("p4"),
          Expr.Str("p5"),
          Expr.Str("p6"),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // SET to null (delete property)
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n.p1 = null
        |RETURN properties(n)""".stripMargin,
      expectedColumns = Vector("properties(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "p2" -> Expr.Str("p2"),
            "p3" -> Expr.Str("p3"),
            "p4" -> Expr.Str("p4"),
            "p5" -> Expr.Str("p5"),
            "p6" -> Expr.Str("p6"),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // SET multiple to null (delete properties)
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n.p2 = null,
        |    n.p3 = null
        |RETURN properties(n)""".stripMargin,
      expectedColumns = Vector("properties(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "p4" -> Expr.Str("p4"),
            "p5" -> Expr.Str("p5"),
            "p6" -> Expr.Str("p6"),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // SET += to delete multiple properties
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n += {
        |    p4: null,
        |    p5: null
        |}
        |RETURN properties(n)""".stripMargin,
      expectedColumns = Vector("properties(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "p6" -> Expr.Str("p6"),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // SET = property map (with history)
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n = {
        |    a1: 'p1',
        |    a2: 'p2',
        |    a3: 'p3'
        |}
        |RETURN properties(n), labels(n)""".stripMargin,
      expectedColumns = Vector("properties(n)", "labels(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "a1" -> Expr.Str("p1"),
            "a2" -> Expr.Str("p2"),
            "a3" -> Expr.Str("p3"),
          ),
          Expr.List(Expr.Str("Address")),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )
    // REMOVE a property
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |REMOVE n.a3
        |RETURN properties(n), labels(n)""".stripMargin,
      expectedColumns = Vector("properties(n)", "labels(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "a1" -> Expr.Str("p1"),
            "a2" -> Expr.Str("p2"),
          ),
          Expr.List(Expr.Str("Address")),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // remove and update in one SET +=
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n += {
        |    a1: 'p1 prime',
        |    a2: null
        |}
        |RETURN properties(n)""".stripMargin,
      expectedColumns = Vector("properties(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "a1" -> Expr.Str("p1 prime"),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )
    // SET += for no-op
    testQuery(
      """
            |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
            |SET n += {}
            |RETURN properties(n)""".stripMargin,
      expectedColumns = Vector("properties(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "a1" -> Expr.Str("p1 prime"),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )
    // SET += a map parameter
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n += $mapParam
        |RETURN properties(n)""".stripMargin,
      parameters = Map("mapParam" -> Expr.Map("a3" -> Expr.Str("p3"))),
      expectedColumns = Vector("properties(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            "a1" -> Expr.Str("p1 prime"),
            "a3" -> Expr.Str("p3"),
          ),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )
    // REMOVE a label, add another
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |REMOVE n:Address
        |SET n:Address2
        |RETURN labels(n)""".stripMargin,
      expectedColumns = Vector("labels(n)"),
      expectedRows = Seq(
        Vector(
          Expr.List(Expr.Str("Address2")),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    // remove all properties with SET = {}
    testQuery(
      """
        |MATCH (n) WHERE id(n) = idFrom("P Sherman 42 Wallaby Way, Syndey")
        |SET n = {}
        |RETURN properties(n), labels(n)""".stripMargin,
      expectedColumns = Vector("properties(n)", "labels(n)"),
      expectedRows = Seq(
        Vector(
          Expr.Map.empty,
          Expr.List(Expr.Str("Address2")),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )

    testQueryStaticAnalysis(
      "MATCH (n) WHERE id(n) = idFrom(0) SET n = { x: n.x + 1 }",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = false, // QU-1843, should be flagged as non-idempotent
      expectedCanContainAllNodeScan = false,
      skip = true,
    )

    testQueryStaticAnalysis(
      "MATCH (n) WHERE id(n) = idFrom(0) SET n.x = n.x + 1",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = false, // QU-1843, should be flagged as non-idempotent
      expectedCanContainAllNodeScan = false,
      skip = true,
    )

    testQueryStaticAnalysis(
      "MATCH (n), (m) WHERE id(n) = idFrom(0) AND id(m) = idFrom(1) SET n.x = m.x + 1, m.x = n.x + 1",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = false, // QU-1843, should be flagged as non-idempotent
      expectedCanContainAllNodeScan = false,
      skip = true,
    )
    /* Broken because we assume `set` always returns no rows. That's not true
     * though - it only returns 0 rows when it is the last clause
     */
    testQuery(
      "match (n:Person) set n.is_bob = (n.name = 'bob') return 1",
      expectedColumns = Vector("1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      skip = true,
    )

    /* Broken because `set` doesn't actually mutate the context, only the data.
     * This requires a bit of book-keeping for mutating queries.
     */
    testQuery(
      "match (n:Person) set n.is_sherry = (n.name = 'sherry') return n.is_sherry",
      expectedColumns = Vector("n.is_sherry"),
      expectedRows = Seq(Vector(Expr.True), Vector(Expr.False)),
      skip = true,
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
      skip = true,
    )

    // SET n.x = 0, n.x = n.x + 1 interprets as `SET n.x = 0, n.x = null + 1`,
    // which means `n.x` ends up as `null` rather than 1. This is not desirable.
    testQuery(
      "CREATE (n) SET n.x = 0, n.x = n.x + 1 RETURN n.x",
      expectedColumns = Vector("n.x"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
      skip = true, // currently null
    )

    // By using WITH, we can force the RHS `n.x` to be evaluated after the first SET
    // rather than before
    testQuery(
      "CREATE (n) SET n.x = 0 WITH n SET n.x = n.x + 1 RETURN n.x",
      expectedColumns = Vector("n.x"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )

    // SET b.x should update both a and b, but we don't yet do that level of analysis
    testQuery(
      """CREATE (a{x: -1}) WITH a, a AS b
        |SET b.x = 1
        |RETURN a.x, b.x""".stripMargin,
      expectedColumns = Vector("a.x", "b.x"),
      expectedRows = Seq(Vector(Expr.Integer(1), Expr.Integer(1))), // currently -1, 1
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
      skip = true,
    )

    // a.x should ideally reflect the update to the node from the setProperty call,
    // but we don't yet do that level of analysis
    testQuery(
      """CREATE (a{x: 1}) WITH a
        |CALL create.setProperty(a, 'x', 2)
        |RETURN a.x""".stripMargin,
      expectedColumns = Vector("a.x"),
      expectedRows = Seq(Vector(Expr.Integer(2))), // currently 1
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
      skip = true,
    )

    testQuery(
      """CREATE (a{x: 1})
        |SET a.x = 200
        |RETURN a[$propertyParam] AS notStaticProp""".stripMargin,
      parameters = Map("propertyParam" -> Expr.Str("x")),
      expectedColumns = Vector("notStaticProp"),
      expectedRows = Seq(Vector(Expr.Integer(200))),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
  }

  describe("atomic adders") {
    // incrementCounter (no history)
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(1230020) CALL incrementCounter(n, 'count', 20) YIELD count RETURN count",
      expectedColumns = Vector("count"),
      expectedRows = Seq(
        Vector(
          Expr.Integer(20L),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
    // incrementCounter (with history)
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(1230020) CALL incrementCounter(n, 'count', 15) YIELD count RETURN count",
      expectedColumns = Vector("count"),
      expectedRows = Seq(
        Vector(
          Expr.Integer(35L),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
    // 2-ary incrementCounter
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(1230020) CALL incrementCounter(n, 'count') YIELD count RETURN count",
      expectedColumns = Vector("count"),
      expectedRows = Seq(
        Vector(
          Expr.Integer(36L),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )

    // int.add (no history)
    testQuery(
      "CALL int.add(idFrom(1230021), 'count', 15) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.Integer(15L),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
    // int.add (with history)
    testQuery(
      "CALL int.add(idFrom(1230021), 'count', 30) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.Integer(45L),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
    // 2-ary int.add
    testQuery(
      "CALL int.add(idFrom(1230021), 'count') YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.Integer(46L),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )

    // float.add (no history)
    testQuery(
      "CALL float.add(idFrom(1230021.0), 'count', 1.5) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.Floating(1.5),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
    // float.add (with history)
    testQuery(
      "CALL float.add(idFrom(1230021.0), 'count', 3.0) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.Floating(4.5),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )
    // 2-ary float.add
    testQuery(
      "CALL float.add(idFrom(1230021.0), 'count') YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.Floating(5.5),
        ),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = false,
    )

    // set.insert (no history)
    testQuery(
      "CALL set.insert(idFrom(12232), 'set-unary', 1.5) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Floating(1.5))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.insert (with history, homogeneous)
    testQuery(
      "CALL set.insert(idFrom(12232), 'set-unary', 2.0) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Floating(1.5), Expr.Floating(2.0))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.insert (with history, homogeneous, deduplicated)
    testQuery(
      "CALL set.insert(idFrom(12232), 'set-unary', 1.50) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Floating(1.5), Expr.Floating(2.0))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.insert (with history, heterogenous)
    testQuery(
      "CALL set.insert(idFrom(12232), 'set-unary', 'foo') YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Floating(1.5), Expr.Floating(2.0), Expr.Str("foo"))),
        ),
      ),
      expectedIsReadOnly = false,
    )

    // set.insert (no history)
    testQuery(
      "CALL set.union(idFrom(12232), 'set-union', [3, 2]) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Integer(3), Expr.Integer(2))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.insert (with history, homogeneous)
    testQuery(
      "CALL set.union(idFrom(12232), 'set-union', [1]) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Integer(3), Expr.Integer(2), Expr.Integer(1))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.insert (with history, homogeneous, partially-deduplicated)
    testQuery(
      "CALL set.union(idFrom(12232), 'set-union', [7, 1]) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Integer(3), Expr.Integer(2), Expr.Integer(1), Expr.Integer(7))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.insert (with history, homogeneous, fully-deduplicated)
    testQuery(
      "CALL set.union(idFrom(12232), 'set-union', [7, 3]) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Integer(3), Expr.Integer(2), Expr.Integer(1), Expr.Integer(7))),
        ),
      ),
      expectedIsReadOnly = false,
    )
    // set.union (with history, heterogenous, partially-deduplicated)
    testQuery(
      "CALL set.union(idFrom(12232), 'set-union', [7, 3, 'jason']) YIELD result RETURN result",
      expectedColumns = Vector("result"),
      expectedRows = Seq(
        Vector(
          Expr.List(Vector(Expr.Integer(3), Expr.Integer(2), Expr.Integer(1), Expr.Integer(7), Expr.Str("jason"))),
        ),
      ),
      expectedIsReadOnly = false,
    )

  }

  describe("setProperty procedure") {
    testQuery(
      """
        |// Setup query
        |MATCH (n) WHERE id(n) = idFrom(42424242)
        |CALL create.setProperty(n, 'test', [1, '2', false])
        |WITH id(n) as nId
        |// re-match to ensure updates will be reflected
        |MATCH (n) WHERE id(n) = nId
        |RETURN n.test
        |""".stripMargin,
      Vector("n.test"),
      Vector(
        Vector(Expr.List(Expr.Integer(1), Expr.Str("2"), Expr.False)),
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
    )
  }
}

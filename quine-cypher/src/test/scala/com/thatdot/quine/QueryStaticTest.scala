package com.thatdot.quine

import com.thatdot.quine.compiler.cypher.{
  CypherHarness,
  MyReverse,
  MyUnwind,
  registerUserDefinedFunction,
  registerUserDefinedProcedure
}

/** Tests that operate on a Query without actually running it. */
class QueryStaticTest extends CypherHarness("query-static-tests") {

  describe("static output of compiled query") {

    registerUserDefinedFunction(MyReverse)
    registerUserDefinedProcedure(MyUnwind)

    testQueryStaticAnalysis(
      queryText = "match (n) return n",
      expectedIsReadOnly = true,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "match (n) set n.foo = 1 return n",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "match (n) set n.foo = datetime() return n",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = false,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "RETURN count(*)",
      expectedIsReadOnly = true,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "MATCH () RETURN count(*)",
      expectedIsReadOnly = true,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "UNWIND [] AS n RETURN count(*)",
      expectedIsReadOnly = true,
      expectedCannotFail = true,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "WITH 1 + 2 AS x WHERE x > 2 RETURN x",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "MATCH (p)-[:has_mother]->(m) RETURN p.first, m.first",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "RETURN myreverse(\"hello\") AS REV",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "CALL myunwind([1,2,\"hello\",null])",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "MATCH (a), (b), (p), (e), (c) MERGE (a)-[:A]->(b)-[:B]->(p)-[:C]->(c)-[:D]->(e)",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "return duration({ days: 24 })",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "return datetime()",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = false,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "return datetime('2000-01-01T00:00:00.000Z')",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = false, // unfortunately any use of datetime is considered nonidempotent
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "return duration('PT20.345S')",
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "create (Sup)",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = false,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText = "merge (n1: Foo { prop1: 'val1' }) return n1",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = false,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "MATCH (a), (b) WHERE id(a) < id(b) CREATE (a)-[:FRIENDS]->(b) RETURN count(*)",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = true
    )
    testQueryStaticAnalysis(
      queryText = "match (n), (m) where id(n) = 33 and id(m) = 34 set n.foo = 34, m.bar = 'hello'",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    testQueryStaticAnalysis(
      queryText =
        "UNWIND [{}, {}] AS s MATCH (a), (b) WHERE strId(a) = s.foo AND strId(b) = s.bar CREATE (a)-[:baz]->(b)",
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
  }
}

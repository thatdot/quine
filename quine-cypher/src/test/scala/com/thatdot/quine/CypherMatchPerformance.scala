package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherMatchPerformance extends CypherHarness("cypher-match-performance") {

  /* Start by creating a complete graph with 9 vertices and one edge type. Then,
   * match a path through that graph, where every step of the graph is uniquely
   * constrained by a property on the node.
   *
   * If we only use properties in the match for filtering afterwards, that would
   * mean we'd have 10! matches to filter. However, if we filter _during_ the
   * match, we should be able to do this quite efficiently. The main thing being
   * tested here is that the second query doesn't time out.
   *
   * See QU-179
   */
  describe("Use properties in `MATCH` pattern for early filtering") {
    testQuery(
      """unwind range(1, 9) as newId
        |match (newNode) where id(newNode) = newId
        |set newNode.prop = newId
        |with newId, newNode
        |unwind range(1, newId-1) as existingId
        |match (existingNode) where id(existingNode) = existingId
        |create (existingNode)-[:edge]->(newNode)""".stripMargin,
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
    )

    testQuery(
      "match ({ prop: 5 })--({ prop: 9 })--({ prop: 4 })--({ prop: 2 })--({ prop: 6 })--({ prop: 8 })--({ prop: 1 })--({ prop: 7 })--({ prop: 3 }) return count(*)",
      expectedColumns = Vector("count(*)"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCanContainAllNodeScan = true,
    )
  }
}

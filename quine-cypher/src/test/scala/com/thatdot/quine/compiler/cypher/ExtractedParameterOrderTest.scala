package com.thatdot.quine.compiler.cypher

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.Expr

/** The front-end hands back auto-extracted literals in identity-hash order, which varies between
  * JVMs. Compilation must not: two hosts compiling the same text have to assign the same
  * parameter indices, or their plans (and the cluster-ingest decomposition digest computed over
  * them) disagree, and every cross-host fragment channel refuses. This pins the canonical
  * (name-sorted) order; it cannot exercise a second JVM, so it asserts the invariant the sort
  * establishes rather than the cross-JVM agreement itself.
  */
class ExtractedParameterOrderTest extends AnyFunSuite {

  test("auto-extracted literal parameters are indexed in name order") {
    // Two extracted string literals; the front-end names them in traversal order
    // (AUTOSTRING0='mtr' first in the text, AUTOSTRING1='cl'), so the sorted assignment must put
    // 'mtr' at the first fixed slot regardless of the map order the front-end returned.
    val compiled = compiler.cypher.compile(
      """MATCH (mtr) WHERE id(mtr) = idFrom('mtr', $that.host)
        |MATCH (cl) WHERE id(cl) = idFrom('cl', $that.cluster)
        |CREATE (cl)-[:HAS_METRICS]->(mtr)""".stripMargin,
      unfixedParameters = Seq("that"),
    )
    assert(compiled.fixedParameters.params == Vector(Expr.Str("mtr"), Expr.Str("cl")))
  }
}

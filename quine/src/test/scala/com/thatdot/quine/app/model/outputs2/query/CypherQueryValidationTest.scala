package com.thatdot.quine.app.model.outputs2.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.graph.cypher.CypherException

class CypherQueryValidationTest extends AnyFunSuite with Matchers {

  test("valid cypher compiles without error") {
    val compiled =
      CypherQuery.validateAndCompile("MATCH (n) WHERE id(n) = $that RETURN n", "that", allowAllNodeScan = false)
    compiled should not be null
  }

  test("invalid cypher syntax throws CypherException") {
    val ex = intercept[CypherException] {
      CypherQuery.validateAndCompile("THIS IS NOT VALID CYPHER", "that", allowAllNodeScan = false)
    }
    ex.pretty should include("Invalid input")
  }

  test("all-node-scan query without opt-in throws AllNodeScanException") {
    val ex = intercept[AllNodeScanException] {
      CypherQuery.validateAndCompile("MATCH (n) SET n.touched = true", "that", allowAllNodeScan = false)
    }
    ex.getMessage should include("full node scan")
  }

  test("all-node-scan query with opt-in compiles successfully") {
    val compiled = CypherQuery.validateAndCompile("MATCH (n) SET n.touched = true", "that", allowAllNodeScan = true)
    compiled should not be null
  }
}

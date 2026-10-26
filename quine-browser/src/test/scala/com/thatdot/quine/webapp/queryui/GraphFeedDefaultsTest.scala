package com.thatdot.quine.webapp.queryui

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.resultspanel.{TapPoint, TapPointQuery, TapTarget}

class GraphFeedDefaultsTest extends AnyFunSuite with Matchers {

  import GraphFeedDefaults.{suggestion, Suggestion}

  private def distinctId(query: String) = TapPointQuery(query, patternMode = Some(TapPointQuery.DistinctIdMode))
  private def multipleValues(query: String) = TapPointQuery(query, patternMode = Some("MULTIPLE_VALUES"))

  private def raw(query: String): Suggestion = suggestion(TapPoint.Raw, Some(multipleValues(query)))
  private def rawDistinct(query: String): Suggestion = suggestion(TapPoint.Raw, Some(distinctId(query)))
  private def enriched(query: String): Suggestion =
    suggestion(TapPoint.PostEnrichment("out"), Some(TapPointQuery(query)))

  private val unfilledRaw = Suggestion("MATCH (n) WHERE id(n) = $data.", "", " RETURN n")
  private val unfilledTopLevel = Suggestion("MATCH (n) WHERE id(n) = $", "", " RETURN n")

  test("a DistinctId pattern's single id column is looked up under $data at the raw point") {
    rawDistinct("MATCH (n:Person) RETURN DISTINCT id(n)") shouldBe
    Suggestion("MATCH (n) WHERE id(n) = $data.`", "id(n)", "` RETURN n")
  }

  test("a backticked column keeps its backticks outside the fill") {
    val s = rawDistinct("MATCH (n:Person) RETURN DISTINCT id(n)")
    s.text shouldBe "MATCH (n) WHERE id(n) = $data.`id(n)` RETURN n"
    s.fill should not include "`"
  }

  test("an aliased id column needs no quoting") {
    rawDistinct("MATCH (n) RETURN DISTINCT strId(n) AS nodeId").text shouldBe
    "MATCH (n) WHERE id(n) = $data.nodeId RETURN n"
  }

  test("several id columns are unwound into one lookup each, non-id columns left out") {
    raw("MATCH (a)-[:PAID]->(b) RETURN id(a) AS payer, id(b) AS payee, a.amount AS amt") shouldBe
    Suggestion("UNWIND [", "$data.payer, $data.payee", "] AS nodeId\nMATCH (n) WHERE id(n) = nodeId\nRETURN n")
  }

  test("after a transformation or enrichment the columns are top-level parameters") {
    val pattern = Some(distinctId("MATCH (n) RETURN DISTINCT id(n) AS id"))
    suggestion(TapPoint.PreEnrichment("out"), pattern).text shouldBe "MATCH (n) WHERE id(n) = $id RETURN n"
    val enrichment = Some(TapPointQuery("MATCH (n) WHERE id(n) = $that.data.id RETURN id(n) AS id"))
    suggestion(TapPoint.PostEnrichment("out"), enrichment).text shouldBe "MATCH (n) WHERE id(n) = $id RETURN n"
  }

  test("a returned node variable is reached through its serialized id field") {
    enriched("MATCH (n) WHERE id(n) = $that.data.id MATCH (n)-[:OWNS]->(acct:Account) RETURN n, acct").text shouldBe
    "UNWIND [$n.id, $acct.id] AS nodeId\nMATCH (n) WHERE id(n) = nodeId\nRETURN n"
  }

  test("a bare variable that is not a node pattern is not treated as a node") {
    enriched("UNWIND $that.data.ids AS x RETURN x, id(x) AS xid").text shouldBe
    "MATCH (n) WHERE id(n) = $xid RETURN n"
    enriched("MATCH (n) WHERE id(n) = $that.data.id WITH n AS person RETURN person") shouldBe unfilledTopLevel
  }

  test("a variable only ever inside a function call's parens is not a node") {
    enriched("MATCH (a)-[r:KNOWS]->(b) RETURN r, type(r)").fill shouldBe ""
    enriched("MATCH (a)-[r:KNOWS]->(b) RETURN r, count(r) AS c").fill shouldBe ""
    enriched("MATCH(a)-[r:KNOWS]->(b) RETURN a, b").fill shouldBe "$a.id, $b.id"
  }

  test("a property is a value, not an ID, whatever it is called") {
    raw("MATCH (a) RETURN a.foo_id") shouldBe unfilledRaw
    raw("MATCH (n) RETURN n.user_id AS user_id, n.paid AS paid, n.valid AS valid") shouldBe unfilledRaw
    raw("MATCH (n:Order) RETURN n.id AS id") shouldBe unfilledRaw
    enriched("MATCH (n) WHERE id(n) = $that.data.id RETURN n.ref AS ref") shouldBe unfilledTopLevel
  }

  test("a lone column is not taken for an ID on the strength of being alone") {
    raw("MATCH (a)-[:edge]->(b) WITH id(b) AS bleh RETURN bleh") shouldBe unfilledRaw
    raw("MATCH (a)-[:edge]->(b) WITH b.name AS bleh RETURN bleh") shouldBe unfilledRaw
    enriched("MATCH (n)-[r]->(m) WHERE id(n) = $that.data.id RETURN r") shouldBe unfilledTopLevel
    enriched("MATCH (n) WHERE id(n) = $that.data.id RETURN count(n) AS cnt") shouldBe unfilledTopLevel
  }

  test("comments and a trailing semicolon do not reach the fill") {
    rawDistinct("MATCH (n:Person) RETURN DISTINCT id(n) // c").text shouldBe
    "MATCH (n) WHERE id(n) = $data.`id(n)` RETURN n"
    rawDistinct("MATCH (n:Person) RETURN DISTINCT id(n);").text shouldBe
    "MATCH (n) WHERE id(n) = $data.`id(n)` RETURN n"
    raw("MATCH (n:Person) RETURN id(n) AS nodeId // the node").text shouldBe
    "MATCH (n) WHERE id(n) = $data.nodeId RETURN n"
    enriched("MATCH (n) WHERE id(n) = $that.data.id RETURN n /* the node */ AS person;").text shouldBe
    "MATCH (n) WHERE id(n) = $person.id RETURN n"
    // A comment inside the expression is part of the server's key, and not an id call.
    rawDistinct("MATCH (n:Person) RETURN DISTINCT id( /* c */ n)").text shouldBe
    "MATCH (n) WHERE id(n) = $data.`id( /* c */ n)` RETURN n"
    raw("MATCH (n:Person) RETURN id( /* c */ n)") shouldBe unfilledRaw
  }

  test("a variable rebound along the way is not taken for the node it once was") {
    raw("MATCH (n:Person) WITH n.name AS n RETURN n") shouldBe unfilledRaw
    raw("MATCH (n:Person) WITH id(n) AS n RETURN n") shouldBe unfilledRaw
    enriched(
      "MATCH (a) WHERE id(a) = $that.data.id CALL { MATCH (m:Person) RETURN m.name AS m LIMIT 1 } RETURN m",
    ) shouldBe unfilledTopLevel
  }

  test("a variable parenthesised outside a pattern, and a UNION, get the unfilled shape") {
    enriched("MATCH (a)-[r]->(b) WHERE id(a) = $that.data.id AND a.x = '(r)' RETURN r") shouldBe unfilledTopLevel
    enriched("MATCH (n) WHERE id(n) = $that.data.id AND (n) IS NOT NULL RETURN n") shouldBe unfilledTopLevel
    enriched("MATCH (a) WHERE id(a) = $that.data.id RETURN a.name AS b UNION MATCH (b) RETURN b") shouldBe
    unfilledTopLevel
  }

  test("a column the scanner cannot name yields the unfilled shape, never an empty key") {
    enriched("MATCH (n) WHERE id(n) = $that.data.id RETURN n.x AS skip") shouldBe unfilledTopLevel
    enriched("MATCH (n) WHERE id(n) = $that.data.id RETURN id(n) AS skip") shouldBe unfilledTopLevel
    rawDistinct("MATCH (n) RETURN DISTINCT id(n) AS skip") shouldBe unfilledRaw
    enriched("MATCH (n) WHERE id(n) = $that.data.id RETURN *") shouldBe unfilledTopLevel
  }

  test("when no column can be inferred the fill is left to the user, the shape still offered") {
    raw("MATCH (n) RETURN n.a AS a, n.b AS b") shouldBe unfilledRaw
    raw("MATCH (n) RETURN *") shouldBe unfilledRaw
    raw("not cypher at all") shouldBe unfilledRaw
    suggestion(TapPoint.Raw, None) shouldBe unfilledRaw
    suggestion(TapPoint.PostEnrichment("out"), None) shouldBe unfilledTopLevel
  }

  test("default names follow the point and stay unique") {
    val raw = TapTarget.StandingQuery("fraud", TapPoint.Raw)
    val post = TapTarget.StandingQuery("fraud", TapPoint.PostEnrichment("slack"))
    GraphFeedDefaults.name(raw, Set.empty) shouldBe "fraud"
    GraphFeedDefaults.name(raw, Set("fraud")) shouldBe "fraud-2"
    GraphFeedDefaults.name(raw, Set("fraud", "fraud-2")) shouldBe "fraud-3"
    GraphFeedDefaults.name(post, Set("fraud")) shouldBe "fraud-slack"
    GraphFeedDefaults.name(TapTarget.StandingQuery("fraud", TapPoint.PreEnrichment("slack")), Set.empty) shouldBe
    "fraud-slack"
  }
}

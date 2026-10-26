package com.thatdot.quine.webapp.queryui

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.resultspanel.{TapPoint, TapPointQuery}
import com.thatdot.quine.webapp.util.CypherReturnClause

class IdColumnInferenceTest extends AnyFunSuite with Matchers {

  import ReturnClauseIdInference.{distinctId, idTyped}

  private def columns(query: String) = CypherReturnClause.columns(query).get
  private def pattern(query: String, mode: String) = TapPointQuery(query, patternMode = Some(mode))
  private def multipleValues(query: String) = pattern(query, "MULTIPLE_VALUES")
  private def distinct(query: String) = pattern(query, TapPointQuery.DistinctIdMode)
  private def text(query: String) = TapPointQuery(query)

  test("idTyped vouches for id calls and node-pattern variables, nothing else") {
    val query = "MATCH (a)-[r:KNOWS]->(b) RETURN id(a), b, r, type(r), b.name AS name"
    idTyped(text(query), columns(query)) shouldBe
    Vector(IdRef("id(a)", throughNode = false), IdRef("b", throughNode = true))
  }

  test("distinctId vouches for a DistinctId pattern's one column, by mode rather than by text") {
    val query = "MATCH (p:Person) RETURN DISTINCT id(p) AS person"
    distinctId(distinct(query), columns(query)) shouldBe Vector(IdRef("person", throughNode = false))
    distinctId(multipleValues(query), columns(query)) shouldBe Vector.empty
    distinctId(text(query), columns(query)) shouldBe Vector.empty
  }

  test("distinctId vouches for nothing unless the text reads as exactly one named column") {
    val two = "MATCH (p:Person) RETURN DISTINCT id(p), p.name"
    distinctId(distinct(two), columns(two)) shouldBe Vector.empty
    val keyword = "MATCH (p:Person) RETURN DISTINCT id(p) AS skip"
    distinctId(distinct(keyword), columns(keyword)) shouldBe Vector.empty
  }

  test("a node variable rebound by a WITH before the RETURN is not vouched for") {
    val shadowed = "MATCH (n:Person) WITH n.name AS n RETURN n"
    idTyped(text(shadowed), columns(shadowed)) shouldBe Vector.empty
    val idUnderTheSameName = "MATCH (n:Person) WITH id(n) AS n RETURN n"
    idTyped(text(idUnderTheSameName), columns(idUnderTheSameName)) shouldBe Vector.empty
    // Rebinding another name leaves the node variable vouched for.
    val other = "MATCH (n:Person)-->(m) WITH n, m.name AS m RETURN n, m"
    idTyped(text(other), columns(other)) shouldBe Vector(IdRef("n", throughNode = true))
    // A CALL subquery binds for the enclosing query too.
    val subquery = "MATCH (a:Person) CALL { MATCH (m:Person) RETURN m.name AS m LIMIT 1 } RETURN m"
    idTyped(text(subquery), columns(subquery)) shouldBe Vector.empty
  }

  test("a variable parenthesised in a string or an expression is not taken for a node") {
    val inString = "MATCH (a)-[r]->(b) WHERE a.x = '(r)' RETURN r"
    idTyped(text(inString), columns(inString)) shouldBe Vector.empty
    val asExpression = "MATCH (n) WHERE (n) IS NOT NULL RETURN n"
    idTyped(text(asExpression), columns(asExpression)) shouldBe Vector.empty
  }

  test("a UNION query vouches for nothing, whatever its branches return") {
    ReturnClauseIdInference.idColumns(
      TapPoint.PostEnrichment("out"),
      Some(text("MATCH (a) RETURN a.name AS b UNION MATCH (b) RETURN b")),
    ) shouldBe Vector.empty
    ReturnClauseIdInference.idColumns(
      TapPoint.PostEnrichment("out"),
      Some(text("MATCH (a) RETURN id(a) AS x UNION MATCH (b) RETURN id(b) AS x")),
    ) shouldBe Vector.empty
  }

  test("a column that merely looks like an ID is not vouched for") {
    ReturnClauseIdInference.idColumns(TapPoint.Raw, Some(multipleValues("MATCH (a) RETURN a.foo_id"))) shouldBe
    Vector.empty
    ReturnClauseIdInference.idColumns(
      TapPoint.Raw,
      Some(multipleValues("MATCH (n) RETURN n.id AS id, n.userId AS userId, n.paid AS paid")),
    ) shouldBe Vector.empty
  }

  test("a lone column is not vouched for, whether or not it happens to hold an ID") {
    ReturnClauseIdInference.idColumns(
      TapPoint.Raw,
      Some(multipleValues("MATCH (a)-[:edge]->(b) WITH id(b) AS bleh RETURN bleh")),
    ) shouldBe Vector.empty
    ReturnClauseIdInference.idColumns(
      TapPoint.Raw,
      Some(multipleValues("MATCH (a)-[:edge]->(b) WITH b.name AS bleh RETURN bleh")),
    ) shouldBe Vector.empty
    ReturnClauseIdInference.idColumns(TapPoint.PostEnrichment("out"), Some(text("RETURN $that.data.id AS id"))) shouldBe
    Vector.empty
  }

  test("the first tier to find anything wins") {
    ReturnClauseIdInference.idColumns(
      TapPoint.Raw,
      Some(multipleValues("MATCH (n) RETURN id(n), n.userId AS userId")),
    ) shouldBe
    Vector(IdRef("id(n)", throughNode = false))
    // The mode tier wins over the typed one even where both would agree, and does so with
    // the alias the text would read as a plain name.
    ReturnClauseIdInference.idColumns(
      TapPoint.Raw,
      Some(distinct("MATCH (n) RETURN DISTINCT strId(n) AS ref")),
    ) shouldBe
    Vector(IdRef("ref", throughNode = false))
  }

  test("no query, no RETURN, or RETURN * means nothing is found") {
    ReturnClauseIdInference.idColumns(TapPoint.Raw, None) shouldBe Vector.empty
    ReturnClauseIdInference.idColumns(TapPoint.Raw, Some(text("MATCH (n) SET n.x = 1"))) shouldBe Vector.empty
    ReturnClauseIdInference.idColumns(TapPoint.Raw, Some(text("MATCH (n) RETURN *"))) shouldBe Vector.empty
    ReturnClauseIdInference.idColumns(TapPoint.Raw, Some(distinct("MATCH (n) RETURN *"))) shouldBe Vector.empty
  }

  test("orElse falls back only when the first inference finds nothing") {
    val fixed: IdColumnInference = (_, _) => Vector(IdRef("fixed", throughNode = false))
    IdColumnInference.none.orElse(fixed).idColumns(TapPoint.Raw, None) shouldBe Vector(
      IdRef("fixed", throughNode = false),
    )
    ReturnClauseIdInference
      .orElse(fixed)
      .idColumns(TapPoint.Raw, Some(text("MATCH (n) RETURN id(n)"))) shouldBe Vector(
      IdRef("id(n)", throughNode = false),
    )
  }

  test("the renderer takes whatever an inference returns") {
    val fixed: IdColumnInference =
      (_, _) => Vector(IdRef("weird key", throughNode = true), IdRef("plain", throughNode = false))
    GraphFeedDefaults.suggestion(TapPoint.PostEnrichment("out"), None, fixed).text shouldBe
    "UNWIND [$`weird key`.id, $plain] AS nodeId\nMATCH (n) WHERE id(n) = nodeId\nRETURN n"
  }
}

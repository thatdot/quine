package com.thatdot.quine.webapp.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.util.CypherReturnClause.Column

class CypherReturnClauseTest extends AnyFunSuite with Matchers {

  test("an unaliased item is named by its verbatim expression text") {
    CypherReturnClause.columns("MATCH (n:Person) RETURN DISTINCT id(n)") shouldBe
    Some(Vector(Column("id(n)", "id(n)")))
  }

  test("an aliased item is named by its alias, the expression kept separately") {
    CypherReturnClause.columns("MATCH (n) RETURN DISTINCT strId(n) AS nodeId") shouldBe
    Some(Vector(Column("strId(n)", "nodeId")))
  }

  test("items split at top-level commas only") {
    val cols = CypherReturnClause.columns(
      "MATCH (a)-[:KNOWS]->(b) RETURN id(a) AS a, id(b) AS b, [a.x, b.y] AS pair, {k: a.name, j: 1} AS m",
    )
    cols.map(_.map(_.name)) shouldBe Some(Vector("a", "b", "pair", "m"))
    cols.map(_.map(_.expression)) shouldBe Some(Vector("id(a)", "id(b)", "[a.x, b.y]", "{k: a.name, j: 1}"))
  }

  test("the last top-level RETURN wins over WITH and subquery RETURNs") {
    val q = "MATCH (n) WITH n CALL { WITH n MATCH (n)-->(m) RETURN count(m) AS c } RETURN n.name AS name, c"
    CypherReturnClause.columns(q).map(_.map(_.name)) shouldBe Some(Vector("name", "c"))
  }

  test("ORDER BY, SKIP, and LIMIT are not part of the projection") {
    CypherReturnClause
      .columns("MATCH (n) RETURN n.age AS age, id(n) ORDER BY age DESC SKIP 1 LIMIT 5")
      .map(_.map(_.name)) shouldBe Some(Vector("age", "id(n)"))
  }

  test("keywords inside string literals, backticks, and comments are ignored") {
    val q =
      """MATCH (n) // RETURN nothing here
        |WHERE n.note = 'RETURN a, b' AND n.`RETURN, AS` = "x"
        |/* RETURN id(n) AS wrong */
        |RETURN n.`odd, name` AS `quoted AS alias`, 'a,b' AS lit""".stripMargin
    CypherReturnClause.columns(q) shouldBe
    Some(Vector(Column("n.`odd, name`", "quoted AS alias"), Column("'a,b'", "lit")))
  }

  test("comments at a column's ends are trimmed off, one inside stays, as the server names them") {
    CypherReturnClause.columns("MATCH (n) RETURN id(n) // trailing") shouldBe Some(Vector(Column("id(n)", "id(n)")))
    CypherReturnClause.columns("MATCH (n) RETURN id( /* c */ n)") shouldBe
    Some(Vector(Column("id( /* c */ n)", "id( /* c */ n)")))
    CypherReturnClause.columns("MATCH (n) RETURN id(n) /* c */ AS x /* d */") shouldBe
    Some(Vector(Column("id(n)", "x")))
    CypherReturnClause.columns("MATCH (n) RETURN id(n) AS nodeId // the node") shouldBe
    Some(Vector(Column("id(n)", "nodeId")))
    CypherReturnClause.columns("MATCH (n) RETURN id(n) /* block */ AS nodeId") shouldBe
    Some(Vector(Column("id(n)", "nodeId")))
    CypherReturnClause.columns("MATCH (n) RETURN id(n)\n// on its own line") shouldBe
    Some(Vector(Column("id(n)", "id(n)")))
    CypherReturnClause.columns("MATCH (n) RETURN n.name, // first\nid(n)") shouldBe
    Some(Vector(Column("n.name", "n.name"), Column("id(n)", "id(n)")))
    CypherReturnClause.columns("MATCH (n) RETURN /* all */ DISTINCT id(n)") shouldBe
    Some(Vector(Column("id(n)", "id(n)")))
  }

  test("a top-level semicolon ends the projection") {
    CypherReturnClause.columns("MATCH (n) RETURN id(n);") shouldBe Some(Vector(Column("id(n)", "id(n)")))
    CypherReturnClause.columns("MATCH (n) RETURN id(n) AS x; // done") shouldBe Some(Vector(Column("id(n)", "x")))
    CypherReturnClause.columns("MATCH (n) RETURN n.`a;b` AS `c;d`, 'e;f' AS g;") shouldBe
    Some(Vector(Column("n.`a;b`", "c;d"), Column("'e;f'", "g")))
  }

  test("names bound by AS ahead of the final RETURN are reported, the RETURN's own aliases not") {
    CypherReturnClause.renamedBeforeReturn(
      "MATCH (n) WITH n.name AS n, 1 AS `x y` UNWIND [1] AS u // AS c\nRETURN n AS n, u AS v",
    ) shouldBe Set("n", "x y", "u")
    CypherReturnClause.renamedBeforeReturn("MATCH (n) RETURN n") shouldBe Set.empty
    CypherReturnClause.renamedBeforeReturn("MATCH (n) WHERE n.as = 'AS z' WITH n.`AS q` AS w RETURN w") shouldBe
    Set("w")
  }

  test("a name a CALL subquery binds counts as bound ahead of the RETURN") {
    CypherReturnClause.renamedBeforeReturn("MATCH (a) CALL { MATCH (m) RETURN m.name AS m } RETURN m") shouldBe
    Set("m")
  }

  test("a top-level UNION is seen, one in a string or a property name is not") {
    CypherReturnClause.isUnion("MATCH (a) RETURN a.name AS b UNION MATCH (b) RETURN b") shouldBe true
    CypherReturnClause.isUnion("MATCH (a) RETURN a.name AS b union all MATCH (b) RETURN b") shouldBe true
    CypherReturnClause.isUnion("MATCH (n) WHERE n.x = 'UNION' RETURN n.union AS u") shouldBe false
  }

  test("a variable is node-bound when every parenthesised use opens a pattern") {
    import CypherReturnClause.boundByNodePattern
    boundByNodePattern("MATCH (n) RETURN n", "n") shouldBe true
    boundByNodePattern("MATCH(n) RETURN n", "n") shouldBe true
    boundByNodePattern("OPTIONAL MATCH (n:Label {k: 1}) RETURN n", "n") shouldBe true
    boundByNodePattern("MATCH (a)-[:K]->(b) RETURN b", "b") shouldBe true
    boundByNodePattern("MATCH (a)<-[:K]-(b) RETURN b", "b") shouldBe true
    boundByNodePattern("MATCH (a), (b) RETURN b", "b") shouldBe true
    boundByNodePattern("MATCH p = (a)-->() RETURN a", "a") shouldBe true
    boundByNodePattern("MERGE (n {k: 1}) RETURN n", "n") shouldBe true
    boundByNodePattern("MATCH (n) WHERE (n)-[:K]->() RETURN n", "n") shouldBe true
    boundByNodePattern("MATCH (n) WHERE EXISTS((n)-->()) RETURN n", "n") shouldBe true
    // A function's argument says nothing either way.
    boundByNodePattern("MATCH (n) WHERE id(n) = $x RETURN n", "n") shouldBe true
    boundByNodePattern("MATCH (a)-[r]->(b) RETURN type(r)", "r") shouldBe false
  }

  test("a variable parenthesised as an expression, or only inside a string or comment, is not node-bound") {
    import CypherReturnClause.boundByNodePattern
    boundByNodePattern("MATCH (a)-[r]->(b) WHERE a.x = '(r)' RETURN r", "r") shouldBe false
    boundByNodePattern("MATCH (a)-[r]->(b) // (r) is the edge\nRETURN r", "r") shouldBe false
    boundByNodePattern("MATCH (a)-[r]->(b) WHERE (r).since > 1 RETURN r", "r") shouldBe false
    boundByNodePattern("MATCH (n) WHERE (n) IS NOT NULL RETURN n", "n") shouldBe false
    boundByNodePattern("MATCH (n) RETURN 1 + (n)", "n") shouldBe false
    boundByNodePattern("MATCH (`my node`) RETURN `my node`", "my node") shouldBe false
  }

  test("a doubled backtick inside a quoted alias is one literal backtick") {
    CypherReturnClause.columns("RETURN n.x AS `a``b`") shouldBe Some(Vector(Column("n.x", "a`b")))
  }

  test("RETURN * has unknowable columns") {
    CypherReturnClause.columns("MATCH (n) RETURN *") shouldBe None
    CypherReturnClause.columns("MATCH (n) RETURN *, n.x") shouldBe None
  }

  test("a query without a top-level RETURN has no columns") {
    CypherReturnClause.columns("MATCH (n) SET n.seen = true") shouldBe None
    CypherReturnClause.columns("") shouldBe None
  }

  test("keyword matching is case-insensitive and whole-word") {
    CypherReturnClause.columns("match (n) return n.returned as returnedAs").map(_.map(_.name)) shouldBe
    Some(Vector("returnedAs"))
  }

  test("a property or parameter spelling a keyword is a name, not a keyword") {
    CypherReturnClause
      .columns("MATCH (n) RETURN n.order AS o, n.limit AS l, n.skip, n.as, n.distinct, $order + $limit AS sum")
      .map(_.map(_.name)) shouldBe
    Some(Vector("o", "l", "n.skip", "n.as", "n.distinct", "sum"))
    CypherReturnClause.columns("MATCH (n) RETURN n.union AS u ORDER BY u LIMIT $limit").map(_.map(_.name)) shouldBe
    Some(Vector("u"))
  }

  test("id(x) and strId(x) columns expose their node variable, bare variables expose themselves") {
    val cols = CypherReturnClause.columns("MATCH (n)-->(m) RETURN id(n), STRID( m ) AS mid, m, n.p, null").get
    cols.map(_.idOf) shouldBe Vector(Some("n"), Some("m"), None, None, None)
    cols.map(_.variable) shouldBe Vector(None, None, Some("m"), None, None)
  }
}

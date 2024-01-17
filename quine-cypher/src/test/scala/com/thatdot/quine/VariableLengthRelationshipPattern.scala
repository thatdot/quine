package com.thatdot.quine

import scala.concurrent.{Await, Future}

import org.apache.pekko.stream.scaladsl.Sink

import cats.implicits._

import com.thatdot.quine.compiler.cypher.{CypherHarness, queryCypherValues}
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.model.QuineValue

class VariableLengthRelationshipPattern extends CypherHarness("variable-length-relationship-pattern-person") {

  case class Person(
    first: String,
    parent: Option[idProv.CustomIdType],
    id: idProv.CustomIdType = idProv.newCustomId()
  )

  val people: List[Person] = {
    // people 0-3 are related in linear sequence
    val _0 =
      Person(first = "0", parent = None)
    val _1 =
      Person(first = "1", parent = Some(_0.id))
    val _2 =
      Person(first = "2", parent = Some(_1.id))
    val _3 =
      Person(first = "3", parent = Some(_2.id))

    // people a-c are related to each other forming a loop
    val aId = idProv.newCustomId()
    val bId = idProv.newCustomId()
    val cId = idProv.newCustomId()
    val a = Person(first = "loop-a", parent = Some(cId), id = aId)
    val b = Person(first = "loop-b", parent = Some(aId), id = bId)
    val c = Person(first = "loop-c", parent = Some(bId), id = cId)

    List(_0, _1, _2, _3, a, b, c)
  }

  // if this setup test fails, nothing else in this suite is expected to pass
  describe("Load some test data") {
    it("should insert some people and their parents") {
      import QuineIdImplicitConversions._
      Future.traverse(people) { (person: Person) =>
        graph.literalOps.setProp(person.id, "first", QuineValue.Str(person.first)) zip
        person.parent.traverse(parent => graph.literalOps.addEdge(person.id, parent, "parent"))
      } as assert(true)
    }
  }

  describe("Variable length relationship patterns") {
    testQuery(
      "MATCH (n)-[:parent*1]->(m) WHERE NOT m.first STARTS WITH 'loop-' RETURN n.first AS n, m.first AS m ORDER BY n.first",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("1"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("2"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)<-[:parent*1]-(m) WHERE NOT m.first STARTS WITH 'loop-' RETURN n.first AS n, m.first AS m ORDER BY n", // reverse direction
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("0"), Expr.Str("1")),
        Vector(Expr.Str("1"), Expr.Str("2")),
        Vector(Expr.Str("2"), Expr.Str("3"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*2]->(m) RETURN n.first AS n, m.first AS m ORDER BY m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("2"), Expr.Str("0")),
        Vector(Expr.Str("3"), Expr.Str("1")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*1..2]->(m) RETURN n.first AS n, m.first AS m ORDER BY m, n",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("1"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("2")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*2..]->(m) RETURN n.first AS n, m.first AS m ORDER BY n, m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("2"), Expr.Str("0")),
        Vector(Expr.Str("3"), Expr.Str("0")),
        Vector(Expr.Str("3"), Expr.Str("1")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*3..]->(m) RETURN n.first AS n, m.first AS m ORDER BY n, m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("3"), Expr.Str("0")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*..2]->(m) RETURN n.first AS n, m.first AS m ORDER BY n, m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("1"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("2")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-b"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*2..3]->(m) RETURN n.first AS n, m.first AS m ORDER BY n, m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("2"), Expr.Str("0")),
        Vector(Expr.Str("3"), Expr.Str("0")),
        Vector(Expr.Str("3"), Expr.Str("1")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)-[:parent*]->(m) RETURN n.first AS n, m.first AS m ORDER BY n, m", // shorthand means "1 or more"
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("1"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("0")),
        Vector(Expr.Str("2"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("0")),
        Vector(Expr.Str("3"), Expr.Str("1")),
        Vector(Expr.Str("3"), Expr.Str("2")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)<-[:parent*]-(m) RETURN n.first AS n, m.first AS m ORDER BY n, m", // reverse
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("0"), Expr.Str("1")),
        Vector(Expr.Str("0"), Expr.Str("2")),
        Vector(Expr.Str("0"), Expr.Str("3")),
        Vector(Expr.Str("1"), Expr.Str("2")),
        Vector(Expr.Str("1"), Expr.Str("3")),
        Vector(Expr.Str("2"), Expr.Str("3")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-a"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-b"), Expr.Str("loop-c")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-a")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-b")),
        Vector(Expr.Str("loop-c"), Expr.Str("loop-c"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (n)<-[:parent*4..6]-(m) RETURN n.first AS n, m.first AS m ORDER BY n, m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(),
      expectedCanContainAllNodeScan = true
    )
  }

}

class VariableLengthRelationshipPatternHarryPotter
    extends CypherHarness("variable-length-relationship-pattern-harry-potter") {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(
      queryCypherValues(
        List(
          "CREATE",
          " (jamessr:Person {name: \"James Potter\", born: 1960})<-[:has_father]-(harry:Person {name: \"Harry Potter\", born: 1980})-[:has_mother]->(:Person {name: \"Lily Potter\", born: 1960}),",
          " (arthur:Person {name: \"Arthur Weasley\", born: 1950})<-[:has_father]-(ginny:Person {name: \"Ginny Weasley\", born: 1981})-[:has_mother]->(molly:Person {name: \"Molly Weasley\", born: 1949}),",
          " (arthur)<-[:has_father]-(ron:Person {name: \"Ron Weasley\", born: 1980})-[:has_mother]->(molly),",
          " (harry)<-[:has_father]-(:Person {name: \"James Sirius Potter\", born: 2003})-[:has_mother]->(ginny),",
          " (harry)<-[:has_father]-(:Person {name: \"Albus Severus Potter\", born: 2005})-[:has_mother]->(ginny),",
          " (harry)<-[:has_father]-(:Person {name: \"Lily Luna\", born: 2007})-[:has_mother]->(ginny),",
          " (ron)<-[:has_father]-(:Person {name: \"Rose Weasley\", born: 2005})-[:has_mother]->(hermione:Person {name: \"Hermione Granger\", born: 1979}),",
          " (ron)<-[:has_father]-(:Person {name: \"Hugo Weasley\", born: 2008})-[:has_mother]->(hermione);"
        ).mkString
      )(graph).results.runWith(Sink.ignore),
      timeout.duration
    )
    ()
  }

  describe("Variable length relationship patterns ~ Harry Potter dataset") {
    testQuery(
      "MATCH (n)-[:has_father*2..]->(m) RETURN n.name AS n, m.name AS m ORDER BY n, m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("Albus Severus Potter"), Expr.Str("James Potter")),
        Vector(Expr.Str("Hugo Weasley"), Expr.Str("Arthur Weasley")),
        Vector(Expr.Str("James Sirius Potter"), Expr.Str("James Potter")),
        Vector(Expr.Str("Lily Luna"), Expr.Str("James Potter")),
        Vector(Expr.Str("Rose Weasley"), Expr.Str("Arthur Weasley"))
      ),
      expectedCanContainAllNodeScan = true
    )
  }

  describe("Aliased variable length relationship") {
    testQuery(
      "MATCH (n)-[e*]->(m) RETURN n.name AS n, m.name AS m, [r in e | type(r)]  AS relation ORDER BY n, m",
      expectedColumns = Vector("n", "m", "relation"),
      expectedRows = Seq(
        Vector(
          Expr.Str("Albus Severus Potter"),
          Expr.Str("Arthur Weasley"),
          Expr.List(Expr.Str("has_mother"), Expr.Str("has_father"))
        ),
        Vector(Expr.Str("Albus Severus Potter"), Expr.Str("Ginny Weasley"), Expr.List(Expr.Str("has_mother"))),
        Vector(Expr.Str("Albus Severus Potter"), Expr.Str("Harry Potter"), Expr.List(Expr.Str("has_father"))),
        Vector(
          Expr.Str("Albus Severus Potter"),
          Expr.Str("James Potter"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_father"))
        ),
        Vector(
          Expr.Str("Albus Severus Potter"),
          Expr.Str("Lily Potter"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_mother"))
        ),
        Vector(
          Expr.Str("Albus Severus Potter"),
          Expr.Str("Molly Weasley"),
          Expr.List(Expr.Str("has_mother"), Expr.Str("has_mother"))
        ),
        Vector(Expr.Str("Ginny Weasley"), Expr.Str("Arthur Weasley"), Expr.List(Expr.Str("has_father"))),
        Vector(Expr.Str("Ginny Weasley"), Expr.Str("Molly Weasley"), Expr.List(Expr.Str("has_mother"))),
        Vector(Expr.Str("Harry Potter"), Expr.Str("James Potter"), Expr.List(Expr.Str("has_father"))),
        Vector(Expr.Str("Harry Potter"), Expr.Str("Lily Potter"), Expr.List(Expr.Str("has_mother"))),
        Vector(
          Expr.Str("Hugo Weasley"),
          Expr.Str("Arthur Weasley"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_father"))
        ),
        Vector(Expr.Str("Hugo Weasley"), Expr.Str("Hermione Granger"), Expr.List(Expr.Str("has_mother"))),
        Vector(
          Expr.Str("Hugo Weasley"),
          Expr.Str("Molly Weasley"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_mother"))
        ),
        Vector(Expr.Str("Hugo Weasley"), Expr.Str("Ron Weasley"), Expr.List(Expr.Str("has_father"))),
        Vector(
          Expr.Str("James Sirius Potter"),
          Expr.Str("Arthur Weasley"),
          Expr.List(Expr.Str("has_mother"), Expr.Str("has_father"))
        ),
        Vector(Expr.Str("James Sirius Potter"), Expr.Str("Ginny Weasley"), Expr.List(Expr.Str("has_mother"))),
        Vector(Expr.Str("James Sirius Potter"), Expr.Str("Harry Potter"), Expr.List(Expr.Str("has_father"))),
        Vector(
          Expr.Str("James Sirius Potter"),
          Expr.Str("James Potter"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_father"))
        ),
        Vector(
          Expr.Str("James Sirius Potter"),
          Expr.Str("Lily Potter"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_mother"))
        ),
        Vector(
          Expr.Str("James Sirius Potter"),
          Expr.Str("Molly Weasley"),
          Expr.List(Expr.Str("has_mother"), Expr.Str("has_mother"))
        ),
        Vector(
          Expr.Str("Lily Luna"),
          Expr.Str("Arthur Weasley"),
          Expr.List(Expr.Str("has_mother"), Expr.Str("has_father"))
        ),
        Vector(Expr.Str("Lily Luna"), Expr.Str("Ginny Weasley"), Expr.List(Expr.Str("has_mother"))),
        Vector(Expr.Str("Lily Luna"), Expr.Str("Harry Potter"), Expr.List(Expr.Str("has_father"))),
        Vector(
          Expr.Str("Lily Luna"),
          Expr.Str("James Potter"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_father"))
        ),
        Vector(
          Expr.Str("Lily Luna"),
          Expr.Str("Lily Potter"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_mother"))
        ),
        Vector(
          Expr.Str("Lily Luna"),
          Expr.Str("Molly Weasley"),
          Expr.List(Expr.Str("has_mother"), Expr.Str("has_mother"))
        ),
        Vector(Expr.Str("Ron Weasley"), Expr.Str("Arthur Weasley"), Expr.List(Expr.Str("has_father"))),
        Vector(Expr.Str("Ron Weasley"), Expr.Str("Molly Weasley"), Expr.List(Expr.Str("has_mother"))),
        Vector(
          Expr.Str("Rose Weasley"),
          Expr.Str("Arthur Weasley"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_father"))
        ),
        Vector(Expr.Str("Rose Weasley"), Expr.Str("Hermione Granger"), Expr.List(Expr.Str("has_mother"))),
        Vector(
          Expr.Str("Rose Weasley"),
          Expr.Str("Molly Weasley"),
          Expr.List(Expr.Str("has_father"), Expr.Str("has_mother"))
        ),
        Vector(Expr.Str("Rose Weasley"), Expr.Str("Ron Weasley"), Expr.List(Expr.Str("has_father")))
      ),
      expectedCanContainAllNodeScan = true
    )
  }
}

class VariableLengthRelationshipPatternMatrix extends CypherHarness("variable-length-relationship-pattern-matrix") {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(
      queryCypherValues(
        """create
        (Neo:Crew {name:'Neo'}),
        (Morpheus:Crew {name: 'Morpheus'}),
        (Trinity:Crew {name: 'Trinity'}),
        (Cypher:Crew:Matrix {name: 'Cypher'}),
        (Smith:Matrix {name: 'Agent Smith'}),
        (Architect:Matrix {name:'The Architect'}),
        (Neo)-[:KNOWS]->(Morpheus),
        (Neo)-[:LOVES]->(Trinity),
        (Morpheus)-[:KNOWS]->(Trinity),
        (Morpheus)-[:KNOWS]->(Cypher),
        (Cypher)-[:KNOWS]->(Smith),
        (Smith)-[:CODED_BY]->(Architect)"""
      )(graph).results.runWith(Sink.ignore),
      timeout.duration
    )
    ()
  }

  describe("Variable length relationship patterns ~ Matrix dataset") {
    testQuery(
      "match (n)--()--()--(m) WHERE n.name = 'Morpheus' RETURN n.name AS n, m.name AS m ORDER BY m.name",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("Morpheus"), Expr.Str("Morpheus")),
        Vector(Expr.Str("Morpheus"), Expr.Str("Morpheus")),
        Vector(Expr.Str("Morpheus"), Expr.Str("The Architect"))
      ),
      expectedCanContainAllNodeScan = true
    )
    testQuery(
      "match (n)-[*3..3]-(m) WHERE n.name = 'Morpheus' RETURN n.name AS n, m.name AS m ORDER BY m.name",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("Morpheus"), Expr.Str("Morpheus")),
        Vector(Expr.Str("Morpheus"), Expr.Str("Morpheus")),
        Vector(Expr.Str("Morpheus"), Expr.Str("The Architect"))
      ),
      expectedCanContainAllNodeScan = true
    )
    testQuery(
      "match (n)--()--()--()--(m) WHERE n.name = 'Morpheus' RETURN n.name AS n, m.name AS m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("Morpheus"), Expr.Str("Cypher")),
        Vector(Expr.Str("Morpheus"), Expr.Str("Cypher"))
      ),
      expectedCanContainAllNodeScan = true
    )
    testQuery(
      "match (n)-[*4..4]-(m) WHERE n.name = 'Morpheus' RETURN n.name AS n, m.name AS m",
      expectedColumns = Vector("n", "m"),
      expectedRows = Seq(
        Vector(Expr.Str("Morpheus"), Expr.Str("Cypher")),
        Vector(Expr.Str("Morpheus"), Expr.Str("Cypher"))
      ),
      expectedCanContainAllNodeScan = true
    )
  }

  describe("variable length relationships with constraints") {
    testQuery(
      "MATCH (a)-[*1]->(b)-->(c) RETURN a.name, c.name",
      expectedColumns = Vector("a.name", "c.name"),
      expectedRows = Seq(
        Vector(Expr.Str("Morpheus"), Expr.Str("Agent Smith")),
        Vector(Expr.Str("Neo"), Expr.Str("Cypher")),
        Vector(Expr.Str("Neo"), Expr.Str("Trinity")),
        Vector(Expr.Str("Cypher"), Expr.Str("The Architect"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (a)-[:foo|:bar]->(b)-[:bar*]->(c) RETURN null",
      expectedColumns = Vector("null"),
      expectedRows = Seq.empty,
      expectedCanContainAllNodeScan = true
    )
  }
}

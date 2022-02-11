package com.thatdot.quine.compiler.cypher

import scala.concurrent.{Await, Future}

import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.model.QuineValue

class OrderedEdgesTest extends CypherHarness("ordered-edges-test") {

  case class Person(id: Long, name: String, knows: Seq[Person] = Seq.empty)

  val alice: Person = Person(1L, "Alice")
  val bob: Person = Person(2L, "Bob")
  val carol: Person = Person(3L, "Carol")
  val david: Person = Person(4L, "David", Seq(alice, bob, carol))

  def addPerson(p: Person): Future[Unit] =
    graph.literalOps.setProp(idProv.customIdToQid(p.id), "name", QuineValue.Str(p.name))

  describe("The edge collection") {
    it("should load some edges with literal ops") {
      Await.ready(
        for {
          _ <- Future.traverse(david.knows)(addPerson)
          _ <- addPerson(david)
          _ <- Future.traverse(david.knows)(p =>
            graph.literalOps.addEdge(idProv.customIdToQid(david.id), idProv.customIdToQid(p.id), "knows")
          )
        } yield (),
        timeout.duration
      )
    }

    testQuery(
      "MATCH (d)-[:knows]->(p) WHERE id(d) = 4 RETURN p.name",
      expectedColumns = Vector("p.name"),
      expectedRows = david.knows.reverse.map(p => Vector(Expr.Str(p.name)))
    )

  }

  describe("We should be able to page through edges in reverse-insertion order") {
    val root = "idFrom('root')"
    val totalEdges = 15
    testQuery(
      s"""
        MATCH (n) WHERE id(n) = $root
        SET n: Root
        WITH n
        UNWIND range(1, $totalEdges) AS x
        MATCH (m) WHERE id(m) = idFrom("other", x)
        SET m.index = x
        CREATE (n)-[:edge]->(m)
      """,
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false
    )

    val pageSize = 5
    def page(pageNo: Int) = testQuery(
      s"""
        MATCH (n)-[:edge]->(m) WHERE id(n) = $root
        RETURN m.index
        SKIP ${pageNo * pageSize}
        LIMIT $pageSize
      """,
      expectedColumns = Vector("m.index"),
      expectedRows = Range(totalEdges - pageNo * pageSize, totalEdges - (pageNo + 1) * pageSize, -1) map (i =>
        Vector(Expr.Integer(i.toLong))
      )
    )

    for (i <- 0 to 2) page(i)

  }

}

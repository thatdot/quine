package com.thatdot.quine.compiler.cypher

import scala.concurrent.Future

import cats.syntax.functor._
import org.scalactic.source.Position

import com.thatdot.quine.graph.cypher.{Expr, Value}

class CypherShortestPath extends CypherHarness("cypher-shortestpath-tests") {

  import idProv.ImplicitConverters._

  private val n1 = Expr.Node(1L, Set.empty, Map.empty)
  private val n2 = Expr.Node(2L, Set.empty, Map.empty)
  private val n3 = Expr.Node(3L, Set.empty, Map.empty)
  private val n4 = Expr.Node(4L, Set.empty, Map.empty)
  private val n5 = Expr.Node(5L, Set.empty, Map.empty)

  private val e12 = Expr.Relationship(1L, Symbol("foo"), Map.empty, 2L)
  private val e13 = Expr.Relationship(1L, Symbol("bar"), Map.empty, 3L)
  private val e23 = Expr.Relationship(2L, Symbol("foo"), Map.empty, 3L)
  private val e35 = Expr.Relationship(3L, Symbol("foo"), Map.empty, 5L)
  private val e43 = Expr.Relationship(4L, Symbol("foo"), Map.empty, 3L)
  private val e51 = Expr.Relationship(5L, Symbol("baz"), Map.empty, 1L)
  private val e54 = Expr.Relationship(5L, Symbol("foo"), Map.empty, 4L)

  // if this setup test fails, nothing else in this suite is expected to pass
  it("should load some test nodes") {
    Future.traverse(List(e12, e13, e23, e35, e43, e51, e54)) { case Expr.Relationship(from, name, _, to) =>
      graph.literalOps.addEdge(from, to, name.name)
    } as assert(true)
  }

  final private def testShortestPath(
    shortestPathText: String,
    from: Long,
    to: Long,
    expectedValue: Option[Value],
    skip: Boolean = false
  )(implicit
    pos: Position
  ): Unit =
    testQuery(
      queryText = s"MATCH (n), (m) WHERE id(n) = $from AND id(m) = $to RETURN $shortestPathText",
      expectedColumns = Vector(shortestPathText),
      expectedRows = expectedValue.map(Vector(_)).toSeq,
      skip = skip
    )

  testShortestPath(
    "shortestPath((n)-[*]-(m))",
    from = 1L,
    to = 4L,
    expectedValue = Some(Expr.Path(n1, Vector(e51 -> n5, e54 -> n4)))
  )

  testShortestPath(
    "shortestPath((n)-[:foo*]-(m))",
    from = 1L,
    to = 4L,
    expectedValue = Some(Expr.Path(n1, Vector(e12 -> n2, e23 -> n3, e43 -> n4)))
  )

  testShortestPath(
    "shortestPath((n)-[:foo*]->(m))",
    from = 1L,
    to = 4L,
    expectedValue = Some(Expr.Path(n1, Vector(e12 -> n2, e23 -> n3, e35 -> n5, e54 -> n4)))
  )

  testShortestPath(
    "shortestPath((n)-[:foo*..4]->(m))",
    from = 1L,
    to = 4L,
    expectedValue = Some(Expr.Path(n1, Vector(e12 -> n2, e23 -> n3, e35 -> n5, e54 -> n4)))
  )

  testShortestPath(
    "shortestPath((n)-[:foo*..3]->(m))",
    from = 1L,
    to = 4L,
    expectedValue = None
  )

  testShortestPath(
    "shortestPath((n)-[:foo]-(m))",
    from = 1L,
    to = 2L,
    expectedValue = Some(Expr.Path(n1, Vector(e12 -> n2)))
  )

  testShortestPath(
    "shortestPath((n)<-[:foo|bar*]-(m))",
    from = 5L,
    to = 1L,
    expectedValue = Some(Expr.Path(n5, Vector(e35 -> n3, e13 -> n1)))
  )

  testShortestPath(
    "shortestPath((n)-[*]->(m))",
    from = 5L,
    to = 1L,
    expectedValue = Some(Expr.Path(n5, Vector(e51 -> n1)))
  )
}

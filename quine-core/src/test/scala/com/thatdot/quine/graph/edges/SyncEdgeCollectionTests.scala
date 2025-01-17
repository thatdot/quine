package com.thatdot.quine.graph.edges
import org.scalactic.source.Position
import org.scalatest.Assertion

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.{DomainEdge, HalfEdge}

abstract class SyncEdgeCollectionTests extends EdgeCollectionTests {
  type F[A] = A
  type S[A] = Iterator[A]

  def loadEdgeCollection(qid: QuineId, edges: Iterable[HalfEdge]): AbstractEdgeCollection.Aux[F, S] = {
    val edgeCollection = newEdgeCollection(qid)
    edges.foreach(edgeCollection.addEdge)
    edgeCollection
  }

  def assertEmpty[A](actual: Iterator[A])(implicit pos: Position): Assertion = actual shouldBe empty

  def valueOf[A](fa: A): A = fa

  def checkContains(localEdges: Seq[HalfEdge], domainEdges: Seq[DomainEdge], qid: QuineId): Boolean = {
    val ec = loadEdgeCollection(qid, localEdges)
    ec.hasUniqueGenEdges(domainEdges)
  }
}

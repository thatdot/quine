package com.thatdot.quine.graph.edges

import org.scalactic.source.Position
import org.scalatest.Assertion

import com.thatdot.quine.model.QuineId

class UnorderedEdgeCollectionTests extends SyncEdgeCollectionTests {

  def newEdgeCollection(qid: QuineId): SyncEdgeCollection = new UnorderedEdgeCollection(qid)

  def assertEdgeCollection[A](actual: Iterator[A], expected: Seq[A])(implicit pos: Position): Assertion =
    actual.toSeq should contain theSameElementsAs expected
}

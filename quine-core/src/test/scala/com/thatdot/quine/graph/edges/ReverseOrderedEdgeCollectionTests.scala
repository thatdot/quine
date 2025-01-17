package com.thatdot.quine.graph.edges

import org.scalactic.source.Position
import org.scalatest.Assertion

import com.thatdot.common.quineid.QuineId

class ReverseOrderedEdgeCollectionTests extends SyncEdgeCollectionTests {

  def newEdgeCollection(qid: QuineId): SyncEdgeCollection = new ReverseOrderedEdgeCollection(qid)

  def assertEdgeCollection[A](actual: Iterator[A], expected: Seq[A])(implicit pos: Position): Assertion =
    actual.toSeq should contain theSameElementsInOrderAs expected.reverse
}

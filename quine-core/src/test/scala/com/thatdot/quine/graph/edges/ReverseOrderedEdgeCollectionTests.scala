package com.thatdot.quine.graph.edges

import org.scalatest.Assertion

import com.thatdot.quine.model.QuineId

class ReverseOrderedEdgeCollectionTests extends EdgeCollectionTests {

  def newEdgeCollection(qid: QuineId): SyncEdgeCollection = new ReverseOrderedEdgeCollection(qid)

  def assertEdgeCollection[A](actual: Seq[A], expected: Seq[A]): Assertion =
    actual should contain theSameElementsInOrderAs expected.reverse
}

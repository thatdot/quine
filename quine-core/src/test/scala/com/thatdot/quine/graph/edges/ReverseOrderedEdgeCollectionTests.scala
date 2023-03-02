package com.thatdot.quine.graph.edges

import org.scalatest.Assertion

class ReverseOrderedEdgeCollectionTests extends EdgeCollectionTests {

  def newEdgeCollection: EdgeCollection = new ReverseOrderedEdgeCollection

  def assertEdgeCollection[A](actual: Seq[A], expected: Seq[A]): Assertion =
    actual should contain theSameElementsInOrderAs expected.reverse
}

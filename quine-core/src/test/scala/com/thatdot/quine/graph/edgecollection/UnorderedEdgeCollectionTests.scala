package com.thatdot.quine.graph.edgecollection

import org.scalatest.Assertion

class UnorderedEdgeCollectionTests extends EdgeCollectionTests {

  def newEdgeCollection: EdgeCollection = new UnorderedEdgeCollection

  def assertEdgeCollection[A](actual: Seq[A], expected: Seq[A]): Assertion =
    actual should contain theSameElementsAs expected
}

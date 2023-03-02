package com.thatdot.quine.graph.edges

import com.thatdot.quine.model.HalfEdge

abstract class EdgeCollection extends EdgeCollectionView {
  def addEdgeSync(edge: HalfEdge): Unit

  def removeEdgeSync(edge: HalfEdge): Unit

}

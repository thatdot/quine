package com.thatdot.quine.graph.edgecollection

import com.thatdot.quine.model.HalfEdge

abstract class EdgeCollection extends EdgeCollectionView {

  def +=(edge: HalfEdge): EdgeCollection

  def -=(edge: HalfEdge): EdgeCollection

  def clear(): Unit
}

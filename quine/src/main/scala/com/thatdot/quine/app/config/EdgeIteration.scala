package com.thatdot.quine.app.config

import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveEnumerationConvert

import com.thatdot.quine.graph.edgecollection.{EdgeCollection, ReverseOrderedEdgeCollection, UnorderedEdgeCollection}

/** Options for edge collection iteration */
sealed abstract class EdgeIteration {

  /** Create a supplier of edge collections */
  def edgeCollectionFactory: () => EdgeCollection
}
object EdgeIteration {
  case object Unordered extends EdgeIteration {
    def edgeCollectionFactory: () => EdgeCollection = () => new UnorderedEdgeCollection
  }

  case object ReverseInsertion extends EdgeIteration {
    def edgeCollectionFactory: () => EdgeCollection = () => new ReverseOrderedEdgeCollection
  }

  implicit val edgeIterationConfigConvert: ConfigConvert[EdgeIteration] =
    deriveEnumerationConvert[EdgeIteration]
}

package com.thatdot.quine.app.config

import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveEnumerationConvert

import com.thatdot.quine.graph.edges.{ReverseOrderedEdgeCollection, SyncEdgeCollection, UnorderedEdgeCollection}
import com.thatdot.quine.model.QuineId

/** Options for edge collection iteration */
sealed abstract class EdgeIteration {

  /** Create a supplier of edge collections */
  def edgeCollectionFactory: QuineId => SyncEdgeCollection
}
object EdgeIteration {
  case object Unordered extends EdgeIteration {
    def edgeCollectionFactory: QuineId => SyncEdgeCollection = new UnorderedEdgeCollection(_)
  }

  case object ReverseInsertion extends EdgeIteration {
    def edgeCollectionFactory: QuineId => SyncEdgeCollection = new ReverseOrderedEdgeCollection(_)
  }

  implicit val edgeIterationConfigConvert: ConfigConvert[EdgeIteration] =
    deriveEnumerationConvert[EdgeIteration]
}

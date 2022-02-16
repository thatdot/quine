package com.thatdot.quine

package object model {

  /** Multiple [[Property]] with distinct keys enforced by the map */
  type Properties = Map[Symbol, PropertyValue]

  type QueryNode = DomainNodeEquiv
  type FoundNode = DomainNodeEquiv

  type CircularEdge = (Symbol, IsDirected)
  type IsDirected = Boolean
}

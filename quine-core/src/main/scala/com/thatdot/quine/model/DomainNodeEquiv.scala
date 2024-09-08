package com.thatdot.quine.model

import DGBOps._

/** A domain/user's view of the local features which define the equivalent of a particular node
  *
  * @param className     Optional name for the type (class) which created this instance
  *                      (e.g. if derived from a Scala class)
  * @param localProps    Predicates properties on this node must match, keyed by property name
  * @param circularEdges Edges that point back to this node. Even though these are edges, they are local features to
  *                      this node and do not require visiting another node to test.
  */
final case class DomainNodeEquiv(
  className: Option[String],
  localProps: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])],
  circularEdges: Set[CircularEdge],
) {
  def containsEquiv(other: DomainNodeEquiv): Boolean =
    (this.className.isEmpty || this.className == other.className) &&
    (this.localProps == other.localProps || this.localProps
      .containsByLeftConditions(other.localProps)) &&
    (this.circularEdges.isEmpty || other.circularEdges.forall(ce => this.circularEdges.contains(ce)))

  def ++(other: DomainNodeEquiv): DomainNodeEquiv =
    if (other == DomainNodeEquiv.empty) this
    else
      DomainNodeEquiv(
        className orElse other.className,
        localProps ++ other.localProps,
        circularEdges ++ other.circularEdges,
      )

  def +(newProp: (Symbol, (PropertyComparisonFunc, Option[PropertyValue]))): DomainNodeEquiv =
    this.copy(localProps = localProps + newProp)
}

case object DomainNodeEquiv {
  // A DomainNodeEquiv with no constraints. When used in a SQ, this will match any node properties/edges.
  val empty: DomainNodeEquiv = DomainNodeEquiv(None, Map.empty, Set.empty)
}

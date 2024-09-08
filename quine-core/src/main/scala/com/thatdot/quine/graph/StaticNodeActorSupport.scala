package com.thatdot.quine.graph

import scala.collection.mutable

import com.thatdot.quine.graph.NodeActor.{Journal, MultipleValuesStandingQueries}
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior

object StaticNodeActorSupport extends StaticNodeSupport[NodeActor, NodeSnapshot, NodeConstructorArgs] {
  def createNodeArgs(
    snapshot: Option[NodeSnapshot],
    initialJournal: Journal,
    multipleValuesStandingQueryStates: MultipleValuesStandingQueries,
  ): NodeConstructorArgs =
    // Using .map.getOrElse instead of fold to avoid needing a lot of type hints
    NodeConstructorArgs(
      properties = snapshot.map(_.properties).getOrElse(Map.empty),
      edges = snapshot.map(_.edges).getOrElse(Iterable.empty),
      distinctIdSubscribers = snapshot.map(_.subscribersToThisNode).getOrElse(mutable.Map.empty),
      domainNodeIndex =
        new DomainNodeIndexBehavior.DomainNodeIndex(snapshot.map(_.domainNodeIndex).getOrElse(mutable.Map.empty)),
      multipleValuesStandingQueryStates = multipleValuesStandingQueryStates,
      initialJournal = initialJournal,
    )
}

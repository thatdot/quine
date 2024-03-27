package com.thatdot.quine.model

import scala.collection.mutable

/** NodeComponents is a kind of reciprocal structure to DomainGraphBranch. It is means to return results from the
  * runtime graph in a structure corresponding to the DomainGraphBranch used to query and traverse the graph.
  *
  * @param qid        The QuineId of the runtime graph node found at this point in the traversal which help the values
  *                   returned in the `components` field.
  * @param components A collection of values found at the runtime graph node with the corresponding `qid`. This field
  *                   is a map of either properties found locally on the node, or else nested NodeComponents
  *                   corresponding to edges found on the node.
  */
final case class NodeComponents(
  qid: QuineId,
  components: Map[Symbol, Either[PropertyValue, (EdgeDirection, Set[NodeComponents])]]
) {
  def get(key: Symbol): Option[Either[PropertyValue, (EdgeDirection, Set[NodeComponents])]] =
    components.get(key)

  def ++(
    otherComponents: IterableOnce[
      (Symbol, Either[PropertyValue, (EdgeDirection, Set[NodeComponents])])
    ]
  ): NodeComponents =
    NodeComponents(qid, components ++ otherComponents)

  def ++:(
    otherComponents: IterableOnce[
      (Symbol, Either[PropertyValue, (EdgeDirection, Set[NodeComponents])])
    ]
  ): NodeComponents =
    NodeComponents(qid, components ++ otherComponents)

  def addProp(key: Symbol, value: PropertyValue): NodeComponents =
    NodeComponents(qid, components.+(key -> Left(value)))

  def addEdge(edgeType: Symbol, direction: EdgeDirection, edgeComponents: Set[NodeComponents]): NodeComponents =
    NodeComponents(qid, components.+(edgeType -> Right(direction -> edgeComponents)))

  // Not tail recursive!
  def allIds: Set[QuineId] = components.values
    .flatMap(
      _.map[Set[QuineId]](_._2.flatMap(_.allIds).toSet).getOrElse(Set.empty[QuineId])
    )
    .toSet[QuineId] + qid

  /** NodeComponents is a nested tree structure. flatValues returns “flatValues” from walking and
    * flattening the tree into a list of tuples. After flattening, the first tuple element is a
    * list corresponding to the path required to reach the associated value—which is the second
    * tuple element.
    */
  def flatValues(
    startingKeys: List[Symbol] = Nil
  ): List[(List[Symbol], Either[QuineId, PropertyValue])] =
    (startingKeys :+ Symbol("_qid_")) -> Left(qid) :: components.toList.flatMap { case (symbol, propOrEdge) =>
      propOrEdge match {
        case Left(prop) => List((startingKeys :+ symbol) -> Right(prop))
        case Right(edge) =>
          edge._2.flatMap(_.flatValues(startingKeys :+ Symbol(s"${edge._1}(${symbol.name})")))
      }
    }
}

object NodeComponents {

  /** Flatten a set of [[NodeComponents]] such that all node components sharing common [[QuineId]]s are merged, with
    * their edges being similarly recursively merged.
    *
    * @param ncs set of node components to merge
    * @return set of node components with distinct [[QuineId]]s
    */
  def flattenAndMerge(ncs: Set[NodeComponents]): Set[NodeComponents] = {
    val qidToNc: mutable.Map[QuineId, NodeComponents] = mutable.Map()

    for (nc <- ncs)
      qidToNc.get(nc.qid) match {
        case None => qidToNc.update(nc.qid, nc)
        case Some(ncExisting) =>
          val newComponents = mutable.Map(ncExisting.components.toSeq: _*)

          for ((k, v) <- nc.components.toSeq)
            (newComponents.get(k), v) match {
              case (None, _) => newComponents.update(k, v)
              case (Some(Left(p)), Right(e)) =>
                assert(false, s"Cannot merge property $p and edge $e both at key $k")
              case (Some(Right(e)), Left(p)) =>
                assert(false, s"Cannot merge edge $e and property $p both at key $k")
              case (Some(Left(p1)), Left(p2)) =>
                assert(
                  p1 == p2,
                  s"Cannot merge two different properties $p1 and $p2 both at key $k"
                )
              case (Some(Right((eDir1, eNc1))), Right((eDir2, eNc2))) =>
                assert(eDir1 == eDir2, "Cannot merge edges with different directions")
                newComponents.update(k, Right((eDir1, flattenAndMerge(eNc1 ++ eNc2))))
            }

          qidToNc.update(nc.qid, NodeComponents(nc.qid, newComponents.toMap))
      }

    qidToNc.values.toSet
  }
}

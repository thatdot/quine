package com.thatdot.quine.model

import scala.util.{Failure, Try}

object DGBOps {

  def linearComponentsInDependencyOrder(dgb: DomainGraphBranch[Create]): List[DomainGraphPath] =
    allLinearComponents(dgb).flatMap(_.inDependencyOrder)

  import DomainGraphBranch.asSingleBranch

  private def shapes(dgb: DomainGraphBranch[Create]): Set[DGBShape] =
    // Note: The `Final` shape is not produced here, because it is added in `def shape` after inspecting these results.
    (if (dgb.nextBranches.map(b => b.branch.domainNodeEquiv).contains(dgb.domainNodeEquiv))
       Set(ImmediatelyCircular)
     else Set.empty) ++
    (if (dgb.nextBranches.size > 1 && dgb.nextBranches.map(_.branch.domainNodeEquiv).size > 1)
       Set(Divergent)
     else Set.empty) ++
    (if (
       dgb.nextBranches.nonEmpty && dgb.nextBranches.map(_.branch.domainNodeEquiv).toSet != Set(
         dgb.domainNodeEquiv
       )
     ) Set(Linear)
     else Set.empty) ++
    //    (if (nextBranches.foldLeft(false)((a,b) => a || b._3.nextBranches.foldLeft(false)((c,d) => c || d._3.contains(domainNode))) ) Set(EventuallyCircularHere) else Set.empty) ++
    (if (containsCommonElement(dgb.nextBranches.map(e => trimmedNodeList(e.branch))))
       Set(Convergent)
     else Set.empty) ++
    dgb.nextBranches.map(_.branch).flatMap(shapes)

  private def containsCommonElement[T](lists: List[List[T]]): Boolean =
    lists.map(_.toSet.size).sum != lists.flatten.toSet.size

  private def shape(dgb: DomainGraphBranch[Create]): DGBShape = shapes(dgb) match {
    case x if x contains Convergent => Convergent
    case x if x contains Divergent => Divergent
    case x if x contains Linear => Linear
    case _ => Final
  }

  private def nextShape(dgb: DomainGraphBranch[Create]): DGBShape =
    if (dgb.nextBranches.isEmpty) Final
    else if (dgb.nextBranches.size == 1) Linear
    else
      shape(dgb)

  private def trimmedNodeList(dgb: DomainGraphBranch[Create]): List[DomainNodeEquiv] = nodeList(
    dgb
  ).reverse match {
    // Remove double listing at the end due to concluding circular edge. (so that shape doesn't always return 'Convergent' when it is 'ImmediatelyCircular'
    case a :: b :: xs if a == b => (b :: xs).reverse
    case x => x.reverse
  } // Double reversing is a hack! Find a better way.

  private def nodeList(dgb: DomainGraphBranch[Create]): List[DomainNodeEquiv] = dgb match {
    case SingleBranch(domainNodeEquiv, _, _, _) =>
      domainNodeEquiv :: dgb.nextBranches.flatMap(e => nodeList(e.branch))
    case _ => dgb.nextBranches.flatMap(e => nodeList(e.branch))
  }

  private def allLinearComponents(dgb: DomainGraphBranch[Create]): List[DomainGraphPath] = {
    val (path, branches) = extractComponent(dgb)
    path :: branches.flatMap(allLinearComponents)
  }

  // WARNING: Not tail recursive!
  private def extractComponent(
    branch: DomainGraphBranch[Create]
  ): (DomainGraphPath, List[DomainGraphBranch[Create]]) =
    nextShape(branch) match {
      case Final =>
        DomainGraphPath(
          Seq(DomainGraphHop(branch.domainNodeEquiv, None, branch.identification))
        ) -> List()
      case Divergent | ImmediatelyCircular =>
        val x = Seq(
          DomainGraphHop(
            branch.domainNodeEquiv,
            Some((branch.nextBranches.head.edge, branch.nextBranches.head.depDirection)),
            branch.identification
          )
        )
        val next = extractComponent(branch.nextBranches.head.branch)
        val hops = x ++ next._1.hops
        DomainGraphPath(hops) -> (SingleBranch(
          branch.domainNodeEquiv,
          branch.identification,
          branch.nextBranches.tail
        ) :: next._2)
      case Linear =>
        val x = Seq(
          DomainGraphHop(
            branch.domainNodeEquiv,
            Some((branch.nextBranches.head.edge, branch.nextBranches.head.depDirection)),
            branch.identification
          )
        )
        val next = extractComponent(branch.nextBranches.head.branch)
        val hops = x ++ next._1.hops
        DomainGraphPath(hops) -> next._2
      case _ => // Convergent
        val x = Seq(
          DomainGraphHop(
            branch.domainNodeEquiv,
            Some((branch.nextBranches.head.edge, branch.nextBranches.head.depDirection)),
            branch.identification
          )
        )
        val next = extractComponent(branch.nextBranches.head.branch)
        val hops = x ++ next._1.hops
        DomainGraphPath(hops) -> (SingleBranch(
          branch.domainNodeEquiv,
          None,
          branch.nextBranches.tail
        ) :: next._2)
    }

  implicit final class DNMapOps(
    private val map: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]
  ) extends AnyVal {
    def containsByLeftConditions(other: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]): Boolean =
      other.forall { case (key, (_, v1)) =>
        map.get(key).exists { case (compFunc, v2) => compFunc(v1, v2) }
      }

    def containsByRightConditions(other: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]): Boolean =
      other.forall { case (key, (compFunc, v1)) =>
        map.get(key).exists { case (_, v2) => compFunc(v1, v2) }
      }

    def containsByBothConditions(other: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]): Boolean =
      other.forall { case (key, (compFunc1, v1)) =>
        map.get(key).exists { case (compFunc2, v2) =>
          compFunc1(v1, v2) &&
            compFunc2(v1, v2)
        }
      }
  }
}

sealed abstract private class DGBShape
private case object Linear extends DGBShape
private case object Divergent extends DGBShape
private case object Convergent extends DGBShape
private case object ImmediatelyCircular extends DGBShape // Is this the same as Convergent without Divergent?
private case object Final extends DGBShape

final case class DomainGraphHop(
  node: DomainNodeEquiv,
  trailingEdgeOpt: Option[(GenericEdge, DependencyDirection)],
  foundDomainNode: Option[QuineId]
) {

  def identifyNode(idNode: DomainNodeEquiv, foundId: QuineId): DomainGraphHop =
    if (foundDomainNode.isEmpty && node.containsEquiv(idNode))
      this.copy(foundDomainNode = Some(foundId))
    else this

  override def toString: String = {
    val idString = if (foundDomainNode.isDefined) "\uD83C\uDD94" else ""
    node.toString +
    idString + trailingEdgeOpt
      .map(e =>
        (if (e._2 == DependsUpon) " =[]=>" else " <=[]=")
          .replace(
            "[]",
//          "")   // Either this one line, or the following:
            (if (e._1.direction == EdgeDirection.Outgoing) "(-[]->)"
             else if (e._1.direction == EdgeDirection.Incoming) "(<-[]-)"
             else "(-[]-)")
              .replace("[]", s"[${e._1.edgeType.name}]")
          )
      )
      .getOrElse("")
  }
}

final case class DomainGraphPath(hops: Seq[DomainGraphHop]) extends AnyVal {
  override def toString: String = hops.mkString("• ", " ", " •")

  // WARNING: Not tail recursive!
  def splitAtDepDirChange: Seq[DomainGraphPath] =
    if (hops.length < 3 || this.singleDependencyDirection) Seq(this)
    else {
      val testDir = hops.head.trailingEdgeOpt.get._2
      val reversalHop =
        hops.find(h => h.trailingEdgeOpt.isDefined && h.trailingEdgeOpt.get._2 == testDir.reverse)
      if (reversalHop.isEmpty) Seq(this)
      else {
        val parts = this.bisect(reversalHop.get.node).get
        Seq(parts._1) ++ parts._2.splitAtDepDirChange
      }
    }

  def inDependencyOrder: Seq[DomainGraphPath] =
    if (this.hops.length == 1) Seq(this)
    else
      this.splitAtDepDirChange
        .filter(c => c.hops.nonEmpty && c.hops.head.trailingEdgeOpt.isDefined)
        .map { c =>
          if (c.hops.head.trailingEdgeOpt.get._2 != IsDependedUpon) c.reverse
          else c
        }

  def singleDependencyDirection: Boolean =
    if (hops.length < 3)
      true // 3 because the second edge (= what is being tested) should always be followed by a third element in the sequence.
    else hops.forall(_.trailingEdgeOpt.forall(_._2 == hops.head.trailingEdgeOpt.get._2))

  // WARNING: Not tail recursive!
  def identifyNode(idNode: DomainNodeEquiv, foundId: QuineId): DomainGraphPath = this.copy(
    hops = hops.map(_.identifyNode(idNode, foundId))
  )

  def reverse: DomainGraphPath = {
    val reversed = hops.reverse.zipWithIndex
    val reassociated = reversed.map { case (hop, idx) =>
      val reversedEdgeOpt =
        if (idx < reversed.length - 1)
          reversed(idx + 1)._1.trailingEdgeOpt.map(x => (x._1.reverse, x._2.reverse))
        else None
      DomainGraphHop(hop.node, reversedEdgeOpt, hop.foundDomainNode)
    }
    DomainGraphPath(reassociated)
  }

  def bisect(domainNode: DomainNodeEquiv): Try[(DomainGraphPath, DomainGraphPath)] =
    Try {
      this.position(domainNode).get
    }.map { idx =>
      if (idx == 0) (DomainGraphPath(Seq()), this)
      else if (idx == this.hops.length - 1) (this.reverse, DomainGraphPath(Seq()))
      else {
        val first = hops.slice(0, idx + 1) // It's ok to slice past the end.
        val second = hops.slice(idx, hops.length)
        (
          DomainGraphPath(first).reverse,
          DomainGraphPath(second)
        )
      }
    }.recoverWith { case _ =>
      Failure(
        new NoSuchElementException(
          s"Could not find domainNode: $domainNode in sequence: $hops"
        )
      )
    }

  def position(domainNode: DomainNodeEquiv): Option[Int] =
    hops.indexWhere(h => domainNode.containsEquiv(h.node)) match {
      case -1 => None
      case i => Some(i)
    }

  def size: Int = hops.size
}

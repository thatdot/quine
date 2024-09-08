package com.thatdot.quine.graph.behavior

import com.thatdot.quine.graph.BaseNodeActorView
import com.thatdot.quine.model.EdgeDirection.{Incoming, Outgoing, Undirected}
import com.thatdot.quine.model.{DomainEdge, DomainNodeEquiv, HalfEdge, SingleBranch}

trait DomainNodeTests extends BaseNodeActorView {

  private[this] def localPropsMatch(testNodeEquiv: DomainNodeEquiv): Boolean =
    testNodeEquiv.localProps forall { case (s, (compFunc, testPropVal)) =>
      compFunc(testPropVal, properties.get(s))
    }

  private[this] def hasCircularEdges(testNodeEquiv: DomainNodeEquiv): Boolean =
    testNodeEquiv.circularEdges.forall(circTest =>
      if (circTest._2) { // isDirected:
        edges.contains(HalfEdge(circTest._1, Outgoing, qid)) &&
        edges.contains(HalfEdge(circTest._1, Incoming, qid))
      } else {
        edges.contains(HalfEdge(circTest._1, Undirected, qid))
      },
    )

  private[this] def hasGenericEdges(requiredEdges: Set[DomainEdge]): Boolean =
    edges.hasUniqueGenEdges(requiredEdges, qid)

  /** Tests the local parts of the provided DGB against this node. Returns true iff all parts of the branch that can be
    * tested match this Quine node.
    *
    * A match on a localTestBranch call is not sufficient to indicate this node is a valid root instance of the provided
    * testBranch. A localTestBranch call matching does mean that if all subtrees of the testBranch are also satisfied
    * (potentially by other nodes), THEN this node is a valid root instance of the provided testBranch.
    */
  protected[this] def localTestBranch(testBranch: SingleBranch): Boolean = {
    val idMatchesDgn = testBranch.identification.forall(_ == qid)
    lazy val propsMatchDgn = localPropsMatch(testBranch.domainNodeEquiv)
    lazy val circularEdgesMatchDgn = hasCircularEdges(
      testBranch.domainNodeEquiv,
    )
    lazy val nonCircularHalfEdgesMatchDgn = hasGenericEdges(testBranch.nextBranches.toSet[DomainEdge])
    idMatchesDgn && propsMatchDgn && circularEdgesMatchDgn && nonCircularHalfEdgesMatchDgn
  }
}

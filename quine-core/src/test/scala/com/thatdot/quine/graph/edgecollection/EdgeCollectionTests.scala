package com.thatdot.quine.graph.edgecollection

import scala.collection.compat._
import scala.collection.mutable.LinkedHashSet

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inspectors, LoneElement}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.HalfEdgeGen
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainGraphBranch,
  EdgeDirection,
  FetchConstraint,
  GenericEdge,
  HalfEdge,
  QuineId
}

abstract class EdgeCollectionTests
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with LoneElement {

  import HalfEdgeGen.{halfEdgeGen, intToQuineId}

  /** The EdgeCollection impl to use.
    * @return
    */
  def newEdgeCollection: EdgeCollection

  /** Describes the specific assertion to make compairing the actual and expected values.
    *
    * @param expected The expecte value to compare against
    * @return
    */
  def assertEdgeCollection[A](actual: Seq[A], expected: Seq[A]): Assertion

  "The EdgeCollection" should "return the appropriate edges when variously queried" in {
    forAll(Gen.listOfN(100, halfEdgeGen)) { edges =>
      // Given a bunch of edges coming in sequentially
      val edgeCollection = newEdgeCollection
      // When the edges are loaded into the EdgeCollection
      edges.foreach(edge => edgeCollection += edge)
      val byEdgeType = edges.groupBy(_.edgeType).map { case (k, v) => k -> v.to(LinkedHashSet) }
      val byDirection = edges.groupBy(_.direction).map { case (k, v) => k -> v.to(LinkedHashSet) }
      val byOther = edges.groupBy(_.other).map { case (k, v) => k -> v.to(LinkedHashSet) }

      // Then:

      // All of the edges should be in the EdgeCollection
      edgeCollection.toSet should contain allElementsOf edges

      // Querying the EdgeCollection by a given edgeType should return all edges of that type
      Inspectors.forAll(byEdgeType) { case (edgeType, typeSet) =>
        assertEdgeCollection(edgeCollection.matching(edgeType).toSeq, typeSet.toSeq)
      }
      // Querying the EdgeCollection by a given direction should return all edges of that direction
      Inspectors.forAll(byDirection) { case (direction, directionSet) =>
        assertEdgeCollection(edgeCollection.matching(direction).toSeq, directionSet.toSeq)
      }
      // Querying the EdgeCollection by a given node should return all edges linked to that node
      Inspectors.forAll(byOther) { case (other, otherSet) =>
        assertEdgeCollection(edgeCollection.matching(other).toSeq, otherSet.toSeq)
      }

      // Querying the EdgeCollection by edge type and direction should return all edges with both that type and direction
      Inspectors.forAll(byEdgeType) { case (edgeType, typeSet) =>
        Inspectors.forAll(byDirection) { case (direction, directionSet) =>
          assertEdgeCollection(
            edgeCollection
              .matching(edgeType, direction)
              .toSeq,
            (typeSet intersect directionSet).toSeq
          )
        }
      }

      // Querying the EdgeCollection by direction node should return all edges with both that direction and node
      Inspectors.forAll(byDirection) { case (direction, directionSet) =>
        Inspectors.forAll(byOther) { case (other, otherSet) =>
          assertEdgeCollection(
            edgeCollection
              .matching(direction, other)
              .toSeq,
            (directionSet intersect otherSet).toSeq
          )
        }
      }

      // Querying the EdgeCollection by edge type and node should return all edges with both that type and node
      Inspectors.forAll(byEdgeType) { case (edgeType, typeSet) =>
        Inspectors.forAll(byOther) { case (other, otherSet) =>
          assertEdgeCollection(
            edgeCollection
              .matching(edgeType, other)
              .toSeq,
            (typeSet intersect otherSet).toSeq
          )
        }
      }

      // Querying the EdgeCollection by type, direction, and node should return the edges with that type, direction, and node
      Inspectors.forAll(edges) { edge =>
        edgeCollection.matching(edge.edgeType, edge.direction, edge.other).toSeq.loneElement shouldBe edge
      }

      // Should not return results for queries involving a edge type and/or node it hasn't seen
      edgeCollection.matching(Symbol("someNewType")) shouldBe empty
      edgeCollection.matching(intToQuineId(-1)) shouldBe empty
      edgeCollection.matching(Symbol("someNewType"), intToQuineId(-1)) shouldBe empty
      edgeCollection.matching(Symbol("someNewType"), edges.head.direction, intToQuineId(-1)) shouldBe empty
      edgeCollection.matching(Symbol("someNewType"), edges.head.other) shouldBe empty
      edgeCollection.matching(Symbol("someNewType"), edges.head.direction) shouldBe empty
      edgeCollection.matching(Symbol("someNewType"), edges.head.direction, edges.head.other) shouldBe empty
      edgeCollection.matching(edges.head.edgeType, intToQuineId(-1)) shouldBe empty
      edgeCollection.matching(edges.head.edgeType, edges.head.direction, intToQuineId(-1)) shouldBe empty

    }

  }

  "hasUniqueGenEdges" should "be sufficient to match" in {
    def checkContains(localEdges: Seq[HalfEdge], domainEdges: Seq[DomainEdge], qid: QuineId): Boolean = {
      val ec = newEdgeCollection
      localEdges.foreach(edge => ec += edge)
      ec.hasUniqueGenEdges(domainEdges.toSet, qid)
    }

    val qid1 = QuineId.fromInternalString("01")
    val qid2 = QuineId.fromInternalString("02")
    val qid3 = QuineId.fromInternalString("03")
    val qid4 = QuineId.fromInternalString("04")

    def domainEdge(sym: Symbol, dir: EdgeDirection, circularMatchAllowed: Boolean, constraintMin: Int) =
      DomainEdge(
        GenericEdge(sym, dir),
        DependsUpon,
        DomainGraphBranch.empty,
        circularMatchAllowed,
        FetchConstraint(constraintMin, None)
      )

    assert(
      checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid2)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 1),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 2)
        ),
        qid3
      ),
      "Base case - matching edges, circularMatchAllowed = false"
    )

    //addition of 1 more input edge fails
    assert(
      !checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid2)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 1),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 2),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 3)
        ),
        qid3
      ),
      "domain edges > collection size"
    )

    //different direction is not matched
    assert(
      !checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid2)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Incoming, circularMatchAllowed = false, 1),
          domainEdge(Symbol("A"), EdgeDirection.Incoming, circularMatchAllowed = false, 2)
        ),
        qid3
      ),
      "Different direction is not matched"
    )

    assert(
      !checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid2)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 1),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 2)
        ),
        qid2
      ),
      "Qid match added totals"
    )

    //with matching circular edges
    assert(
      !checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid2),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid3)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 1),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 2),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = true, 3),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = true, 4)
        ),
        qid4
      ),
      "Matching circAllowed and non-circAllowed edges"
    )

    // with only circular edges as input always succeeds
    assert(
      checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid2),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid3)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = true, 1),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = true, 2),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = true, 3),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = true, 4)
        ),
        qid4
      ),
      "Only circAllowedEdges always succeeds"
    )
  }

}

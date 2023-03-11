package com.thatdot.quine.graph.edges

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

  import HalfEdgeGen.{halfEdgeGen, intToQuineId, quineIdGen}

  // Override this if tests need to be skipped
  def runnable: Boolean = true

  /** The EdgeCollection impl to use.
    * @return
    */
  def newEdgeCollection(qid: QuineId): SyncEdgeCollection

  /** Describes the specific assertion to make comparing the actual and expected values.
    *
    * @param expected The expected value to compare against
    * @return
    */
  def assertEdgeCollection[A](actual: Seq[A], expected: Seq[A]): Assertion

  "The EdgeCollection" should "return the appropriate edges when variously queried" in {
    assume(runnable)
    forAll(quineIdGen, Gen.listOfN(100, halfEdgeGen)) { (qid, edges) =>
      // Given a bunch of edges coming in sequentially
      val edgeCollection = newEdgeCollection(qid)
      // When the edges are loaded into the EdgeCollection
      edges.foreach(edge => edgeCollection.addEdge(edge))
      val byEdgeType = edges.groupBy(_.edgeType).map { case (k, v) => k -> v.to(LinkedHashSet) }
      val byDirection = edges.groupBy(_.direction).map { case (k, v) => k -> v.to(LinkedHashSet) }
      val byOther = edges.groupBy(_.other).map { case (k, v) => k -> v.to(LinkedHashSet) }

      // Then:

      // All of the edges should be in the EdgeCollection
      edgeCollection.all.toSet should contain allElementsOf edges

      // Querying the EdgeCollection by a given edgeType should return all edges of that type
      Inspectors.forAll(byEdgeType) { case (edgeType, typeSet) =>
        assertEdgeCollection(edgeCollection.edgesByType(edgeType).toSeq, typeSet.toSeq)
      }

      // Querying the EdgeCollection by a given direction should return all edges of that direction
      Inspectors.forAll(byDirection) { case (direction, directionSet) =>
        assertEdgeCollection(edgeCollection.edgesByDirection(direction).toSeq, directionSet.toSeq)
      }

      // Querying the EdgeCollection by a given node should return all edges linked to that node
      Inspectors.forAll(byOther) { case (other, otherSet) =>
        assertEdgeCollection(
          edgeCollection.edgesByQid(other).toSeq,
          otherSet.map(e => GenericEdge(e.edgeType, e.direction)) toSeq
        )
      }

      // Querying the EdgeCollection by edge type and direction should return all edges with both that type and direction
      Inspectors.forAll(byEdgeType) { case (edgeType, typeSet) =>
        Inspectors.forAll(byDirection) { case (direction, directionSet) =>
          assertEdgeCollection(
            edgeCollection
              .qidsByTypeAndDirection(edgeType, direction)
              .toSeq,
            (typeSet intersect directionSet).map(_.other).toSeq
          )
        }
      }

      // Querying the EdgeCollection by direction node should return all edges with both that direction and node
      Inspectors.forAll(byDirection) { case (direction, directionSet) =>
        Inspectors.forAll(byOther) { case (other, otherSet) =>
          assertEdgeCollection(
            edgeCollection
              .typesByDirectionAndQid(direction, other)
              .toSeq,
            (directionSet intersect otherSet).map(_.edgeType).toSeq
          )
        }
      }

      // Querying the EdgeCollection by edge type and node should return all edges with both that type and node
      Inspectors.forAll(byEdgeType) { case (edgeType, typeSet) =>
        Inspectors.forAll(byOther) { case (other, otherSet) =>
          assertEdgeCollection(
            edgeCollection
              .directionsByTypeAndQid(edgeType, other)
              .toSeq,
            (typeSet intersect otherSet).map(_.direction).toSeq
          )
        }
      }

      // Querying the EdgeCollection by type, direction, and node should return the edges with that type, direction, and node
      Inspectors.forAll(edges) { edge =>
        assert(edgeCollection.contains(edge))
      }

      // Should not return results for queries involving a edge type and/or node it hasn't seen
      edgeCollection.edgesByType(Symbol("someNewType")) shouldBe empty
      edgeCollection.edgesByQid(intToQuineId(-1)) shouldBe empty
      edgeCollection.directionsByTypeAndQid(Symbol("someNewType"), intToQuineId(-1)) shouldBe empty
      edgeCollection.contains(HalfEdge(Symbol("someNewType"), edges.head.direction, intToQuineId(-1))) shouldBe false
      edgeCollection.directionsByTypeAndQid(Symbol("someNewType"), edges.head.other) shouldBe empty
      edgeCollection.qidsByTypeAndDirection(Symbol("someNewType"), edges.head.direction) shouldBe empty
      edgeCollection.contains(HalfEdge(Symbol("someNewType"), edges.head.direction, edges.head.other)) shouldBe false
      edgeCollection.directionsByTypeAndQid(edges.head.edgeType, intToQuineId(-1)) shouldBe empty
      edgeCollection.contains(HalfEdge(edges.head.edgeType, edges.head.direction, intToQuineId(-1))) shouldBe false

    }

  }

  "hasUniqueGenEdges" should "be sufficient to match" in {
    assume(runnable)
    val thisQid = QuineId.fromInternalString("00")
    def checkContains(localEdges: Seq[HalfEdge], domainEdges: Seq[DomainEdge], qid: QuineId): Boolean = {
      val ec = newEdgeCollection(thisQid)
      localEdges.foreach(edge => ec.addEdge(edge))
      ec.hasUniqueGenEdges(domainEdges.toSet)
    }

    val qid1 = QuineId.fromInternalString("01")
    val qid2 = QuineId.fromInternalString("02")
    val qid3 = QuineId.fromInternalString("03")

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
        thisQid
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
        thisQid
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
        thisQid
      ),
      "Different direction is not matched"
    )

    assert(
      !checkContains(
        Seq(
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, qid1),
          HalfEdge(Symbol("A"), EdgeDirection.Outgoing, thisQid)
        ),
        Seq(
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 1),
          domainEdge(Symbol("A"), EdgeDirection.Outgoing, circularMatchAllowed = false, 2)
        ),
        thisQid
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
        thisQid
      ),
      "Matching circAllowed and non-circAllowed edges"
    )

    /* With only circular edge requirements as input, the behavior is undefined:
       hasUniqueGenEdges may or may not succeed; it doesn't matter as hasUniqueGenEdges is always called _after_
       circular edges have been checked.

       If we wanted to assert that circular edges are entirely unchecked, we could do so with:

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
     */
  }

}

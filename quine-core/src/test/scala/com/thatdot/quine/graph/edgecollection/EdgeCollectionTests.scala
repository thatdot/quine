package com.thatdot.quine.graph.edgecollection

import scala.collection.compat._
import scala.collection.mutable.LinkedHashSet

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inspectors, LoneElement}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.HalfEdgeGen

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

}

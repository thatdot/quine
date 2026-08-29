package com.thatdot.quine.graph.standing

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.{Expr, HeapEdgeContributionStore, QueryContext}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

/** The properties that let an edge's contribution be written down one edge at a time, in any order, and applied more
  * than once, which is what a node whose edges do not fit in memory needs of them.
  */
class EdgeContributionStoreTests extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private val column = Symbol("value")

  private def row(value: Long): QueryContext = QueryContext(Map(column -> Expr.Integer(value)))

  private def edge(id: Byte): HalfEdge = HalfEdge(Symbol("edge"), EdgeDirection.Outgoing, QuineId(Array(id)))

  /** A contribution: which edge answered, and the rows it answered with (repeats included, since a group may hold
    * the same row more than once).
    */
  private val contributions: Gen[List[(HalfEdge, Seq[QueryContext])]] = Gen.listOf(
    for {
      edgeId <- Gen.choose[Byte](1, 5)
      rows <- Gen.listOf(Gen.choose(1L, 4L).map(row))
    } yield edge(edgeId) -> (rows: Seq[QueryContext]),
  )

  private def totalOf(applied: Seq[(HalfEdge, Seq[QueryContext])]): Map[QueryContext, Int] = {
    val store = new HeapEdgeContributionStore
    applied.foreach { case (halfEdge, rows) => store.contribute(halfEdge, rows)(_ => ()) }
    store.total.toMap
  }

  private def countsOf(rows: Seq[QueryContext]): Map[QueryContext, Int] =
    rows.groupBy(identity).view.mapValues(_.size).toMap

  test("applying an edge's current contribution again changes nothing") {
    forAll(contributions) { applied =>
      val store = new HeapEdgeContributionStore
      applied.foreach { case (halfEdge, rows) => store.contribute(halfEdge, rows)(_ => ()) }
      val settled = store.total.toMap

      // An edge's contribution is the last level it sent, so that is the one whose redelivery must be a no-op
      applied.groupBy(_._1).view.mapValues(_.last._2).foreach { case (halfEdge, rows) =>
        store.contribute(halfEdge, rows) { outcome =>
          assert(!outcome.totalChanged, "reapplying a contribution reported a change")
          assert(!outcome.edgeChanged, "reapplying a contribution reported the edge changing")
          ()
        }
      }
      assert(store.total.toMap == settled)
    }
  }

  test("contributions from different edges commute") {
    forAll(contributions, Gen.long) { (applied, seed) =>
      // Only the last contribution per edge is the edge's level, so compare orderings of distinct edges
      val perEdge = applied.groupBy(_._1).view.mapValues(_.last._2).toList
      val shuffled = new scala.util.Random(seed).shuffle(perEdge)
      assert(totalOf(perEdge) == totalOf(shuffled))
    }
  }

  test("retracting an edge takes back exactly what it contributed") {
    forAll(contributions) { applied =>
      val perEdge = applied.groupBy(_._1).view.mapValues(_.last._2).toList
      perEdge.foreach { case (retracted, _) =>
        val remaining = perEdge.filterNot(_._1 == retracted)
        val store = new HeapEdgeContributionStore
        perEdge.foreach { case (halfEdge, rows) => store.contribute(halfEdge, rows)(_ => ()) }
        store.retract(retracted)(_ => ())
        assert(store.total.toMap == totalOf(remaining))
        assert(store.answeredEdges == remaining.size)
      }
    }
  }

  test("the total stands for the rows of every edge, with repeats") {
    forAll(contributions) { applied =>
      val perEdge = applied.groupBy(_._1).view.mapValues(_.last._2).toList
      val store = new HeapEdgeContributionStore
      perEdge.foreach { case (halfEdge, rows) => store.contribute(halfEdge, rows)(_ => ()) }
      assert(store.total.toMap == countsOf(perEdge.flatMap(_._2)))
    }
  }

  test("tracking an edge again never discards what it contributed") {
    val store = new HeapEdgeContributionStore
    store.contribute(edge(1), Seq(row(1L)))(_ => ())
    store.track(edge(1))
    assert(store.answeredEdges == 1)
    assert(store.total.toMap == countsOf(Seq(row(1L))))
  }

  test("an edge that has not answered is tracked but contributes nothing") {
    val store = new HeapEdgeContributionStore
    store.track(edge(1))
    assert(store.hasTrackedEdges)
    assert(store.answeredEdges == 0)
    assert(store.total.isEmpty)

    store.contribute(edge(1), Seq(row(1L))) { outcome => assert(outcome.totalChanged); () }
    assert(store.hasTrackedEdges)
    assert(store.answeredEdges == 1)

    store.retract(edge(1)) { outcome => assert(outcome.totalChanged); () }
    assert(!store.hasTrackedEdges)
    assert(store.total.isEmpty)
  }
}

package com.thatdot.quine

import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.compiler.cypher.CypherHarness
import com.thatdot.quine.graph.cypher.{Columns, CompiledQuery, Expr, Location, Parameters, Query}
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

/** Semantics of [[Query.SetHalfEdge]]: the building block for scattered
  * ingest execution. A `CREATE (a)-[:e]->(b)` between two id-known nodes
  * decomposes into two independent fragments, each anchored at one endpoint
  * and writing only its own half-edge, with no fragment ever messaging the
  * other node. These tests pin the two properties that make the
  * decomposition sound:
  *
  *  1. `SetHalfEdge` writes ONLY the local half (unlike [[Query.SetEdge]],
  *     which ships the reciprocal to the other endpoint), and
  *  2. two half-edge fragments compose to exactly the edge `CREATE` would
  *     have produced, because a Quine edge has never been anything but two
  *     half-edges written by separate events.
  */
class SetHalfEdgeTest extends CypherHarness("set-half-edge-tests") {

  private val label = Symbol("knows")

  private def drain(anchor: QuineId, direction: EdgeDirection, other: QuineId): Future[Unit] = {
    val fragment: Query[Location.Anywhere] = Query.ArgumentEntry(
      node = Expr.Bytes(anchor),
      andThen = Query.SetHalfEdge(
        label = label,
        direction = direction,
        target = Expr.Bytes(other),
        add = true,
        columns = Columns.Specified(Vector.empty),
      ),
      columns = Columns.Specified(Vector.empty),
    )
    val compiled = CompiledQuery[Location.External](
      queryText = None,
      query = fragment,
      unfixedParameters = Seq.empty,
      fixedParameters = Parameters.empty,
      initialColumns = Seq.empty,
    )
    graph.cypherOps
      .query(compiled, defaultNamespaceId, atTime = None, parameters = Map.empty)
      .results
      .runWith(Sink.ignore)(graph.materializer)
      .map(_ => ())(graph.system.dispatcher)
  }

  private def halfEdgesOf(qid: QuineId): Future[Set[HalfEdge]] =
    graph.literalOps(defaultNamespaceId).getHalfEdges(qid).map(_.toSet)(graph.system.dispatcher)

  describe("SetHalfEdge") {
    it("writes only the local half: no reciprocal, no hop") {
      val a = graph.idProvider.newQid()
      val b = graph.idProvider.newQid()
      for {
        _ <- drain(a, EdgeDirection.Outgoing, b)
        aEdges <- halfEdgesOf(a)
        bEdges <- halfEdgesOf(b)
      } yield {
        assert(aEdges == Set(HalfEdge(label, EdgeDirection.Outgoing, b)))
        assert(bEdges.isEmpty, "the other endpoint must be untouched: its half is another fragment's job")
      }
    }

    it("two independent fragments materialize the same edge CREATE would") {
      val a = graph.idProvider.newQid()
      val b = graph.idProvider.newQid()
      for {
        _ <- drain(a, EdgeDirection.Outgoing, b) // fragment anchored at a
        _ <- drain(b, EdgeDirection.Incoming, a) // fragment anchored at b
        aEdges <- halfEdgesOf(a)
        bEdges <- halfEdgesOf(b)
      } yield {
        assert(aEdges == Set(HalfEdge(label, EdgeDirection.Outgoing, b)))
        assert(bEdges == Set(HalfEdge(label, EdgeDirection.Incoming, a)))
      }
    }

    it("re-adding the same half is a no-op (idempotent under at-least-once retry)") {
      val a = graph.idProvider.newQid()
      val b = graph.idProvider.newQid()
      for {
        _ <- drain(a, EdgeDirection.Outgoing, b)
        _ <- drain(a, EdgeDirection.Outgoing, b) // the retry
        aEdges <- halfEdgesOf(a)
      } yield assert(aEdges == Set(HalfEdge(label, EdgeDirection.Outgoing, b)))
    }
  }
}

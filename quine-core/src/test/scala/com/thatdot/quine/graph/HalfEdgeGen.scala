package com.thatdot.quine.graph

import java.nio.ByteBuffer

import org.scalacheck.Gen

import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

trait HalfEdgeGen {
  val edgeTypes: List[Symbol] =
    List("knows", "likes", "dislikes", "fears", "eats", "foo", "bar", "baz", "other", "misc", "etc").map(Symbol(_))

  def intToQuineId(i: Int): QuineId = {
    val bb = ByteBuffer.allocate(4)
    QuineId(bb.putInt(i).array)
  }
  val quineIdGen: Gen[QuineId] = Gen.posNum[Int] map intToQuineId

  val halfEdgeGen: Gen[HalfEdge] = for {
    edgeType <- Gen.oneOf(edgeTypes)
    direction <- Gen.oneOf(EdgeDirection.values)
    other <- quineIdGen
  } yield HalfEdge(edgeType, direction, other)
}

object HalfEdgeGen extends HalfEdgeGen

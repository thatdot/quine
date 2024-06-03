package com.thatdot.quine.graph

import com.google.common.hash.Hashing.murmur3_128
import org.scalacheck.rng.Seed
import org.scalatest.flatspec.AnyFlatSpec

class StandingQueryResultTest extends AnyFlatSpec with HalfEdgeGen with ArbitraryInstances {
  it must "generate stable identifiers for arbitrary values" in {
    val hasher = murmur3_128.newHasher
    val standingQueries = Generators.generateN[StandingQueryResult](n = 1000, size = 100, Seed(0L))
    standingQueries.map(_.dataHashCode).foreach(hasher.putLong)
    assert(hasher.hash().asLong() === 6405060061703069172L)
  }
}

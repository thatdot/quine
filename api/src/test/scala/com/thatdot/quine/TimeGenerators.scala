package com.thatdot.quine

import java.time.Instant
import java.time.temporal.ChronoUnit.YEARS

import org.scalacheck.{Arbitrary, Gen}

object TimeGenerators {
  object Gens {

    /** Generates timestamps from the full possible range. */
    val instant: Gen[Instant] = Arbitrary.arbLong.arbitrary.map(Instant.ofEpochMilli)

    /** Generates timestamps from epoch to now. */
    val instantPositive: Gen[Instant] = Gen.chooseNum(0L, Instant.now().toEpochMilli).map(Instant.ofEpochMilli)

    /** Generates timestamps between two years before a reference point and itself (default `now`). */
    def instantRecentBefore(reference: Instant = Instant.now()): Gen[Instant] =
      Gen.chooseNum(reference.minus(2, YEARS).toEpochMilli, reference.toEpochMilli).map(Instant.ofEpochMilli)

    /** Generates timestamps between a reference point (default `now`) and two years after it. */
    def instantSoonAfter(reference: Instant = Instant.now()): Gen[Instant] =
      Gen.chooseNum(reference.toEpochMilli, reference.plus(2, YEARS).toEpochMilli).map(Instant.ofEpochMilli)

    /** Generates timestamps surrounding a reference point by up to two years on either side. */
    def instantNearTo(reference: Instant = Instant.now()): Gen[Instant] =
      Gen
        .chooseNum(reference.minus(2, YEARS).toEpochMilli, reference.plus(2, YEARS).toEpochMilli)
        .map(Instant.ofEpochMilli)
  }

}

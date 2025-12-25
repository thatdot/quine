package com.thatdot.quine

import java.time.Instant

import org.scalacheck.{Arbitrary, Gen}

object TimeGenerators {
  object Gens {

    /** Generates timestamps from the full possible range. */
    val instant: Gen[Instant] = Arbitrary.arbLong.arbitrary.map(Instant.ofEpochMilli)

    /** Generates timestamps within a specified range.
      *
      * @param from
      *   Optional start of range. Uses `Instant.now()` if not provided and `to` is provided.
      * @param to
      *   Optional end of range. Uses `Instant.now()` if not provided and `from` is provided.
      * @return
      *   A generator for Instants within the range. If neither bound is provided, returns the full-range [[instant]]
      *   generator.
      */
    def instantWithinRange(from: Option[Instant] = None, to: Option[Instant] = None): Gen[Instant] =
      (from, to) match {
        case (Some(f), Some(t)) => Gen.chooseNum(f.toEpochMilli, t.toEpochMilli).map(Instant.ofEpochMilli)
        case (Some(f), None) => Gen.chooseNum(f.toEpochMilli, Instant.now().toEpochMilli).map(Instant.ofEpochMilli)
        case (None, Some(t)) => Gen.chooseNum(Instant.now().toEpochMilli, t.toEpochMilli).map(Instant.ofEpochMilli)
        case (None, None) => instant
      }
  }

}

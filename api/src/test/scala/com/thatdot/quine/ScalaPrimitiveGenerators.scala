package com.thatdot.quine

import org.scalacheck.{Arbitrary, Gen}

/** Popular primitive-based generators (no `Arbs`; would conflict with ScalaCheck's). */
object ScalaPrimitiveGenerators {
  object Gens {
    val bool: Gen[Boolean] = Arbitrary.arbitrary[Boolean]
    val smallNonNegNum: Gen[Int] = Gen.chooseNum(0, 10)
    val smallPosNum: Gen[Int] = Gen.chooseNum(1, 10)
    val mediumNonNegNum: Gen[Int] = Gen.chooseNum(0, 1000)
    val mediumPosNum: Gen[Int] = Gen.chooseNum(1, 1000)
    val largePosNum: Gen[Int] = Gen.chooseNum(1, 1000000)
    val port: Gen[Int] = Gen.choose(1, 65535)
    val mediumPosLong: Gen[Long] = Gen.chooseNum(1L, 10000L)
    val largeNonNegLong: Gen[Long] = Gen.chooseNum(0L, 1000000L)
    val largePosLong: Gen[Long] = Gen.chooseNum(1L, 1000000L)
    val unitInterval: Gen[Double] = Gen.chooseNum(0.0, 1.0)
    val percentage: Gen[Double] = Gen.choose(0.0, 100.0)
    val mediumNonNegDouble: Gen[Double] = Gen.chooseNum(0.0, 1000.0)

    /** Generates positive integers within the range representable by `2^pow` bits (`1` to `2^pow - 1`).
      *
      * @param pow the "power" (exponent) of base-2 from which a bit range may be derived (e.g. `7` yields `2^7` or `128` bits)
      * @return an integer between `1` and `2^pow - 1`
      */
    def numWithinBits(pow: Int): Gen[Int] = Gen.chooseNum(1, (1 << pow) - 1)

    val nonEmptyAlphaStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
    val nonEmptyAlphaNumStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString)
    val optNonEmptyAlphaStr: Gen[Option[String]] = Gen.option(nonEmptyAlphaStr)
    val optNonEmptyAlphaNumStr: Gen[Option[String]] = Gen.option(nonEmptyAlphaNumStr)
  }
}

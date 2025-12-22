package com.thatdot.quine

import org.scalacheck.{Arbitrary, Gen}

/** Popular primitive-based generators (no Arbs; would conflict with ScalaCheck's). */
object ScalaPrimitiveGenerators {
  object Gens {
    val bool: Gen[Boolean] = Arbitrary.arbitrary[Boolean]
    val smallNum: Gen[Int] = Gen.chooseNum(0, 10)
    val smallPosNum: Gen[Int] = Gen.chooseNum(1, 10)
    val nonEmptyAlphaStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
    val nonEmptyAlphaNumStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString)
    val optNonEmptyAlphaStr: Gen[Option[String]] = Gen.option(nonEmptyAlphaStr)
    val optNonEmptyAlphaNumStr: Gen[Option[String]] = Gen.option(nonEmptyAlphaNumStr)
  }
}

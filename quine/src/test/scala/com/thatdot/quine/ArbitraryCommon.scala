package com.thatdot.quine

import org.scalacheck.{Arbitrary, Gen}

/** Common generators and arbitraries, focused on Scala primitives. This should remain independent. */
trait ArbitraryCommon {
  val genBool: Gen[Boolean] = Arbitrary.arbitrary[Boolean]

  val genSmallNum: Gen[Int] = Gen.chooseNum(0, 5)
  val genSmallPosNum: Gen[Int] = Gen.chooseNum(1, 5)

  val genNonEmptyAlphaStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
  val arbNonEmptyAlphaStr: Arbitrary[String] = Arbitrary(genNonEmptyAlphaStr)

  val genNonEmptyAlphaNumStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString)
  val arbNonEmptyAlphaNumStr: Arbitrary[String] = Arbitrary(genNonEmptyAlphaNumStr)

  val genOptNonEmptyAlphaStr: Gen[Option[String]] = Gen.option(genNonEmptyAlphaStr)
  val genOptNonEmptyAlphaNumStr: Gen[Option[String]] = Gen.option(genNonEmptyAlphaNumStr)
}

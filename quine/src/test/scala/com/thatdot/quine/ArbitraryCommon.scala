package com.thatdot.quine

import org.scalacheck.{Arbitrary, Gen}

trait ArbitraryCommon {
  val genNonEmptyAlphaStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
  implicit val arbNonEmptyAlphaStr: Arbitrary[String] = Arbitrary(genNonEmptyAlphaStr)

  val genNonEmptyAlphaNumStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString)
  implicit val arbNonEmptyAlphaNumStr: Arbitrary[String] = Arbitrary(genNonEmptyAlphaNumStr)
}

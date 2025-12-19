package com.thatdot.quine

import io.circe.Json
import org.scalacheck.{Arbitrary, Gen}

trait ArbitraryCommon {
  val genNonEmptyAlphaStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
  val arbNonEmptyAlphaStr: Arbitrary[String] = Arbitrary(genNonEmptyAlphaStr)

  val genNonEmptyAlphaNumStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString)
  val arbNonEmptyAlphaNumStr: Arbitrary[String] = Arbitrary(genNonEmptyAlphaNumStr)

  val genJsonPrimitive: Gen[Json] = Gen.oneOf(
    Gen.const(Json.Null),
    Arbitrary.arbBool.arbitrary.map(Json.fromBoolean),
    Arbitrary.arbLong.arbitrary.map(Json.fromLong),
    Arbitrary.arbDouble.arbitrary.map(Json.fromDoubleOrNull),
    Arbitrary.arbString.arbitrary.map(Json.fromString),
  )
}

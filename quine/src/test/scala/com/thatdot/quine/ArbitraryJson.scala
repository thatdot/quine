package com.thatdot.quine

import io.circe.Json
import org.scalacheck.{Arbitrary, Gen}

/** Generators and arbitraries for Circe JSON values. */
trait ArbitraryJson extends ArbitraryCommon {
  val genJsonPrimitive: Gen[Json] = Gen.oneOf(
    Gen.const(Json.Null),
    Arbitrary.arbBool.arbitrary.map(Json.fromBoolean),
    Arbitrary.arbLong.arbitrary.map(Json.fromLong),
    Arbitrary.arbDouble.arbitrary.map(Json.fromDoubleOrNull),
    Arbitrary.arbString.arbitrary.map(Json.fromString),
  )
  val arbJsonPrimitive: Arbitrary[Json] = Arbitrary(genJsonPrimitive)

  /** Generates a size-specified JSON Map[String, JSON primitive]. The result is a flat dictionary. */
  def genJsonDictionaryOfSize(size: Int): Gen[Map[String, Json]] =
    Gen.mapOfN(size, Gen.zip(genNonEmptyAlphaStr, genJsonPrimitive))

  /** Generates a small, flat dictionary, which may be empty. */
  val genJsonDictionary: Gen[Map[String, Json]] = genSmallNum.flatMap(genJsonDictionaryOfSize)

  /** Generates a small, non-empty, flat dictionary. */
  val genNonEmptyJsonDictionary: Gen[Map[String, Json]] = genSmallPosNum.flatMap(genJsonDictionaryOfSize)

  /** Generates an idiomatically sized flat dictionary (participates in ScalaCheck shrinking). */
  val genSizedJsonDictionary: Gen[Map[String, Json]] = Gen.sized(genJsonDictionaryOfSize)
}

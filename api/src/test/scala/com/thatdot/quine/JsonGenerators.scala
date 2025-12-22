package com.thatdot.quine

import io.circe.Json
import org.scalacheck.{Arbitrary, Gen}

object JsonGenerators {
  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaStr, smallNum, smallPosNum}

  object Gens {
    val primitive: Gen[Json] = Gen.oneOf(
      Gen.const(Json.Null),
      Arbitrary.arbBool.arbitrary.map(Json.fromBoolean),
      Arbitrary.arbLong.arbitrary.map(Json.fromLong),
      Arbitrary.arbDouble.arbitrary.map(Json.fromDoubleOrNull),
      Arbitrary.arbString.arbitrary.map(Json.fromString),
    )

    def dictionaryOfSize(size: Int): Gen[Map[String, Json]] =
      Gen.mapOfN(size, Gen.zip(nonEmptyAlphaStr, primitive))

    val dictionary: Gen[Map[String, Json]] = smallNum.flatMap(dictionaryOfSize)
    val nonEmptyDictionary: Gen[Map[String, Json]] = smallPosNum.flatMap(dictionaryOfSize)
    val sizedDictionary: Gen[Map[String, Json]] = Gen.sized(dictionaryOfSize)
  }

  object Arbs {
    implicit val primitive: Arbitrary[Json] = Arbitrary(Gens.primitive)
    implicit val dictionary: Arbitrary[Map[String, Json]] = Arbitrary(Gens.dictionary)
  }
}

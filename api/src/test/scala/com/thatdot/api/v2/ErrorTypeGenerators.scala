package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators

object ErrorTypeGenerators {
  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, optNonEmptyAlphaNumStr}

  object Gens {
    val apiError: Gen[ErrorType.ApiError] =
      nonEmptyAlphaNumStr.map(ErrorType.ApiError(_))

    val decodeError: Gen[ErrorType.DecodeError] = for {
      message <- nonEmptyAlphaNumStr
      help <- optNonEmptyAlphaNumStr
    } yield ErrorType.DecodeError(message, help)

    val cypherError: Gen[ErrorType.CypherError] =
      nonEmptyAlphaNumStr.map(ErrorType.CypherError(_))

    val errorType: Gen[ErrorType] =
      Gen.oneOf(apiError, decodeError, cypherError)
  }

  object Arbs {
    implicit val apiError: Arbitrary[ErrorType.ApiError] = Arbitrary(Gens.apiError)
    implicit val decodeError: Arbitrary[ErrorType.DecodeError] = Arbitrary(Gens.decodeError)
    implicit val cypherError: Arbitrary[ErrorType.CypherError] = Arbitrary(Gens.cypherError)
    implicit val errorType: Arbitrary[ErrorType] = Arbitrary(Gens.errorType)
  }
}

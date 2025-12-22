package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators

object ErrorResponseGenerators {
  import ScalaPrimitiveGenerators.Gens.smallPosNum
  import ErrorTypeGenerators.Gens.errorType

  object Gens {
    val errorList: Gen[List[ErrorType]] = smallPosNum.flatMap(Gen.listOfN(_, errorType))

    val serverError: Gen[ErrorResponse.ServerError] = errorList.map(ErrorResponse.ServerError(_))
    val badRequest: Gen[ErrorResponse.BadRequest] = errorList.map(ErrorResponse.BadRequest(_))
    val notFound: Gen[ErrorResponse.NotFound] = errorList.map(ErrorResponse.NotFound(_))
    val unauthorized: Gen[ErrorResponse.Unauthorized] = errorList.map(ErrorResponse.Unauthorized(_))
    val serviceUnavailable: Gen[ErrorResponse.ServiceUnavailable] = errorList.map(ErrorResponse.ServiceUnavailable(_))
  }

  object Arbs {
    implicit val serverError: Arbitrary[ErrorResponse.ServerError] = Arbitrary(Gens.serverError)
    implicit val badRequest: Arbitrary[ErrorResponse.BadRequest] = Arbitrary(Gens.badRequest)
    implicit val notFound: Arbitrary[ErrorResponse.NotFound] = Arbitrary(Gens.notFound)
    implicit val unauthorized: Arbitrary[ErrorResponse.Unauthorized] = Arbitrary(Gens.unauthorized)
    implicit val serviceUnavailable: Arbitrary[ErrorResponse.ServiceUnavailable] = Arbitrary(Gens.serviceUnavailable)
  }
}

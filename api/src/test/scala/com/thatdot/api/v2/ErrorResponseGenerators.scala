package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators

object ErrorResponseGenerators {
  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, smallPosNum}

  object Gens {
    val help: Gen[ErrorDetail.Help] = nonEmptyAlphaNumStr.map(ErrorDetail.Help(_))

    val requestInfo: Gen[ErrorDetail.RequestInfo] = nonEmptyAlphaNumStr.map(ErrorDetail.RequestInfo(_))

    val metadataMap: Gen[Map[String, String]] =
      Gen.choose(0, 3).flatMap { n =>
        Gen.mapOfN(n, Gen.zip(nonEmptyAlphaNumStr, nonEmptyAlphaNumStr))
      }

    val errorInfo: Gen[ErrorDetail.ErrorInfo] = for {
      reason <- nonEmptyAlphaNumStr
      domain <- nonEmptyAlphaNumStr
      metadata <- metadataMap
    } yield ErrorDetail.ErrorInfo(reason, domain, metadata)

    val errorDetail: Gen[ErrorDetail] = Gen.oneOf(help, requestInfo, errorInfo)

    val detailList: Gen[List[ErrorDetail]] =
      smallPosNum.flatMap(n => Gen.listOfN(n, errorDetail)).map(_.take(3))

    private def make[A](build: (String, List[ErrorDetail]) => A): Gen[A] = for {
      msg <- nonEmptyAlphaNumStr
      details <- detailList
    } yield build(msg, details)

    val serverError: Gen[ErrorResponse.ServerError] = make(ErrorResponse.ServerError(_, _))
    val badRequest: Gen[ErrorResponse.BadRequest] = make(ErrorResponse.BadRequest(_, _))
    val notFound: Gen[ErrorResponse.NotFound] = make(ErrorResponse.NotFound(_, _))
    val unauthorized: Gen[ErrorResponse.Unauthorized] = make(ErrorResponse.Unauthorized(_, _))
    val serviceUnavailable: Gen[ErrorResponse.ServiceUnavailable] = make(ErrorResponse.ServiceUnavailable(_, _))
  }

  object Arbs {
    implicit val errorDetail: Arbitrary[ErrorDetail] = Arbitrary(Gens.errorDetail)
    implicit val serverError: Arbitrary[ErrorResponse.ServerError] = Arbitrary(Gens.serverError)
    implicit val badRequest: Arbitrary[ErrorResponse.BadRequest] = Arbitrary(Gens.badRequest)
    implicit val notFound: Arbitrary[ErrorResponse.NotFound] = Arbitrary(Gens.notFound)
    implicit val unauthorized: Arbitrary[ErrorResponse.Unauthorized] = Arbitrary(Gens.unauthorized)
    implicit val serviceUnavailable: Arbitrary[ErrorResponse.ServiceUnavailable] = Arbitrary(Gens.serviceUnavailable)
  }
}

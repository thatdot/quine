package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators

object SuccessEnvelopeGenerators {
  import ScalaPrimitiveGenerators.Gens._

  object Gens {
    val warnings: Gen[List[String]] = smallNonNegNum.flatMap(Gen.listOfN(_, nonEmptyAlphaStr))

    def ok[A](contentGen: Gen[A]): Gen[SuccessEnvelope.Ok[A]] =
      for {
        content <- contentGen
        message <- optNonEmptyAlphaStr
        warns <- warnings
      } yield SuccessEnvelope.Ok(content, message, warns)

    def created[A](contentGen: Gen[A]): Gen[SuccessEnvelope.Created[A]] =
      for {
        content <- contentGen
        message <- optNonEmptyAlphaStr
        warns <- warnings
      } yield SuccessEnvelope.Created(content, message, warns)

    val accepted: Gen[SuccessEnvelope.Accepted] =
      for {
        message <- nonEmptyAlphaStr
        monitorUrl <- optNonEmptyAlphaStr
      } yield SuccessEnvelope.Accepted(message, monitorUrl)
  }

  object Arbs {
    implicit def okArb[A](implicit arbA: Arbitrary[A]): Arbitrary[SuccessEnvelope.Ok[A]] =
      Arbitrary(Gens.ok(arbA.arbitrary))

    implicit def createdArb[A](implicit arbA: Arbitrary[A]): Arbitrary[SuccessEnvelope.Created[A]] =
      Arbitrary(Gens.created(arbA.arbitrary))

    implicit val acceptedArb: Arbitrary[SuccessEnvelope.Accepted] = Arbitrary(Gens.accepted)
  }
}

package com.thatdot.quine

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

object AwsGenerators {

  import ScalaPrimitiveGenerators.Gens.nonEmptyAlphaNumStr

  object Gens {
    val awsCredentials: Gen[AwsCredentials] = for {
      accessKey <- nonEmptyAlphaNumStr
      secretKey <- nonEmptyAlphaNumStr
    } yield AwsCredentials(accessKey, secretKey)

    val optAwsCredentials: Gen[Option[AwsCredentials]] = Gen.option(awsCredentials)

    val awsRegion: Gen[AwsRegion] =
      Gen.oneOf("us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1").map(AwsRegion.apply)

    val optAwsRegion: Gen[Option[AwsRegion]] = Gen.option(awsRegion)
  }

  object Arbs {
    implicit val awsCredentials: Arbitrary[AwsCredentials] = Arbitrary(Gens.awsCredentials)
    implicit val optAwsCredentials: Arbitrary[Option[AwsCredentials]] = Arbitrary(Gens.optAwsCredentials)
    implicit val awsRegion: Arbitrary[AwsRegion] = Arbitrary(Gens.awsRegion)
    implicit val optAwsRegion: Arbitrary[Option[AwsRegion]] = Arbitrary(Gens.optAwsRegion)
  }
}

package com.thatdot.api.v2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.common.security.Secret
import com.thatdot.quine.ScalaPrimitiveGenerators

object AwsGenerators {

  import ScalaPrimitiveGenerators.Gens.nonEmptyAlphaNumStr

  object Gens {

    val awsCredentials: Gen[AwsCredentials] = for {
      accessKey <- nonEmptyAlphaNumStr
      secretKey <- nonEmptyAlphaNumStr
    } yield AwsCredentials(Secret(accessKey), Secret(secretKey))

    val optAwsCredentials: Gen[Option[AwsCredentials]] = Gen.option(awsCredentials)

    val awsRegion: Gen[AwsRegion] =
      Gen.oneOf("us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1").map(AwsRegion.apply)

    val optAwsRegion: Gen[Option[AwsRegion]] = Gen.option(awsRegion)
  }

  object Arbs {
    implicit val arbAwsCredentials: Arbitrary[AwsCredentials] = Arbitrary(Gens.awsCredentials)
    implicit val arbOptAwsCredentials: Arbitrary[Option[AwsCredentials]] = Arbitrary(Gens.optAwsCredentials)
    implicit val arbAwsRegion: Arbitrary[AwsRegion] = Arbitrary(Gens.awsRegion)
    implicit val arbOptAwsRegion: Arbitrary[Option[AwsRegion]] = Arbitrary(Gens.optAwsRegion)
  }
}

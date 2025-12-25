package com.thatdot.quine.routes

import org.scalacheck.{Arbitrary, Gen}

object AwsGenerators {

  object Gens {
    val nonEmptyAlphaNumStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString)

    val awsCredentials: Gen[AwsCredentials] = for {
      accessKeyId <- nonEmptyAlphaNumStr
      secretAccessKey <- nonEmptyAlphaNumStr
    } yield AwsCredentials(accessKeyId, secretAccessKey)

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

package com.thatdot.quine

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

trait ArbitraryAwsTypes {

  implicit val arbAwsCredentials: Arbitrary[Option[AwsCredentials]] = Arbitrary(
    Gen.option(
      for {
        accessKey <- Gen.alphaNumStr.suchThat(_.nonEmpty)
        secretKey <- Gen.alphaNumStr.suchThat(_.nonEmpty)
      } yield AwsCredentials(accessKey, secretKey),
    ),
  )

  implicit val arbAwsRegion: Arbitrary[Option[AwsRegion]] = Arbitrary(
    Gen.option(
      Gen.oneOf("us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1").map(AwsRegion.apply),
    ),
  )
}

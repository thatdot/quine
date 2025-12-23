package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.app.v2api.endpoints.V2AlgorithmEndpointEntities._

object V2AlgorithmEndpointGenerators {

  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, optNonEmptyAlphaNumStr}

  object Gens {
    val localFile: Gen[LocalFile] = optNonEmptyAlphaNumStr.map(LocalFile(_))

    val s3Bucket: Gen[S3Bucket] = for {
      bucketName <- nonEmptyAlphaNumStr
      key <- optNonEmptyAlphaNumStr
    } yield S3Bucket(bucketName, key)

    val tSaveLocation: Gen[TSaveLocation] = Gen.oneOf(localFile, s3Bucket)
  }

  object Arbs {
    implicit val localFile: Arbitrary[LocalFile] = Arbitrary(Gens.localFile)
    implicit val s3Bucket: Arbitrary[S3Bucket] = Arbitrary(Gens.s3Bucket)
    implicit val tSaveLocation: Arbitrary[TSaveLocation] = Arbitrary(Gens.tSaveLocation)
  }
}

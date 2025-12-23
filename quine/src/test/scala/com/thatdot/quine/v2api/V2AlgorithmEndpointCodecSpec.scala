package com.thatdot.quine.v2api

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2AlgorithmEndpointEntities._

class V2AlgorithmEndpointCodecSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import V2AlgorithmEndpointGenerators.Arbs._

  describe("LocalFile codec") {
    it("should roundtrip encode/decode") {
      forAll { (localFile: LocalFile) =>
        val json = localFile.asJson
        val decoded = json.as[LocalFile]
        decoded shouldBe Right(localFile)
      }
    }

    it("should encode with correct field name and value") {
      forAll { (localFile: LocalFile) =>
        val json = localFile.asJson
        json.hcursor.downField("fileName").as[Option[String]] shouldBe Right(localFile.fileName)
      }
    }
  }

  describe("S3Bucket codec") {
    it("should roundtrip encode/decode") {
      forAll { (s3Bucket: S3Bucket) =>
        val json = s3Bucket.asJson
        val decoded = json.as[S3Bucket]
        decoded shouldBe Right(s3Bucket)
      }
    }

    it("should encode with correct field names and values") {
      forAll { (s3Bucket: S3Bucket) =>
        val json = s3Bucket.asJson
        json.hcursor.downField("bucketName").as[String] shouldBe Right(s3Bucket.bucketName)
        json.hcursor.downField("key").as[Option[String]] shouldBe Right(s3Bucket.key)
      }
    }
  }

  describe("TSaveLocation codec") {
    it("should roundtrip encode/decode for all subtypes") {
      forAll { (location: TSaveLocation) =>
        val json = location.asJson
        val decoded = json.as[TSaveLocation]
        decoded shouldBe Right(location)
      }
    }

    it("should include type discriminator") {
      forAll { (location: TSaveLocation) =>
        val json = location.asJson
        json.hcursor.downField("type").as[String] shouldBe Right(location.getClass.getSimpleName)
      }
    }
  }
}

package com.thatdot.api.v2

import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class AwsRegionCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {
  import AwsGenerators.Arbs._

  test("AwsRegion encodes as plain string") {
    val region = AwsRegion("us-west-2")
    region.asJson shouldBe Json.fromString("us-west-2")
  }

  test("AwsRegion decodes from plain string") {
    val json = Json.fromString("us-west-2")
    json.as[AwsRegion] shouldBe Right(AwsRegion("us-west-2"))
  }

  test("AwsRegion roundtrips encode/decode") {
    forAll { (region: AwsRegion) =>
      region.asJson.as[AwsRegion] shouldBe Right(region)
    }
  }

  test("Option[AwsRegion] roundtrips encode/decode") {
    forAll { (region: Option[AwsRegion]) =>
      region.asJson.as[Option[AwsRegion]] shouldBe Right(region)
    }
  }
}

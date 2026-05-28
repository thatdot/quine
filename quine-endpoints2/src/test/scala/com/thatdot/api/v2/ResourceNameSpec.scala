package com.thatdot.api.v2

import io.circe.syntax._
import io.circe.{Decoder, Json}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import sttp.tapir.DecodeResult

class ResourceNameSpec extends AnyFunSpec with Matchers {

  describe("ResourceName.apply") {
    it("accepts AIP-122-style names") {
      val good = Seq("a", "A", "0", "_", "-", ".", "~", "my-stream", "ingest_42", "v.1", "Stream~hello", "x" * 128)
      good.foreach(s => ResourceName(s).map(_.value) shouldBe Right(s))
    }

    it("rejects the empty string") {
      ResourceName("") shouldBe a[Left[_, _]]
    }

    it("rejects names longer than 128 characters") {
      ResourceName("x" * 129) shouldBe a[Left[_, _]]
    }

    it("rejects names containing a colon (the AIP-136 reserved delimiter)") {
      ResourceName("foo:bar") shouldBe a[Left[_, _]]
      ResourceName("foo:bar:pause") shouldBe a[Left[_, _]]
      ResourceName(":pause") shouldBe a[Left[_, _]]
      ResourceName("pause:") shouldBe a[Left[_, _]]
    }

    it("rejects other URL-reserved characters") {
      Seq("a/b", "a?b", "a#b", "a%20b", "a b", "a&b", "a+b").foreach { s =>
        ResourceName(s) shouldBe a[Left[_, _]]
      }
    }

    it("rejects non-ASCII characters") {
      ResourceName("héllo") shouldBe a[Left[_, _]]
      ResourceName("ünicode") shouldBe a[Left[_, _]]
    }

    it("produces an error message naming the offending input") {
      ResourceName("bad:name") match {
        case Left(msg) =>
          msg should include("'bad:name'")
          msg should include("ASCII letters")
        case Right(rn) => fail(s"expected validation failure, got $rn")
      }
    }
  }

  describe("tapir codec") {
    it("decodes valid names to a ResourceName") {
      ResourceName.tapirCodec.decode("numbers") shouldBe a[DecodeResult.Value[_]]
    }

    it("rejects names containing a colon with a DecodeResult.Error") {
      ResourceName.tapirCodec.decode("foo:bar") shouldBe a[DecodeResult.Error]
    }

    it("round-trips through encode/decode") {
      val rn = ResourceName("my-stream").toOption.get
      val encoded = ResourceName.tapirCodec.encode(rn)
      ResourceName.tapirCodec.decode(encoded) shouldBe DecodeResult.Value(rn)
    }
  }

  describe("circe codec") {
    it("decodes a JSON string to a ResourceName") {
      Decoder[ResourceName].decodeJson(Json.fromString("numbers")).map(_.value) shouldBe Right("numbers")
    }

    it("rejects a JSON string containing a colon") {
      Decoder[ResourceName].decodeJson(Json.fromString("foo:bar")) shouldBe a[Left[_, _]]
    }

    it("rejects a non-string JSON value") {
      Decoder[ResourceName].decodeJson(Json.fromInt(7)) shouldBe a[Left[_, _]]
    }

    it("encodes a ResourceName as a plain JSON string") {
      ResourceName("numbers").toOption.get.asJson.noSpaces shouldBe "\"numbers\""
    }
  }

}

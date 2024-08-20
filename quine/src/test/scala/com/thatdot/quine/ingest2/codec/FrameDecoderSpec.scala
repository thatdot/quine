package com.thatdot.quine.ingest2.codec

import scala.util.Success

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest2.codec.StringDecoder
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString

class FrameDecoderSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  describe("String Decoder") {
    it("decodes String values") {
      val s = randomString()
      StringDecoder.decode(s.getBytes) shouldBe Success(s)
    }
  }

}

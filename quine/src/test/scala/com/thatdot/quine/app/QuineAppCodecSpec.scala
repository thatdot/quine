package com.thatdot.quine.app

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class QuineAppCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {

  import QuineAppGenerators.V2StandingQueryDataMap
  import QuineAppGenerators.Arbs.v2StandingQueryDataMap

  private val sqDataMapCodec = QuineApp.sqOutputs2Codec

  test("V2StandingQueryDataMap roundtrip: encode then decode equals identity") {
    forAll { (data: V2StandingQueryDataMap) =>
      val json = data.asJson(sqDataMapCodec.encoder)
      val decoded = json.as[V2StandingQueryDataMap](sqDataMapCodec.decoder)
      decoded shouldBe Right(data)
    }
  }

  test("V2StandingQueryDataMap preserves StandingQueryId UUIDs exactly") {
    forAll { (data: V2StandingQueryDataMap) =>
      val json = data.asJson(sqDataMapCodec.encoder)
      val decoded = json.as[V2StandingQueryDataMap](sqDataMapCodec.decoder)

      for {
        decodedData <- decoded
        (name, (originalId, _)) <- data
        (decodedId, _) <- decodedData.get(name)
      } decodedId shouldBe originalId
    }
  }

  test("V2StandingQueryDataMap encodes empty map correctly") {
    val empty: V2StandingQueryDataMap = Map.empty
    val json = empty.asJson(sqDataMapCodec.encoder)
    json.asObject.map(_.isEmpty) shouldBe Some(true)

    val decoded = json.as[V2StandingQueryDataMap](sqDataMapCodec.decoder)
    decoded shouldBe Right(empty)
  }

  test("V2StandingQueryDataMap encodes StandingQueryId as correct UUID string in JSON") {
    forAll { (data: V2StandingQueryDataMap) =>
      whenever(data.nonEmpty) {
        val json = data.asJson(sqDataMapCodec.encoder)
        val originalIds = data.values.map(_._1.uuid.toString).toSet
        val jsonIds = for {
          obj <- json.asObject.toVector
          (_, entry) <- obj.toVector
          arr <- entry.asArray
          sqIdJson <- arr.headOption
          uuidStr <- sqIdJson.asString
        } yield uuidStr

        jsonIds should contain theSameElementsAs originalIds
      }
    }
  }
}

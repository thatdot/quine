package com.thatdot.quine.ingest2

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.app.model.ingest2.V2IngestEntityEncoderDecoders._

class V2IngestEntitiesCodecSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import V2IngestEntitiesGenerators.Arbs._

  describe("BillingMode codec") {
    it("should roundtrip encode/decode") {
      forAll { (bm: BillingMode) =>
        val json = bm.asJson
        val decoded = json.as[BillingMode]
        decoded shouldBe Right(bm)
      }
    }
  }

  describe("MetricsLevel codec") {
    it("should roundtrip encode/decode") {
      forAll { (ml: MetricsLevel) =>
        val json = ml.asJson
        val decoded = json.as[MetricsLevel]
        decoded shouldBe Right(ml)
      }
    }
  }

  describe("MetricsDimension codec") {
    it("should roundtrip encode/decode") {
      forAll { (md: MetricsDimension) =>
        val json = md.asJson
        val decoded = json.as[MetricsDimension]
        decoded shouldBe Right(md)
      }
    }
  }

  describe("ClientVersionConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (cvc: ClientVersionConfig) =>
        val json = cvc.asJson
        val decoded = json.as[ClientVersionConfig]
        decoded shouldBe Right(cvc)
      }
    }
  }

  describe("ShardPrioritization codec") {
    it("should roundtrip encode/decode") {
      forAll { (sp: ShardPrioritization) =>
        val json = sp.asJson
        val decoded = json.as[ShardPrioritization]
        decoded shouldBe Right(sp)
      }
    }
  }

  describe("RetrievalSpecificConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (rsc: RetrievalSpecificConfig) =>
        val json = rsc.asJson
        val decoded = json.as[RetrievalSpecificConfig]
        decoded shouldBe Right(rsc)
      }
    }

    it("should include type discriminator") {
      forAll { (rsc: RetrievalSpecificConfig) =>
        val json = rsc.asJson
        val expectedType = rsc match {
          case _: RetrievalSpecificConfig.FanOutConfig => "FanOutConfig"
          case _: RetrievalSpecificConfig.PollingConfig => "PollingConfig"
        }
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("KinesisCheckpointSettings codec") {
    it("should roundtrip encode/decode") {
      forAll { (kcs: KinesisCheckpointSettings) =>
        val json = kcs.asJson
        val decoded = json.as[KinesisCheckpointSettings]
        decoded shouldBe Right(kcs)
      }
    }
  }

  describe("KinesisSchedulerSourceSettings codec") {
    it("should roundtrip encode/decode") {
      forAll { (ksss: KinesisSchedulerSourceSettings) =>
        val json = ksss.asJson
        val decoded = json.as[KinesisSchedulerSourceSettings]
        decoded shouldBe Right(ksss)
      }
    }
  }

  describe("ConfigsBuilder codec") {
    it("should roundtrip encode/decode") {
      forAll { (cb: ConfigsBuilder) =>
        val json = cb.asJson
        val decoded = json.as[ConfigsBuilder]
        decoded shouldBe Right(cb)
      }
    }
  }

  describe("LifecycleConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (lc: LifecycleConfig) =>
        val json = lc.asJson
        val decoded = json.as[LifecycleConfig]
        decoded shouldBe Right(lc)
      }
    }
  }

  describe("RetrievalConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (rc: RetrievalConfig) =>
        val json = rc.asJson
        val decoded = json.as[RetrievalConfig]
        decoded shouldBe Right(rc)
      }
    }
  }

  describe("ProcessorConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (pc: ProcessorConfig) =>
        val json = pc.asJson
        val decoded = json.as[ProcessorConfig]
        decoded shouldBe Right(pc)
      }
    }
  }

  describe("LeaseManagementConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (lmc: LeaseManagementConfig) =>
        val json = lmc.asJson
        val decoded = json.as[LeaseManagementConfig]
        decoded shouldBe Right(lmc)
      }
    }
  }

  describe("CoordinatorConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (cc: CoordinatorConfig) =>
        val json = cc.asJson
        val decoded = json.as[CoordinatorConfig]
        decoded shouldBe Right(cc)
      }
    }
  }

  describe("MetricsConfig codec") {
    it("should roundtrip encode/decode") {
      forAll { (mc: MetricsConfig) =>
        val json = mc.asJson
        val decoded = json.as[MetricsConfig]
        decoded shouldBe Right(mc)
      }
    }
  }

  describe("KCLConfiguration codec") {
    it("should roundtrip encode/decode") {
      forAll { (kcl: KCLConfiguration) =>
        val json = kcl.asJson
        val decoded = json.as[KCLConfiguration]
        decoded shouldBe Right(kcl)
      }
    }
  }
}

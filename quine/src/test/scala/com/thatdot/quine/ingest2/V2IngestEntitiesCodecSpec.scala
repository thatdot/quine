package com.thatdot.quine.ingest2

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.app.model.ingest2.V2IngestEntityEncoderDecoders._
import com.thatdot.quine.{routes => V1}

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

  describe("V1.AwsCredentials codec") {
    it("should roundtrip encode/decode") {
      forAll { (creds: V1.AwsCredentials) =>
        val json = creds.asJson
        val decoded = json.as[V1.AwsCredentials]
        decoded shouldBe Right(creds)
      }
    }
  }

  describe("V1.AwsRegion codec") {
    it("should roundtrip encode/decode") {
      forAll { (region: V1.AwsRegion) =>
        val json = region.asJson
        val decoded = json.as[V1.AwsRegion]
        decoded shouldBe Right(region)
      }
    }
  }

  describe("V1.KinesisIngest.IteratorType codec") {
    it("should roundtrip encode/decode") {
      forAll { (it: V1.KinesisIngest.IteratorType) =>
        val json = it.asJson
        val decoded = json.as[V1.KinesisIngest.IteratorType]
        decoded shouldBe Right(it)
      }
    }

    it("should include type discriminator") {
      forAll { (it: V1.KinesisIngest.IteratorType) =>
        val json = it.asJson
        val expectedType = it match {
          case V1.KinesisIngest.IteratorType.TrimHorizon => "TrimHorizon"
          case V1.KinesisIngest.IteratorType.Latest => "Latest"
          case _: V1.KinesisIngest.IteratorType.AtSequenceNumber => "AtSequenceNumber"
          case _: V1.KinesisIngest.IteratorType.AfterSequenceNumber => "AfterSequenceNumber"
          case _: V1.KinesisIngest.IteratorType.AtTimestamp => "AtTimestamp"
        }
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("Transformation codec") {
    it("should roundtrip encode/decode") {
      forAll { (t: Transformation) =>
        val json = t.asJson
        val decoded = json.as[Transformation]
        decoded shouldBe Right(t)
      }
    }

    it("should include type discriminator") {
      forAll { (t: Transformation) =>
        val json = t.asJson
        val expectedType = t match {
          case _: Transformation.JavaScript => "JavaScript"
        }
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("IngestSource codec") {
    it("should roundtrip encode/decode") {
      forAll { (is: IngestSource) =>
        val json = is.asJson
        val decoded = json.as[IngestSource]
        decoded shouldBe Right(is)
      }
    }

    it("should include type discriminator") {
      forAll { (is: IngestSource) =>
        val json = is.asJson
        val expectedType = is match {
          case _: FileIngest => "FileIngest"
          case _: S3Ingest => "S3Ingest"
          case _: ReactiveStreamIngest => "ReactiveStreamIngest"
          case _: WebSocketFileUpload => "WebSocketFileUpload"
          case _: StdInputIngest => "StdInputIngest"
          case _: NumberIteratorIngest => "NumberIteratorIngest"
          case _: WebsocketIngest => "WebsocketIngest"
          case _: KinesisIngest => "KinesisIngest"
          case _: KinesisKclIngest => "KinesisKclIngest"
          case _: ServerSentEventIngest => "ServerSentEventIngest"
          case _: SQSIngest => "SQSIngest"
          case _: KafkaIngest => "KafkaIngest"
        }
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("V1.IngestStreamStatus codec") {
    it("should roundtrip encode/decode") {
      forAll { (status: V1.IngestStreamStatus) =>
        val json = status.asJson
        val decoded = json.as[V1.IngestStreamStatus]
        decoded shouldBe Right(status)
      }
    }

    it("should encode as simple string (enumeration style)") {
      forAll { (status: V1.IngestStreamStatus) =>
        val json = status.asJson
        val expectedValue = status match {
          case V1.IngestStreamStatus.Running => "Running"
          case V1.IngestStreamStatus.Paused => "Paused"
          case V1.IngestStreamStatus.Restored => "Restored"
          case V1.IngestStreamStatus.Completed => "Completed"
          case V1.IngestStreamStatus.Terminated => "Terminated"
          case V1.IngestStreamStatus.Failed => "Failed"
        }
        json.as[String] shouldBe Right(expectedValue)
      }
    }
  }

  describe("IngestFormat codec") {
    // Note: IngestFormat roundtrip has ambiguity for JsonFormat because both
    // FileFormat.JsonFormat and StreamingFormat.JsonFormat serialize to {"type": "JsonFormat"}.
    // FileFormat and StreamingFormat are tested separately (via "IngestSource codec" tests above).
    it("should encode with correct type discriminator") {
      forAll { (format: IngestFormat) =>
        val json = format.asJson
        val typeField = json.hcursor.downField("type").as[String]
        val expectedType = format.getClass.getSimpleName.stripSuffix("$")
        typeField shouldBe Right(expectedType)
      }
    }

    it("should decode non-JsonFormat types correctly") {
      // Filter out JsonFormat types; they are differentiable only given their IngestSource context
      forAll { (format: IngestFormat) =>
        whenever(
          !format.isInstanceOf[FileFormat.JsonFormat.type] && !format.isInstanceOf[StreamingFormat.JsonFormat.type],
        ) {
          val json = format.asJson
          val decoded = json.as[IngestFormat]
          decoded shouldBe Right(format)
        }
      }
    }
  }

  describe("QuineIngestConfiguration codec") {
    it("should roundtrip encode/decode") {
      forAll { (config: QuineIngestConfiguration) =>
        val json = config.asJson
        val decoded = json.as[QuineIngestConfiguration]
        decoded shouldBe Right(config)
      }
    }

    it("should encode with correct field names") {
      forAll { (config: QuineIngestConfiguration) =>
        val json = config.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(config.name)
        json.hcursor.downField("query").as[String] shouldBe Right(config.query)
        json.hcursor.downField("parallelism").as[Int] shouldBe Right(config.parallelism)
      }
    }
  }

  describe("QuineIngestStreamWithStatus codec") {
    it("should roundtrip encode/decode") {
      forAll { (ingest: QuineIngestStreamWithStatus) =>
        val json = ingest.asJson
        val decoded = json.as[QuineIngestStreamWithStatus]
        decoded shouldBe Right(ingest)
      }
    }

    it("should encode with correct structure") {
      forAll { (ingest: QuineIngestStreamWithStatus) =>
        val json = ingest.asJson
        json.hcursor.downField("config").succeeded shouldBe true
        // status is optional
        ingest.status.foreach { status =>
          val expectedValue = status match {
            case V1.IngestStreamStatus.Running => "Running"
            case V1.IngestStreamStatus.Paused => "Paused"
            case V1.IngestStreamStatus.Restored => "Restored"
            case V1.IngestStreamStatus.Completed => "Completed"
            case V1.IngestStreamStatus.Terminated => "Terminated"
            case V1.IngestStreamStatus.Failed => "Failed"
          }
          json.hcursor.downField("status").as[String] shouldBe Right(expectedValue)
        }
      }
    }
  }
}

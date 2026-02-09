package com.thatdot.quine.ingest2

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.model.ingest2.V1IngestCodecs._
import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.app.model.ingest2._
import com.thatdot.quine.{routes => V1}

/** Codec tests for V2 Ingest types verifying default (API-facing) behavior.
  *
  * For preserving encoder tests (persistence), see [[V2IngestEntitiesPreservingCodecSpec]].
  */
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
    import com.thatdot.common.security.Secret
    import io.circe.parser.parse

    it("should encode with redacted credential values") {
      val creds = V1.AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG"))
      val json = creds.asJson

      json.hcursor.downField("accessKeyId").as[String] shouldBe Right("Secret(****)")
      json.hcursor.downField("secretAccessKey").as[String] shouldBe Right("Secret(****)")
    }

    it("should decode from JSON with credential values") {
      import Secret.Unsafe._
      val json = parse("""{"accessKeyId": "AKIAIOSFODNN7EXAMPLE", "secretAccessKey": "wJalrXUtnFEMI/K7MDENG"}""")
        .getOrElse(fail("Invalid JSON"))
      val decoded = json.as[V1.AwsCredentials].getOrElse(fail("Failed to decode"))

      decoded.accessKeyId.unsafeValue shouldBe "AKIAIOSFODNN7EXAMPLE"
      decoded.secretAccessKey.unsafeValue shouldBe "wJalrXUtnFEMI/K7MDENG"
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
    import com.thatdot.common.security.Secret

    it("should encode with type discriminator") {
      val source: IngestSource = SQSIngest(
        format = StreamingFormat.JsonFormat,
        queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        credentials = None,
        region = None,
      )
      val json = source.asJson

      json.hcursor.downField("type").as[String] shouldBe Right("SQSIngest")
    }

    it("should redact credentials in encoded JSON") {
      val source: IngestSource = SQSIngest(
        format = StreamingFormat.JsonFormat,
        queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        credentials = Some(V1.AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG"))),
        region = Some(V1.AwsRegion("us-east-1")),
      )
      val json = source.asJson

      json.hcursor.downField("credentials").downField("accessKeyId").as[String] shouldBe Right("Secret(****)")
      json.hcursor.downField("credentials").downField("secretAccessKey").as[String] shouldBe Right("Secret(****)")
    }

    it("should decode from JSON with credential values") {
      import Secret.Unsafe._
      import io.circe.parser.parse
      val json = parse("""{
        "type": "SQSIngest",
        "format": {"type": "JsonFormat"},
        "queueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        "credentials": {"accessKeyId": "AKIAIOSFODNN7EXAMPLE", "secretAccessKey": "wJalrXUtnFEMI/K7MDENG"},
        "region": {"region": "us-east-1"}
      }""").getOrElse(fail("Invalid JSON"))

      val decoded = json.as[IngestSource].getOrElse(fail("Failed to decode IngestSource"))

      decoded match {
        case sqs: SQSIngest =>
          val creds = sqs.credentials.getOrElse(fail("Credentials missing"))
          creds.accessKeyId.unsafeValue shouldBe "AKIAIOSFODNN7EXAMPLE"
          creds.secretAccessKey.unsafeValue shouldBe "wJalrXUtnFEMI/K7MDENG"
        case other =>
          fail(s"Expected SQSIngest but got: $other")
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

  describe("V2 IngestStreamStatus codec") {
    it("should roundtrip encode/decode") {
      forAll { (status: IngestStreamStatus) =>
        val json = status.asJson
        val decoded = json.as[IngestStreamStatus]
        decoded shouldBe Right(status)
      }
    }

    it("should include type discriminator") {
      forAll { (status: IngestStreamStatus) =>
        val json = status.asJson
        val expectedType = status match {
          case IngestStreamStatus.Running => "Running"
          case IngestStreamStatus.Paused => "Paused"
          case IngestStreamStatus.Restored => "Restored"
          case IngestStreamStatus.Completed => "Completed"
          case IngestStreamStatus.Terminated => "Terminated"
          case IngestStreamStatus.Failed => "Failed"
        }
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("RatesSummary codec") {
    it("should roundtrip encode/decode") {
      forAll { (rs: RatesSummary) =>
        val json = rs.asJson
        val decoded = json.as[RatesSummary]
        decoded shouldBe Right(rs)
      }
    }

    it("should encode with correct field names") {
      forAll { (rs: RatesSummary) =>
        val json = rs.asJson
        json.hcursor.downField("count").as[Long] shouldBe Right(rs.count)
        json.hcursor.downField("oneMinute").as[Double] shouldBe Right(rs.oneMinute)
        json.hcursor.downField("fiveMinute").as[Double] shouldBe Right(rs.fiveMinute)
        json.hcursor.downField("fifteenMinute").as[Double] shouldBe Right(rs.fifteenMinute)
        json.hcursor.downField("overall").as[Double] shouldBe Right(rs.overall)
      }
    }
  }

  describe("IngestStreamStats codec") {
    it("should roundtrip encode/decode") {
      forAll { (iss: IngestStreamStats) =>
        val json = iss.asJson
        val decoded = json.as[IngestStreamStats]
        decoded shouldBe Right(iss)
      }
    }

    it("should encode with correct field names") {
      forAll { (iss: IngestStreamStats) =>
        val json = iss.asJson
        json.hcursor.downField("ingestedCount").as[Long] shouldBe Right(iss.ingestedCount)
        json.hcursor.downField("rates").succeeded shouldBe true
        json.hcursor.downField("byteRates").succeeded shouldBe true
        json.hcursor.downField("startTime").succeeded shouldBe true
        json.hcursor.downField("totalRuntime").as[Long] shouldBe Right(iss.totalRuntime)
      }
    }
  }

  describe("IngestStreamInfo codec") {
    it("should encode and decode successfully preserving status") {
      forAll { (isi: IngestStreamInfo) =>
        val json = isi.asJson
        val decoded = json.as[IngestStreamInfo].getOrElse(fail("Failed to decode IngestStreamInfo"))
        decoded.status shouldBe isi.status
      }
    }

    it("should encode with correct field names") {
      forAll { (isi: IngestStreamInfo) =>
        val json = isi.asJson
        json.hcursor.downField("status").succeeded shouldBe true
        json.hcursor.downField("settings").succeeded shouldBe true
        json.hcursor.downField("stats").succeeded shouldBe true
      }
    }

    it("should redact credentials in encoded JSON") {
      import com.thatdot.common.security.Secret
      val info = IngestStreamInfo(
        status = IngestStreamStatus.Running,
        message = None,
        settings = SQSIngest(
          format = StreamingFormat.JsonFormat,
          queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/queue",
          credentials = Some(V1.AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG"))),
          region = Some(V1.AwsRegion("us-east-1")),
        ),
        stats = IngestStreamStats(0, RatesSummary(0, 0, 0, 0, 0), RatesSummary(0, 0, 0, 0, 0), java.time.Instant.now, 0),
      )
      val json = info.asJson

      json.hcursor.downField("settings").downField("credentials").downField("accessKeyId").as[String] shouldBe
      Right("Secret(****)")
    }
  }

  describe("IngestStreamInfoWithName codec") {
    it("should encode and decode successfully preserving name and status") {
      forAll { (isi: IngestStreamInfoWithName) =>
        val json = isi.asJson
        val decoded = json.as[IngestStreamInfoWithName].getOrElse(fail("Failed to decode IngestStreamInfoWithName"))
        decoded.name shouldBe isi.name
        decoded.status shouldBe isi.status
      }
    }

    it("should encode with correct field names") {
      forAll { (isi: IngestStreamInfoWithName) =>
        val json = isi.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(isi.name)
        json.hcursor.downField("status").succeeded shouldBe true
        json.hcursor.downField("settings").succeeded shouldBe true
        json.hcursor.downField("stats").succeeded shouldBe true
      }
    }

    it("should redact credentials in encoded JSON") {
      import com.thatdot.common.security.Secret
      val info = IngestStreamInfoWithName(
        name = "test-ingest",
        status = IngestStreamStatus.Running,
        message = None,
        settings = SQSIngest(
          format = StreamingFormat.JsonFormat,
          queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/queue",
          credentials = Some(V1.AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG"))),
          region = Some(V1.AwsRegion("us-east-1")),
        ),
        stats = IngestStreamStats(0, RatesSummary(0, 0, 0, 0, 0), RatesSummary(0, 0, 0, 0, 0), java.time.Instant.now, 0),
      )
      val json = info.asJson

      json.hcursor.downField("settings").downField("credentials").downField("accessKeyId").as[String] shouldBe
      Right("Secret(****)")
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

  // Note: roundtrip equality can't be tested because API codecs intentionally redact
  // Secret values (AWS credentials in SQS/Kinesis ingests).
  describe("QuineIngestConfiguration codec") {
    it("should encode and decode successfully preserving non-credential fields") {
      forAll { (config: QuineIngestConfiguration) =>
        val json = config.asJson
        val decoded = json.as[QuineIngestConfiguration].getOrElse(fail("Failed to decode QuineIngestConfiguration"))
        decoded.name shouldBe config.name
        decoded.query shouldBe config.query
        decoded.parallelism shouldBe config.parallelism
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

    it("should redact credentials in encoded JSON") {
      import com.thatdot.common.security.Secret
      val config = QuineIngestConfiguration(
        name = "test-config",
        source = SQSIngest(
          format = StreamingFormat.JsonFormat,
          queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/queue",
          credentials = Some(V1.AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG"))),
          region = Some(V1.AwsRegion("us-east-1")),
        ),
        query = "CREATE ($that)",
      )
      val json = config.asJson

      json.hcursor.downField("source").downField("credentials").downField("accessKeyId").as[String] shouldBe
      Right("Secret(****)")
      json.hcursor.downField("source").downField("credentials").downField("secretAccessKey").as[String] shouldBe
      Right("Secret(****)")
    }
  }

  describe("QuineIngestStreamWithStatus codec") {
    it("should encode and decode successfully preserving status") {
      forAll { (ingest: QuineIngestStreamWithStatus) =>
        val json = ingest.asJson
        val decoded =
          json.as[QuineIngestStreamWithStatus].getOrElse(fail("Failed to decode QuineIngestStreamWithStatus"))
        decoded.status shouldBe ingest.status
      }
    }

    it("should redact credentials in encoded JSON") {
      import com.thatdot.common.security.Secret
      val config = QuineIngestConfiguration(
        name = "test-config",
        source = SQSIngest(
          format = StreamingFormat.JsonFormat,
          queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/queue",
          credentials = Some(V1.AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG"))),
          region = Some(V1.AwsRegion("us-east-1")),
        ),
        query = "CREATE ($that)",
      )
      val ingest = QuineIngestStreamWithStatus(config, Some(V1.IngestStreamStatus.Running))
      val json = ingest.asJson

      json.hcursor
        .downField("config")
        .downField("source")
        .downField("credentials")
        .downField("accessKeyId")
        .as[String] shouldBe Right("Secret(****)")
      json.hcursor
        .downField("config")
        .downField("source")
        .downField("credentials")
        .downField("secretAccessKey")
        .as[String] shouldBe Right("Secret(****)")
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

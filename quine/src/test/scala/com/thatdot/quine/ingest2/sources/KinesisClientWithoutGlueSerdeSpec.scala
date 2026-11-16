package com.thatdot.quine.ingest2.sources

import java.nio.ByteBuffer

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.retrieval.{KinesisClientRecord, RetrievalConfig}

/** Tests that the Kinesis Client Library (KCL) doesn't break even with `software.amazon.glue:schema-registry-serde`
  * excluded from the classpath.
  *
  * The KCL only uses that library to decode records encoded with the AWS Glue Schema Registry, and only when it is
  * given a `GlueSchemaRegistryDeserializer`, which Quine never does. The compiler cannot check the KCL's own code
  * against the missing library, so these tests load every KCL class that refers to it and call the KCL methods
  * whose signatures mention it.
  */
class KinesisClientWithoutGlueSerdeSpec extends AnyFunSpec with Matchers {

  describe("the classpath") {
    it("has no schema-registry-serde") {
      a[ClassNotFoundException] should be thrownBy Class.forName(
        "com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer",
      )
    }
  }

  describe("the KCL") {
    it("loads, verifies, and initializes every class that refers to the Glue library") {
      // Class.forName links each class, which makes the JVM verify the bytecode of all of its methods.
      Seq(
        "software.amazon.kinesis.retrieval.RetrievalConfig",
        "software.amazon.kinesis.retrieval.KinesisClientRecord",
        "software.amazon.kinesis.schemaregistry.SchemaRegistryDecoder",
        "software.amazon.kinesis.lifecycle.ProcessTask",
        "software.amazon.kinesis.lifecycle.ShardConsumerArgument",
        "software.amazon.kinesis.coordinator.Scheduler",
      ).foreach { className =>
        withClue(s"$className: ") {
          noException should be thrownBy Class.forName(className)
        }
      }
    }

    it("builds a RetrievalConfig and calls its Glue deserializer getter") {
      // A stub client, so that no AWS SDK client is built. RetrievalConfig only stores it.
      val kinesis = new KinesisAsyncClient {
        def serviceName(): String = "kinesis"
        def close(): Unit = ()
      }
      val retrievalConfig = new RetrievalConfig(kinesis, "stream", "app")

      // These Lombok-generated methods call `glueSchemaRegistryDeserializer()`, whose return type is missing.
      retrievalConfig.toString should include("glueSchemaRegistryDeserializer=null")
      retrievalConfig shouldEqual retrievalConfig
      noException should be thrownBy retrievalConfig.hashCode()
    }

    it("builds, copies, and prints records, whose schema is null") {
      val record = KinesisClientRecord.builder().data(ByteBuffer.wrap(Array[Byte](1, 2, 3))).sequenceNumber("1").build()
      record.schema() shouldBe null
      record.toBuilder.build() shouldEqual record
      record.toString should include("schema=null")
    }
  }
}

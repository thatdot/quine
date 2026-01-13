package com.thatdot.quine.ingest2.sources

import java.util.Optional

import io.circe.Json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.data.DataFolderTo
import com.thatdot.quine.app.model.ingest2.sources.KafkaSource.{NoOffset, noOffsetFoldable}

class KafkaFoldableSpec extends AnyFunSpec with Matchers {

  private def createConsumerRecord(
    topic: String = "test-topic",
    partition: Int = 0,
    offset: Long = 0L,
    key: Array[Byte] = null,
    value: Array[Byte] = "test-value".getBytes,
    headers: RecordHeaders = new RecordHeaders(),
  ): NoOffset =
    new ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      partition,
      offset,
      ConsumerRecord.NO_TIMESTAMP,
      TimestampType.NO_TIMESTAMP_TYPE,
      ConsumerRecord.NULL_SIZE,
      ConsumerRecord.NULL_SIZE,
      key,
      value,
      headers,
      Optional.empty[Integer](),
    )

  describe("noOffsetFoldable") {

    describe("key handling") {

      // Per Kafka API documentation:
      // - ConsumerRecord.key(): "The key (or null if no key is specified)"
      //   https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html#key()
      // - ProducerRecord(topic, value): "Create a record with no key"
      //   https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html
      //
      // When key is null, it means "no key was specified" - not "key with null value".
      // Therefore, we omit the key field entirely rather than serializing as {"key": null}.
      it("omits key field when key is null (no key specified)") {
        val record = createConsumerRecord(key = null)
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj.contains("key") shouldBe false
        obj.contains("value") shouldBe true
        obj.contains("topic") shouldBe true
      }

      it("includes key field when key is non-null") {
        val keyBytes = "my-key".getBytes
        val record = createConsumerRecord(key = keyBytes)
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj.contains("key") shouldBe true
        obj("key").get.isString shouldBe true
      }

      it("includes key field when key is empty byte array") {
        val record = createConsumerRecord(key = Array.emptyByteArray)
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        // Empty array is still a valid key - should be included
        obj.contains("key") shouldBe true
      }
    }

    describe("value handling") {

      it("includes value field when value is non-null") {
        val record = createConsumerRecord(value = "test".getBytes)
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj.contains("value") shouldBe true
      }
    }

    describe("metadata fields") {

      it("always includes topic, partition, offset") {
        val record = createConsumerRecord(
          topic = "my-topic",
          partition = 5,
          offset = 12345L,
        )
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj("topic").get shouldBe Json.fromString("my-topic")
        obj("partition").get shouldBe Json.fromLong(5)
        obj("offset").get shouldBe Json.fromLong(12345L)
      }

      it("includes serializedKeySize and serializedValueSize") {
        val record = createConsumerRecord()
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj.contains("serializedKeySize") shouldBe true
        obj.contains("serializedValueSize") shouldBe true
      }
    }

    describe("headers handling") {

      it("omits headers field when headers is empty") {
        val record = createConsumerRecord(headers = new RecordHeaders())
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj.contains("headers") shouldBe false
      }

      it("includes headers field when headers are present") {
        val headers = new RecordHeaders()
        headers.add(new RecordHeader("header-key", "header-value".getBytes))

        val record = createConsumerRecord(headers = headers)
        val json = noOffsetFoldable.fold(record, DataFolderTo.jsonFolder)
        val obj = json.asObject.get

        obj.contains("headers") shouldBe true
        val headersObj = obj("headers").get.asObject.get
        headersObj.contains("header-key") shouldBe true
      }
    }
  }
}

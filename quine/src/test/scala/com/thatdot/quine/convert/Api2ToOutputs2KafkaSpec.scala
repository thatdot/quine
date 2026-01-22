package com.thatdot.quine.convert

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.outputs.{DestinationSteps, OutputFormat}
import com.thatdot.convert.Api2ToOutputs2
import com.thatdot.outputs2.FoldableDestinationSteps
import com.thatdot.outputs2.destination.Kafka
import com.thatdot.quine.graph.FakeQuineGraph
import com.thatdot.quine.serialization.ProtobufSchemaCache

class Api2ToOutputs2KafkaSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var graph: FakeQuineGraph = _
  implicit private var ec: ExecutionContext = _
  implicit private val protobufSchemaCache: ProtobufSchemaCache =
    //noinspection ScalaDeprecation
    ProtobufSchemaCache.Blocking: @nowarn("cat=deprecation")

  override def beforeAll(): Unit = {
    super.beforeAll()
    graph = new FakeQuineGraph()
    ec = graph.system.dispatcher
  }

  override def afterAll(): Unit = {
    if (graph != null) {
      graph.system.terminate()
    }
    super.afterAll()
  }

  test("Kafka kafkaProperties should contain raw string values, not KafkaPropertyValue wrappers") {
    implicit val g: FakeQuineGraph = graph

    val apiKafka = DestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9093",
      format = OutputFormat.JSON,
      kafkaProperties = Map(
        "security.protocol" -> KafkaPropertyValue("SASL_SSL"),
        "sasl.mechanism" -> KafkaPropertyValue("PLAIN"),
        "ssl.truststore.password" -> KafkaPropertyValue("test-password"),
      ),
    )

    val futureResult = Api2ToOutputs2(apiKafka)
    val result = Await.result(futureResult, 5.seconds)

    val internalKafka = result match {
      case FoldableDestinationSteps.WithByteEncoding(_, kafka: Kafka) => kafka
      case other => fail(s"Expected WithByteEncoding containing Kafka, got: $other")
    }

    internalKafka.kafkaProperties("security.protocol") shouldBe "SASL_SSL"
    internalKafka.kafkaProperties("sasl.mechanism") shouldBe "PLAIN"
    internalKafka.kafkaProperties("ssl.truststore.password") shouldBe "test-password"

    // Verify the values do not contain the wrapper class name
    internalKafka.kafkaProperties.values.foreach { value =>
      value should not include "KafkaPropertyValue"
    }
  }

  test("Kafka conversion preserves topic and bootstrapServers") {
    implicit val g: FakeQuineGraph = graph

    val apiKafka = DestinationSteps.Kafka(
      topic = "my-topic",
      bootstrapServers = "broker1:9092,broker2:9092",
      format = OutputFormat.JSON,
      kafkaProperties = Map.empty,
    )

    val futureResult = Api2ToOutputs2(apiKafka)
    val result = Await.result(futureResult, 5.seconds)

    val internalKafka = result match {
      case FoldableDestinationSteps.WithByteEncoding(_, kafka: Kafka) => kafka
      case other => fail(s"Expected WithByteEncoding containing Kafka, got: $other")
    }

    internalKafka.topic shouldBe "my-topic"
    internalKafka.bootstrapServers shouldBe "broker1:9092,broker2:9092"
  }
}

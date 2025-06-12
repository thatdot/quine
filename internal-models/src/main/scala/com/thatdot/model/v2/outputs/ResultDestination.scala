package com.thatdot.model.v2.outputs

import com.thatdot.aws.model.{AwsCredentials, AwsRegion}

trait SinkName {
  def slug: String
  def sinkName(outputName: String): String = s"result-destination--$slug--$outputName"
}

/** The interface (despite the API needing an ADT) for result destinations,
  * which are adapters for sending/writing to a location.
  */
sealed trait ResultDestination extends SinkName

object ResultDestination {

  sealed trait Bytes extends ResultDestination with ByteArraySink

  object Bytes {
    trait ReactiveStream extends Bytes {
      def address: String
      def port: Int
    }
    trait StandardOut extends Bytes {
      def logLevel: destination.StandardOut.LogLevel
      def logMode: destination.StandardOut.LogMode
    }
    trait SNS extends Bytes {
      def credentials: Option[AwsCredentials]
      def region: Option[AwsRegion]
      def topic: String
    }
    trait Kafka extends Bytes {
      def topic: String
      def bootstrapServers: String
      def kafkaProperties: Map[String, String]
    }
    trait Kinesis extends Bytes {
      def credentials: Option[AwsCredentials]
      def region: Option[AwsRegion]
      def streamName: String
      def kinesisParallelism: Option[Int]
      def kinesisMaxBatchSize: Option[Int]
      def kinesisMaxRecordsPerSecond: Option[Int]
      def kinesisMaxBytesPerSecond: Option[Int]
    }
    trait File extends Bytes {
      def path: String
    }
  }

  sealed trait FoldableData extends ResultDestination with DataFoldableSink

  object FoldableData {
    trait HttpEndpoint extends FoldableData {
      def url: String
      def parallelism: Int
    }
  }

  sealed trait AnyData extends ResultDestination with AnySink

  object AnyData {
    trait Drop extends AnyData
  }
}

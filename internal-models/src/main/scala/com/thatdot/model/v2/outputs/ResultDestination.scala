package com.thatdot.model.v2.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.aws.model.{AwsCredentials, AwsRegion}
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.graph.NamespaceId

/** The interface (despite the API needing an ADT) for result destinations,
  * which are adapters for sending/writing to a location.
  */
sealed trait ResultDestination

object ResultDestination {
  sealed trait Bytes extends ResultDestination {
    def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Array[Byte], NotUsed]
  }

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

  sealed trait FoldableData extends ResultDestination {
    def sink[A: DataFoldableFrom](name: String, inNamespace: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[A, NotUsed]
  }

  object FoldableData {
    trait HttpEndpoint extends FoldableData {
      def url: String
      def parallelism: Int
    }
  }

  sealed trait AnyData extends ResultDestination {
    def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Any, NotUsed]
  }

  object AnyData {
    trait Drop extends AnyData
  }
}

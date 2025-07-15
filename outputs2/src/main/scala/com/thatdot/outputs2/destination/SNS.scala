package com.thatdot.outputs2.destination

import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.sns.scaladsl.SnsPublisher
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.ByteString

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sns.SnsAsyncClient

import com.thatdot.aws.model.{AwsCredentials, AwsRegion}
import com.thatdot.aws.util.AwsOps
import com.thatdot.aws.util.AwsOps.AwsBuilderOps
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.outputs2.ResultDestination
import com.thatdot.quine.graph.NamespaceId

final case class SNS(
  credentials: Option[AwsCredentials],
  region: Option[AwsRegion],
  topic: String,
) extends ResultDestination.Bytes.SNS {
  override def slug: String = "sns"

  override def sink(name: String, inNamespace: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[Array[Byte], NotUsed] = {
    val awsSnsClient = SnsAsyncClient
      .builder()
      .credentialsV2(credentials)
      .regionV2(region)
      .httpClient(
        NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build(),
      )
      .build()

    def closeClient(): Unit = awsSnsClient.close()

    // NOTE pekko-connectors requires we close the SNS client
    val lifecycleSink = Sink.onComplete {
      case Failure(exception) =>
        closeClient()
      case Success(value) =>
        closeClient()
    }

    // NB: by default, this will make 10 parallel requests [configurable via parameter to SnsPublisher.flow]
    // TODO if any request to SNS errors, that thread (of the aforementioned 10) will retry its request
    // indefinitely. If all worker threads block, the SnsPublisher.flow will backpressure indefinitely.
    SnsPublisher
      .flow(topic)(awsSnsClient)
      .named(sinkName(name))
      .contramap[Array[Byte]](ByteString(_).utf8String)
      .mapMaterializedValue(_ => NotUsed)
      .to(lifecycleSink)
  }
}

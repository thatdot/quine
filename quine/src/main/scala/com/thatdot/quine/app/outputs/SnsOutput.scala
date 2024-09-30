package com.thatdot.quine.app.outputs
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.sns.scaladsl.SnsPublisher
import org.apache.pekko.stream.scaladsl.{Flow, Keep}

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sns.SnsAsyncClient

import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToSNS
import com.thatdot.quine.util.Log._

class SnsOutput(val config: WriteToSNS)(implicit private val logConfig: LogConfig) extends OutputRuntime {

  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val token = execToken(name, inNamespace)
    val WriteToSNS(credentialsOpt, regionOpt, topic) = config
    val awsSnsClient = SnsAsyncClient
      .builder()
      .credentials(credentialsOpt)
      .region(regionOpt)
      .httpClient(
        NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build(),
      )
      .build()

    // NOTE pekko-connectors requires we close the SNS client
    graph.system.registerOnTermination(awsSnsClient.close())

    // NB: by default, this will make 10 parallel requests [configurable via parameter to SnsPublisher.flow]
    // TODO if any request to SNS errors, that thread (of the aforementioned 10) will retry its request
    // indefinitely. If all worker threads block, the SnsPublisher.flow will backpressure indefinitely.
    Flow[StandingQueryResult]
      .map(result => result.toJson(graph.idProvider, logConfig).noSpaces + "\n")
      .viaMat(SnsPublisher.flow(topic)(awsSnsClient).named(s"sq-output-sns-producer-for-$name"))(Keep.right)
      .map(_ => token)
  }
}

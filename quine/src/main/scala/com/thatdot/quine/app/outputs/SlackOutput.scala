package com.thatdot.quine.app.outputs

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.quine.app.StandingQueryResultOutput.SlackSerializable
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.PostToSlack
import com.thatdot.quine.util.Log._

class SlackOutput(val config: PostToSlack)(implicit private val logConfig: LogConfig)
    extends OutputRuntime
    with LazySafeLogging {

  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val token = execToken(name, inNamespace)
    val PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds) = config

    implicit val system: ActorSystem = graph.system
    implicit val idProvider: QuineIdProvider = graph.idProvider
    val http = Http(graph.system)

    // how often to send notifications (notifications will be batched by [[PostToSlack.SlackSerializable.apply]]
    val rate = math.max(1, intervalSeconds).seconds

    Flow[StandingQueryResult]
      .conflateWithSeed(List(_))((acc, newResult) => newResult :: acc)
      .throttle(1, rate) // Slack webhooks have a 1 message per second rate limit
      .map(newResults => SlackSerializable(onlyPositiveMatchData, newResults))
      .collect { case Some(slackMessage) => slackMessage }
      .mapAsync(1) { result =>
        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = hookUrl,
          entity = HttpEntity.apply(contentType = `application/json`, result.slackJson),
        )
        val posted = http
          .singleRequest(request)
          .flatMap { response =>
            if (response.status.isSuccess()) {
              response.entity
                .discardBytes()
                .future()
            } else {
              Unmarshal(response)
                .to[String]
                .andThen {
                  case Failure(err) =>
                    logger.error(
                      log"""Failed to deserialize error response from POST ${result.slackJson} to slack webhook.
                             |Response status was ${Safe(response.status.value)}
                             |""".cleanLines
                      withException err,
                    )
                  case Success(responseBody) =>
                    logger.error(
                      log"Failed to POST ${result.slackJson} to slack webhook. " +
                      log"Response status was ${Safe(response.status.value)}: " +
                      log"${Safe(responseBody)}",
                    )
                }(system.dispatcher)
            }
          }(system.dispatcher)
          .map(_ => token)(system.dispatcher)

        // TODO: principled error handling
        posted.recover { case err =>
          logger.error(log"Failed to POST standing query result" withException err)
          token
        }(system.dispatcher)
      }
  }
}

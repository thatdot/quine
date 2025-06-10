package com.thatdot.quine.app.model.outputs2

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import io.circe.Json

import com.thatdot.common.logging.Log._
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.StandingQueryResultOutput.SlackSerializable
import com.thatdot.quine.app.util.QuineLoggables.LogStatusCode
import com.thatdot.quine.graph.NamespaceId

final case class Slack(
  hookUrl: String,
  onlyPositiveMatchData: Boolean = false,
  intervalSeconds: Int = 20,
)(implicit system: ActorSystem)
    extends QuineResultDestination.FoldableData.Slack
    with LazySafeLogging {

  override def sink[A: DataFoldableFrom](name: String, inNamespace: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[A, NotUsed] = {
    val http: HttpExt = Http(system)

    // Slack webhooks have a 1 message per second rate limit
    val rate: FiniteDuration = math.max(1, intervalSeconds).seconds

    Flow[A]
      .map(DataFoldableFrom[A].to[Json])
      .conflateWithSeed(List(_))((acc, newResult) => newResult :: acc)
      .throttle(1, rate)
      .map(results => SlackSerializable(results))
      .collect { case Some(slackMessage) => slackMessage }
      .mapAsync(1) { slackSerializable =>
        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = hookUrl,
          entity = HttpEntity.apply(contentType = `application/json`, slackSerializable.slackJson),
        )
        val posted = http
          .singleRequest(request)
          .flatMap { response =>
            if (response.status.isSuccess()) {
              response.entity
                .discardBytes()
                .future()
                .map(_ => ())(ExecutionContext.parasitic)
            } else {
              Unmarshal(response)
                .to[String]
                .andThen {
                  case Failure(err) =>
                    // FIXME Not importing/mixing in right logging stuff
                    logger.error(
                      log"""Failed to deserialize error response from POST ${slackSerializable.slackJson} to Slack webhook.
                               |Response status was ${response.status}
                               |""".cleanLines
                      withException err,
                    )
                  case Success(responseBody) =>
                    logger.error(
                      log"""Failed to POST ${slackSerializable.slackJson} to Slack webhook.
                               |Response status was ${response.status}
                               |""".cleanLines + log": ${Safe(responseBody)}",
                    )
                }(system.dispatcher)
                .map(_ => ())(ExecutionContext.parasitic)
            }
          }(system.dispatcher)

        posted.recover { case err =>
          logger.error(log"Failed to POST standing query result" withException err)
        }(system.dispatcher)
      }
      .to(Sink.ignore)
  }
}

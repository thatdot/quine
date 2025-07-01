package com.thatdot.model.v2.outputs.destination

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import io.circe.Json

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.data.DataFoldableFrom
import com.thatdot.model.v2.outputs.OutputsLoggables.LogStatusCode
import com.thatdot.model.v2.outputs.ResultDestination
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.util.Log.implicits._

final case class HttpEndpoint(
  url: String,
  parallelism: Int = 8,
)(implicit system: ActorSystem)
    extends ResultDestination.FoldableData.HttpEndpoint
    with LazySafeLogging {
  override def slug: String = "http"

  override def sink[A: DataFoldableFrom](name: String, inNamespace: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[A, NotUsed] = {
    val http = Http()
    val toJson = DataFoldableFrom[A].to[Json]

    Flow[A]
      .mapAsync(parallelism) { (a: A) =>
        val json = toJson(a)
        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = url,
          entity = HttpEntity(
            contentType = `application/json`,
            json.noSpaces.getBytes,
          ),
        )

        val posted: Future[Unit] =
          http
            .singleRequest(request)
            .flatMap(response =>
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
                      logger.error(
                        log"""Failed to deserialize error response from POST ${Safe(json.toString)} to ${Safe(url)}.
                             |Response status was ${response.status}""".cleanLines
                        withException err,
                      )
                    case Success(responseBody) =>
                      logger.error(
                        log"""Failed to POST ${Safe(json.toString)} to ${Safe(url)}.
                             |Response was ${response.status}
                             |""".cleanLines + log": ${Safe(responseBody)}",
                      )
                  }(system.dispatcher)
                  .map(_ => ())(ExecutionContext.parasitic)
              },
            )(system.dispatcher)

        posted.recover { case err =>
          logger.error(log"Failed to POST result" withException err)
        }(system.dispatcher)
      }
      .to(Sink.ignore)
      .named(sinkName(name))
  }
}

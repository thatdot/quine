package com.thatdot.quine.app.outputs

import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.quine.app.util.QuineLoggables._
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.PostToEndpoint
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

class PostToEndpointOutput(val config: PostToEndpoint)(implicit private val logConfig: LogConfig)
    extends OutputRuntime
    with LazySafeLogging {

  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val PostToEndpoint(url, parallelism, onlyPositiveMatchData, structure) = config
    val token = execToken(name, inNamespace)

    // TODO: use a host connection pool

    implicit val system: ActorSystem = graph.system
    implicit val idProvider: QuineIdProvider = graph.idProvider
    val http = Http()

    Flow[StandingQueryResult]
      .mapAsync(parallelism) { (result: StandingQueryResult) =>
        val request = HttpRequest(
          method = HttpMethods.POST,
          uri = url,
          entity = HttpEntity(
            contentType = `application/json`,
            if (onlyPositiveMatchData) QuineValue.toJson(QuineValue.Map(result.data)).noSpaces
            else result.toJson(structure).noSpaces,
          ),
        )

        val posted =
          http
            .singleRequest(request)
            .flatMap(response =>
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
                        log"""Failed to deserialize error response from POST $result to ${Safe(url)}.
                             |Response status was ${response.status}""".cleanLines
                        withException err,
                      )
                    case Success(responseBody) =>
                      logger.error(
                        log"""Failed to POST $result to ${Safe(url)}.
                             |Response was ${response.status}
                             |""".cleanLines + log": ${Safe(responseBody)}",
                      )
                  }(system.dispatcher)
              },
            )(system.dispatcher)
            .map(_ => token)(system.dispatcher)

        // TODO: principled error handling
        posted.recover { case err =>
          logger.error(log"Failed to POST standing query result" withException err)
          token
        }(system.dispatcher)
      }
  }
}

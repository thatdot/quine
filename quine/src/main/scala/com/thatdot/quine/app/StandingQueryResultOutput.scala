package com.thatdot.quine.app

import java.nio.file.{Paths, StandardOpenOption}
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Random, Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.MediaTypes.`application/json`
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.kafka.scaladsl.{Producer => KafkaProducer}
import org.apache.pekko.kafka.{ProducerMessage, ProducerSettings}
import org.apache.pekko.stream.connectors.kinesis.KinesisFlowSettings
import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisFlow
import org.apache.pekko.stream.connectors.sns.scaladsl.SnsPublisher
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, Keep, Source}
import org.apache.pekko.util.ByteString

import cats.syntax.either._
import io.circe.Json
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.awssdk.services.sns.SnsAsyncClient

import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.serialization.{ConversionFailure, ProtobufSchemaCache, QuineValueToProtobuf}
import com.thatdot.quine.app.util.AtLeastOnceCypherQuery
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph, NamespaceId, StandingQueryResult, cypher}
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}
import com.thatdot.quine.routes.{OutputFormat, StandingQueryResultOutputUserDef}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.PekkoStreams.wireTapFirst
import com.thatdot.quine.util.StringInput.filenameOrUrl

object StandingQueryResultOutput extends LazySafeLogging {

  import StandingQueryResultOutputUserDef._

  // Invariant: these keys must be fixed to the names of the loggers in Quine App's application.conf
  private val printLogger = SafeLogger("thatdot.StandingQueryResults")
  private val printLoggerNonBlocking = SafeLogger("thatdot.StandingQueryResultsSampled")

  /** Construct a destination to which results are output
    *
    * @param name        name of the Standing Query Output
    * @param inNamespace the namespace running this standing query
    * @param output      configuration for handling the results
    * @param graph       reference to the graph
    */
  def resultHandlingFlow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph
  )(implicit
    protobufSchemaCache: ProtobufSchemaCache,
    logConfig: LogConfig
  ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed] = {
    val execToken = SqResultsExecToken(s"SQ: $name in: $inNamespace")
    output match {
      case Drop => Flow[StandingQueryResult].map(_ => execToken)
      case iq: InternalQueue =>
        Flow[StandingQueryResult].map { r =>
          iq.results
            .asInstanceOf[AtomicReference[Vector[StandingQueryResult]]] // ugh. gross.
            .getAndUpdate(results => results :+ r)
          execToken
        // TODO: Note that enqueuing a result does not properly respect the spirit of `execToken` in that the work
        //       of processing the result in the queue has not been done before emitting the token. But this
        //       `InternalQueue` is only meant for internal testing.
        }
      case PostToEndpoint(url, parallelism, onlyPositiveMatchData) =>
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
                else result.toJson.noSpaces
              )
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
                                 |Response status was ${Safe(response.status.value)}""".cleanLines
                            withException err
                          )
                        case Success(responseBody) =>
                          logger.error(
                            log"""Failed to POST $result to ${Safe(url)}.
                                 |Response was ${Safe(response.status.value)}
                                 |""".cleanLines + log": ${Safe(responseBody)}"
                          )
                      }(system.dispatcher)
                  }
                )(system.dispatcher)
                .map(_ => execToken)(system.dispatcher)

            // TODO: principled error handling
            posted.recover { case err =>
              logger.error(log"Failed to POST standing query result" withException err)
              execToken
            }(system.dispatcher)
          }

      case WriteToKafka(topic, bootstrapServers, format, properties) =>
        val settings = ProducerSettings(
          graph.system,
          new ByteArraySerializer,
          new ByteArraySerializer
        ).withBootstrapServers(bootstrapServers)
          .withProperties(properties)
        logger.info(log"Writing to kafka with properties ${properties}")
        serialized(name, format, graph)
          .map(bytes => ProducerMessage.single(new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)))
          .via(KafkaProducer.flexiFlow(settings).named(s"sq-output-kafka-producer-for-$name"))
          .map(_ => execToken)

      case WriteToKinesis(
            credentialsOpt,
            regionOpt,
            streamName,
            format,
            kinesisParallelism,
            kinesisMaxBatchSize,
            kinesisMaxRecordsPerSecond,
            kinesisMaxBytesPerSecond
          ) =>
        val builder = KinesisAsyncClient
          .builder()
          .credentials(credentialsOpt)
          .region(regionOpt)
          .httpClient(NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build())
        val kinesisAsyncClient: KinesisAsyncClient =
          builder
            .build()
        graph.system.registerOnTermination(kinesisAsyncClient.close()) // TODO

        val settings = {
          var s = KinesisFlowSettings.create()
          s = kinesisParallelism.foldLeft(s)(_ withParallelism _)
          s = kinesisMaxBatchSize.foldLeft(s)(_ withMaxBatchSize _)
          s = kinesisMaxRecordsPerSecond.foldLeft(s)(_ withMaxRecordsPerSecond _)
          s = kinesisMaxBytesPerSecond.foldLeft(s)(_ withMaxBytesPerSecond _)
          s
        }

        serialized(name, format, graph)
          .map { bytes =>
            val builder = PutRecordsRequestEntry.builder()
            builder.data(SdkBytes.fromByteArray(bytes))
            builder.partitionKey("undefined")
            builder.explicitHashKey(BigInt(128, Random).toString)
            builder.build()
          }
          .via(
            KinesisFlow(
              streamName,
              settings
            )(kinesisAsyncClient).named(s"sq-output-kinesis-producer-for-$name")
          )
          .map(_ => execToken)

      case WriteToSNS(credentialsOpt, regionOpt, topic) =>
        val awsSnsClient = SnsAsyncClient
          .builder()
          .credentials(credentialsOpt)
          .region(regionOpt)
          .httpClient(
            NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()
          )
          .build()

        // NOTE pekko-connectors requires we close the SNS client
        graph.system.registerOnTermination(awsSnsClient.close()) // TODO

        // NB: by default, this will make 10 parallel requests [configurable via parameter to SnsPublisher.flow]
        // TODO if any request to SNS errors, that thread (of the aforementioned 10) will retry its request
        // indefinitely. If all worker threads block, the SnsPublisher.flow will backpressure indefinitely.
        Flow[StandingQueryResult]
          .map(result => result.toJson(graph.idProvider, logConfig).noSpaces + "\n")
          .viaMat(SnsPublisher.flow(topic)(awsSnsClient).named(s"sq-output-sns-producer-for-$name"))(Keep.right)
          .map(_ => execToken)

      case PrintToStandardOut(logLevel, logMode) =>
        import PrintToStandardOut._
        val resultLogger: SafeLogger = logMode match {
          case LogMode.Complete => printLogger
          case LogMode.FastSampling => printLoggerNonBlocking
        }
        val logFn: SafeInterpolator => Unit =
          logLevel match {
            case LogLevel.Trace => resultLogger.trace(_)
            case LogLevel.Debug => resultLogger.debug(_)
            case LogLevel.Info => resultLogger.info(_)
            case LogLevel.Warn => resultLogger.warn(_)
            case LogLevel.Error => resultLogger.error(_)
          }

        Flow[StandingQueryResult].map { result =>
          // NB we are using `Safe` here despite `result` potentially containing PII because the entire purpose of this
          // output is to log SQ results. If the user has configured this output, they have accepted the risk of PII
          // in their logs.
          logFn(log"Standing query `${Safe(name)}` match: ${Safe(result.toJson(graph.idProvider, logConfig).noSpaces)}")
          execToken
        }

      case WriteToFile(path) =>
        Flow[StandingQueryResult]
          .map(result => ByteString(result.toJson(graph.idProvider, logConfig).noSpaces + "\n"))
          .alsoTo(
            FileIO
              .toPath(
                Paths.get(path),
                Set(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
              )
              .named(s"sq-output-file-writer-for-$name")
          )
          .map(_ => execToken)

      case PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds) =>
        implicit val system: ActorSystem = graph.system
        implicit val idProvider: QuineIdProvider = graph.idProvider
        val http = Http()

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
              entity = HttpEntity.apply(contentType = `application/json`, result.slackJson)
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
                          withException err
                        )
                      case Success(responseBody) =>
                        logger.error(
                          log"Failed to POST ${result.slackJson} to slack webhook. " +
                          log"Response status was ${Safe(response.status.value)}: " +
                          log"${Safe(responseBody)}"
                        )
                    }(system.dispatcher)
                }
              }(system.dispatcher)
              .map(_ => execToken)(system.dispatcher)

            // TODO: principled error handling
            posted.recover { case err =>
              logger.error(log"Failed to POST standing query result" withException err)
              execToken
            }(system.dispatcher)
          }

      case CypherQuery(query, parameter, parallelism, andThen, allowAllNodeScan, shouldRetry) =>
        val compiledQuery @ cypher.CompiledQuery(_, queryAst, _, _, _) = compiler.cypher.compile(
          query,
          unfixedParameters = Seq(parameter)
        )

        // TODO: When in the initial set of SQ outputs, these should be tested before the SQ is registered!
        if (queryAst.canContainAllNodeScan && !allowAllNodeScan) {
          throw new RuntimeException(
            "Cypher query may contain full node scan; re-write without possible full node scan, or pass allowAllNodeScan true. " +
            s"The provided query was: $query"
          )
        }
        if (!queryAst.isIdempotent && shouldRetry) {
          logger.warn(
            log"""Could not verify that the provided Cypher query is idempotent. If timeouts or external system errors
                 |occur, query execution may be retried and duplicate data may be created. To avoid this
                 |set shouldRetry = false in the Standing Query output""".cleanLines
          )
        }

        val andThenFlow: Flow[(StandingQueryResult.Meta, cypher.QueryContext), SqResultsExecToken, NotUsed] =
          (andThen match {
            case None =>
              wireTapFirst[(StandingQueryResult.Meta, cypher.QueryContext)](tup =>
                logger.warn(
                  safe"""Unused Cypher Standing Query output for Standing Query output:
                        |${Safe(name)} with: ${Safe(tup._2.environment.size)} columns.
                        |Did you mean to specify `andThen`?""".cleanLines
                )
              ).map(_ => execToken)

            case Some(thenOutput) =>
              Flow[(StandingQueryResult.Meta, cypher.QueryContext)]
                .map { case (meta: StandingQueryResult.Meta, qc: cypher.QueryContext) =>
                  val newData = qc.environment.map { case (keySym, cypherVal) =>
                    keySym.name -> Try(cypher.Expr.toQuineValue(cypherVal)).getOrElse {
                      logger.warn(
                        log"""Cypher Value: ${cypherVal.toString} could not be represented as a Quine value in Standing
                             |Query output: ${Safe(name)}. Using `null` instead.""".cleanLines
                      )
                      QuineValue.Null
                    }
                  }
                  StandingQueryResult(meta, newData)
                }
                .via(resultHandlingFlow(name, inNamespace, thenOutput, graph))
          }).named(s"sq-output-andthen-for-$name")

        lazy val atLeastOnceCypherQuery =
          AtLeastOnceCypherQuery(compiledQuery, parameter, s"sq-output-action-query-for-$name")

        Flow[StandingQueryResult]
          .flatMapMerge(
            breadth = parallelism,
            result => {
              val value: cypher.Value = cypher.Expr.fromQuineValue(result.toQuineValueMap)

              val cypherResultRows =
                if (shouldRetry) atLeastOnceCypherQuery.stream(value, inNamespace)(graph)
                else
                  graph.cypherOps
                    .query(
                      compiledQuery,
                      namespace = inNamespace,
                      atTime = None,
                      parameters = Map(parameter -> value)
                    )
                    .results

              cypherResultRows
                .map { resultRow =>
                  QueryContext(compiledQuery.columns.zip(resultRow).toMap)
                }
                .map(data => (result.meta, data))
            }
          )
          .via(andThenFlow)
    }
  }.named(s"sq-output-$name")

  private def serialized(
    name: String,
    format: OutputFormat,
    graph: BaseGraph
  )(implicit
    protobufSchemaCache: ProtobufSchemaCache,
    logConfig: LogConfig
  ): Flow[StandingQueryResult, Array[Byte], NotUsed] =
    format match {
      case OutputFormat.JSON =>
        Flow[StandingQueryResult].map(_.toJson(graph.idProvider, logConfig).noSpaces.getBytes)
      case OutputFormat.Protobuf(schemaUrl, typeName) =>
        val serializer: Future[QuineValueToProtobuf] =
          protobufSchemaCache
            .getMessageDescriptor(filenameOrUrl(schemaUrl), typeName, flushOnFail = true)
            .map(new QuineValueToProtobuf(_))(
              graph.materializer.executionContext // this is effectively part of stream materialization
            )
        val serializerRepeated: Source[QuineValueToProtobuf, Future[NotUsed]] = Source.futureSource(
          serializer
            .map(Source.repeat[QuineValueToProtobuf])(graph.materializer.executionContext)
        )
        Flow[StandingQueryResult]
          .filter(_.meta.isPositiveMatch)
          .zip(serializerRepeated)
          .map { case (result, serializer) =>
            serializer
              .toProtobufBytes(result.data)
              .leftMap { (err: ConversionFailure) =>
                logger.warn(
                  log"""On Standing Query output: ${Safe(name)}, can't serialize provided datum: $result
                       |to protobuf type: ${Safe(typeName)}. Skipping datum. Error: ${err.toString}
                       |""".cleanLines
                )
              }
          }
          .collect { case Right(value) => value }
    }

  sealed abstract class SlackSerializable {
    def slackJson: String
    implicit def stringToJson(s: String): Json = Json.fromString(s)

  }

  object SlackSerializable {
    def apply(positiveOnly: Boolean, results: Seq[StandingQueryResult])(implicit
      idProvider: QuineIdProvider,
      logConfig: LogConfig
    ): Option[SlackSerializable] = results match {
      case Seq() => None // no new results or cancellations
      case cancellations if positiveOnly && !cancellations.exists(_.meta.isPositiveMatch) =>
        None // no new results, only cancellations, and we're configured to drop cancellations
      case Seq(result) => // one new result or cancellations
        if (result.meta.isPositiveMatch) Some(NewResult(result.data))
        else if (!positiveOnly) Some(CancelledResult(result.data))
        else None
      case _ => // multiple results (but maybe not all valid given `positiveOnly`)
        val (positiveResults, cancellations) = results.partition(_.meta.isPositiveMatch)

        if (positiveOnly && positiveResults.length == 1) {
          val singleResult = positiveResults.head
          Some(NewResult(singleResult.data))
        } else if (!positiveOnly && positiveResults.isEmpty && cancellations.length == 1) {
          Some(CancelledResult(cancellations.head.data))
        } else if (positiveOnly && positiveResults.nonEmpty) {
          Some(MultipleUpdates(positiveResults, Seq.empty))
        } else if (positiveResults.nonEmpty || cancellations.nonEmpty) {
          Some(MultipleUpdates(positiveResults, cancellations))
        } else None
    }
  }

  final case class NewResult(data: Map[String, QuineValue])(implicit idProvider: QuineIdProvider, logConfig: LogConfig)
      extends SlackSerializable {
    // pretty-printed JSON representing `data`. Note that since this is used as a value in another JSON object, it
    // may not be perfectly escaped (for example, if the data contains a triple-backquote)
    private val dataPrettyJson: String =
      Json.fromFields(data.view.map { case (k, v) => (k, QuineValue.toJson(v)) }.toSeq).spaces2

    def slackBlock: Json =
      Json.obj("type" -> "section", "text" -> Json.obj("type" -> "mrkdwn", "text" -> s"```$dataPrettyJson```"))

    override def slackJson: String = Json
      .obj(
        "text" -> "New Standing Query Result",
        "blocks" -> Json.arr(
          Json
            .obj("type" -> "header", "text" -> Json.obj("type" -> "plain_text", "text" -> "New Standing Query Result"))
        )
      )
      .noSpaces
  }

  final case class CancelledResult(data: Map[String, QuineValue])(implicit
    idProvider: QuineIdProvider,
    protected val logConfig: LogConfig
  ) extends SlackSerializable {
    // pretty-printed JSON representing `data`. Note that since this is used as a value in another JSON object, it
    // may not be perfectly escaped (for example, if the data contains a triple-backquote)
    private val dataPrettyJson: String =
      Json.fromFields(data.view.map { case (k, v) => (k, QuineValue.toJson(v)) }.toSeq).spaces2

    def slackBlock: Json =
      Json.obj("type" -> "section", "text" -> Json.obj("type" -> "mrkdwn", "text" -> s"```$dataPrettyJson```"))

    override def slackJson: String = Json
      .obj(
        "text" -> "Standing Query Result Cancelled",
        "blocks" ->

        Json.arr(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "Standing Query Result Cancelled"
            )
          ),
          slackBlock
        )
      )
      .noSpaces
  }

  final case class MultipleUpdates(newResults: Seq[StandingQueryResult], newCancellations: Seq[StandingQueryResult])(
    implicit
    idProvider: QuineIdProvider,
    logConfig: LogConfig
  ) extends SlackSerializable {
    val newResultsBlocks: Vector[Json] = newResults match {
      case Seq() => Vector.empty
      case Seq(result) =>
        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "New Standing Query Result"
            )
          ),
          NewResult(result.data).slackBlock
        )
      case result +: remainingResults =>
        val excessMetaData = remainingResults.map(_.meta.toString) // TODO: what here since no result ID?
        val excessResultItems: Seq[String] =
          if (excessMetaData.length <= 10) excessMetaData
          else excessMetaData.take(9) :+ s"(${excessMetaData.length - 9} more)"

        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "New Standing Query Results"
            )
          ),
          Json.obj(
            "type" -> "section",
            "text" -> Json.obj(
              "type" -> "mrkdwn",
              "text" -> "Latest result:"
            )
          )
        ) ++ (NewResult(result.data).slackBlock +: Vector(
          Json.obj(
            "type" -> "section",
            "text" -> Json.obj(
              "type" -> "mrkdwn",
              "text" -> "*Other New Result Meta Data:*"
            )
          ),
          Json.obj(
            "type" -> "section",
            "fields" -> Json.fromValues(excessResultItems.map { str =>
              Json.obj(
                "type" -> "mrkdwn",
                "text" -> str
              )
            })
          )
        ))
      case _ => throw new Exception(s"Unexpected value $newResults")
    }

    val cancellationBlocks: Vector[Json] = newCancellations match {
      case Seq() => Vector.empty
      case Seq(cancellation) =>
        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "Standing Query Result Cancelled"
            )
          ),
          CancelledResult(cancellation.data).slackBlock
        )
      case cancellations =>
        val cancelledMetaData = cancellations.map(_.meta.toString)
        val itemsCancelled: Seq[String] =
          if (cancellations.length <= 10) cancelledMetaData
          else cancelledMetaData.take(9) :+ s"(${cancellations.length - 9} more)"

        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "Standing Query Results Cancelled"
            )
          ),
          Json.obj(
            "type" -> "section",
            "fields" -> Json.fromValues(itemsCancelled.map { str =>
              Json.obj(
                "type" -> "mrkdwn",
                "text" -> str
              )
            })
          )
        )
    }
    override def slackJson: String = Json
      .obj(
        "text" -> "New Standing Query Updates",
        "blocks" -> Json.fromValues(newResultsBlocks ++ cancellationBlocks)
      )
      .noSpaces
  }

}

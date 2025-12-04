package com.thatdot.quine.app

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future
import scala.language.implicitConversions

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}

import cats.syntax.either._
import io.circe.Json

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.model.outputs.{
  ConsoleLoggingOutput,
  CypherQueryOutput,
  DropOutput,
  FileOutput,
  KafkaOutput,
  KinesisOutput,
  PostToEndpointOutput,
  QuinePatternOutput,
  SlackOutput,
  SnsOutput,
}
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiV2Standing}
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.{
  BaseGraph,
  CypherOpsGraph,
  NamespaceId,
  StandingQueryResult,
  StandingQueryResultStructure,
}
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}
import com.thatdot.quine.routes.{OutputFormat, StandingQueryResultOutputUserDef}
import com.thatdot.quine.serialization.{ConversionFailure, ProtobufSchemaCache, QuineValueToProtobuf}
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.StringInput.filenameOrUrl
import com.thatdot.quine.{routes => RoutesV1}
object StandingQueryResultOutput extends LazySafeLogging {

  import StandingQueryResultOutputUserDef._

  sealed trait OutputTarget
  object OutputTarget {
    case class V1(definition: RoutesV1.StandingQueryResultOutputUserDef, killSwitch: UniqueKillSwitch)
        extends OutputTarget
    case class V2(definition: ApiV2Standing.StandingQueryResultWorkflow, killSwitch: UniqueKillSwitch)
        extends OutputTarget
  }

  private def resultHandlingFlow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  )(implicit
    protobufSchemaCache: ProtobufSchemaCache,
    logConfig: LogConfig,
  ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed] = {
    val execToken = SqResultsExecToken(s"SQ: $name in: $inNamespace")
    output match {
      case Drop => DropOutput.flow(name, inNamespace, output, graph)
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
      case webhookConfig: PostToEndpoint =>
        new PostToEndpointOutput(webhookConfig).flow(name, inNamespace, output, graph)

      case kafkaSettings: WriteToKafka =>
        new KafkaOutput(kafkaSettings).flow(name, inNamespace, output, graph)

      case kinesisSettings: WriteToKinesis =>
        new KinesisOutput(kinesisSettings).flow(name, inNamespace, output, graph)

      case snsSettings: WriteToSNS =>
        new SnsOutput(snsSettings).flow(name, inNamespace, output, graph)

      case loggingConfig: PrintToStandardOut =>
        new ConsoleLoggingOutput(loggingConfig).flow(name, inNamespace, output, graph)

      case fileConfig: WriteToFile =>
        new FileOutput(fileConfig).flow(name, inNamespace, output, graph)

      case slackSettings: PostToSlack =>
        new SlackOutput(slackSettings).flow(name, inNamespace, output, graph)

      case query: CypherQuery =>
        // Closures can't have implicit arguments in scala 2.13, so flatten the arguments list
        def createRecursiveOutput(
          name: String,
          inNamespace: NamespaceId,
          output: StandingQueryResultOutputUserDef,
          graph: CypherOpsGraph,
          protobufSchemaCache: ProtobufSchemaCache,
          logConfig: LogConfig,
        ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed] =
          resultHandlingFlow(name, inNamespace, output, graph)(protobufSchemaCache, logConfig)

        new CypherQueryOutput(query, createRecursiveOutput).flow(name, inNamespace, output, graph)
      case pattern: QuinePatternQuery =>
        def createRecursiveOutput(
          name: String,
          inNamespace: NamespaceId,
          output: StandingQueryResultOutputUserDef,
          graph: CypherOpsGraph,
          protobufSchemaCache: ProtobufSchemaCache,
          logConfig: LogConfig,
        ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed] =
          resultHandlingFlow(name, inNamespace, output, graph)(protobufSchemaCache, logConfig)

        new QuinePatternOutput(pattern, createRecursiveOutput).flow(name, inNamespace, output, graph)
    }
  }.named(s"sq-output-$name")

  /** Construct a destination to which results are output. Results will flow through one or more
    * chained [[resultHandlingFlow]]s before emitting a completion token to the master stream
    *
    * @param name        name of the Standing Query Output
    * @param inNamespace the namespace running this standing query
    * @param output      configuration for handling the results
    * @param graph       reference to the graph
    */
  def resultHandlingSink(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  )(implicit
    protobufSchemaCache: ProtobufSchemaCache,
    logConfig: LogConfig,
  ): Sink[StandingQueryResult, UniqueKillSwitch] =
    Flow[StandingQueryResult]
      .viaMat(KillSwitches.single)(Keep.right)
      .via(resultHandlingFlow(name, inNamespace, output, graph))
      .to(graph.masterStream.standingOutputsCompletionSink)

  def serialized(
    name: String,
    format: OutputFormat,
    graph: BaseGraph,
    structure: StandingQueryResultStructure,
  )(implicit
    protobufSchemaCache: ProtobufSchemaCache,
    logConfig: LogConfig,
  ): Flow[StandingQueryResult, Array[Byte], NotUsed] =
    format match {
      case OutputFormat.JSON =>
        Flow[StandingQueryResult].map(_.toJson(structure)(graph.idProvider, logConfig).noSpaces.getBytes)
      case OutputFormat.Protobuf(schemaUrl, typeName) =>
        val serializer: Future[QuineValueToProtobuf] =
          protobufSchemaCache
            .getMessageDescriptor(filenameOrUrl(schemaUrl), typeName, flushOnFail = true)
            .map(new QuineValueToProtobuf(_))(
              graph.materializer.executionContext, // this is effectively part of stream materialization
            )
        val serializerRepeated: Source[QuineValueToProtobuf, Future[NotUsed]] = Source.futureSource(
          serializer
            .map(Source.repeat[QuineValueToProtobuf])(graph.materializer.executionContext),
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
                       |""".cleanLines,
                )
              }
          }
          .collect { case Right(value) => value }
    }

  sealed abstract class SlackSerializable {
    def slackJson: String
  }

  object SlackSerializable {
    implicit def stringToJson(s: String): Json = Json.fromString(s)

    def jsonFromQuineValueMap(
      map: Map[String, QuineValue],
    )(implicit logConfig: LogConfig, idProvider: QuineIdProvider): Json =
      Json.fromFields(map.view.map { case (k, v) => (k, QuineValue.toJson(v)) }.toSeq)

    def apply(positiveOnly: Boolean, results: Seq[StandingQueryResult])(implicit
      idProvider: QuineIdProvider,
      logConfig: LogConfig,
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

    private def isInferredCancellation(json: Json): Boolean =
      (json \\ "meta").exists(meta => (meta \\ "isPositiveMatch").contains(Json.False))

    /** @param results TODO document shape of...may contain meta, may not...
      * @return
      */
    def apply(results: Seq[Json]): Option[SlackSerializable] = results.partition(isInferredCancellation) match {
      case (Nil, Nil) => None
      case (singlePositive :: Nil, Nil) => Some(NewResult(singlePositive))
      case (Nil, singleCancellation :: Nil) => Some(CancelledResult(singleCancellation))
      case (positiveResults, cancellations) => Some(MultipleUpdates(positiveResults, cancellations))
    }
  }

  final private case class NewResult(data: Json) extends SlackSerializable {

    import SlackSerializable.stringToJson

    def slackBlock: Json = {
      // May not be perfectly escaped (for example, if the data contains a triple-backquote)
      val codeBlockContent = data.spaces2
      Json.obj("type" -> "section", "text" -> Json.obj("type" -> "mrkdwn", "text" -> s"```$codeBlockContent```"))
    }

    override def slackJson: String = Json
      .obj(
        "text" -> "New Standing Query Result",
        "blocks" -> Json.arr(
          NewResult.header,
          slackBlock,
        ),
      )
      .noSpaces

  }

  private object NewResult {

    import SlackSerializable._

    def apply(data: Map[String, QuineValue])(implicit logConfig: LogConfig, idProvider: QuineIdProvider): NewResult =
      NewResult(jsonFromQuineValueMap(data))

    val header: Json = Json.obj(
      "type" -> "header",
      "text" -> Json.obj(
        "type" -> "plain_text",
        "text" -> "New Standing Query Result",
      ),
    )
  }

  final private case class CancelledResult(data: Json) extends SlackSerializable {

    import SlackSerializable.stringToJson

    def slackBlock: Json = {
      // May not be perfectly escaped (for example, if the data contains a triple-backquote)
      val codeBlockContent = data.spaces2
      Json.obj("type" -> "section", "text" -> Json.obj("type" -> "mrkdwn", "text" -> s"```$codeBlockContent```"))
    }

    override def slackJson: String = Json
      .obj(
        "text" -> "Standing Query Result Cancelled",
        "blocks" -> Json.arr(
          CancelledResult.header,
          slackBlock,
        ),
      )
      .noSpaces
  }

  private object CancelledResult {

    import SlackSerializable._

    def apply(
      data: Map[String, QuineValue],
    )(implicit logConfig: LogConfig, idProvider: QuineIdProvider): CancelledResult = CancelledResult(
      jsonFromQuineValueMap(data),
    )

    val header: Json = Json.obj(
      "type" -> "header",
      "text" -> Json.obj(
        "type" -> "plain_text",
        "text" -> "Standing Query Result Cancelled",
      ),
    )
  }

  final private case class MultipleUpdates(
    newResults: Seq[Json],
    newCancellations: Seq[Json],
  ) extends SlackSerializable {

    import SlackSerializable._

    private val newResultsBlocks: Vector[Json] = newResults match {
      case Seq() =>
        Vector.empty
      case Seq(jData) =>
        Vector(
          NewResult.header,
          NewResult(jData).slackBlock,
        )
      case result +: remainingResults =>
        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "New Standing Query Results",
            ),
          ),
          Json.obj(
            "type" -> "section",
            "text" -> Json.obj(
              "type" -> "mrkdwn",
              // Note: "Latest" is a side effect of presumed list-prepending at batching call site
              "text" -> s"Latest result of ${remainingResults.size}:",
            ),
          ),
        ) :+ (NewResult(result).slackBlock)
      case _ => throw new Exception(s"Unexpected value $newResults")
    }

    private val cancellationBlocks: Vector[Json] = newCancellations match {
      case Seq() => Vector.empty
      case Seq(jData) =>
        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> "Standing Query Result Cancelled",
            ),
          ),
          CancelledResult(jData).slackBlock,
        )
      case cancellations =>
        Vector(
          Json.obj(
            "type" -> "header",
            "text" -> Json.obj(
              "type" -> "plain_text",
              "text" -> s"Standing Query Results Cancelled: ${cancellations.size}",
            ),
          ),
        )
    }

    override def slackJson: String = Json
      .obj(
        "text" -> "New Standing Query Updates",
        "blocks" -> Json.fromValues(newResultsBlocks ++ cancellationBlocks),
      )
      .noSpaces
  }

  private object MultipleUpdates {
    import SlackSerializable.jsonFromQuineValueMap

    def apply(
      newResults: Seq[StandingQueryResult],
      newCancellations: Seq[StandingQueryResult],
    )(implicit logConfig: LogConfig, idProvider: QuineIdProvider): MultipleUpdates =
      MultipleUpdates(
        newResults = newResults.map(jsonFromStandingQueryResult),
        newCancellations = newCancellations.map(jsonFromStandingQueryResult),
      )

    private def jsonFromStandingQueryResult(
      result: StandingQueryResult,
    )(implicit logConfig: LogConfig, idProvider: QuineIdProvider): Json = jsonFromQuineValueMap(result.data)

  }
}

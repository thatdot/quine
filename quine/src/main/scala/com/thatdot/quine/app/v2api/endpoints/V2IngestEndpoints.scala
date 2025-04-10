package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, EndpointInput, Schema, path, statusCode}

import com.thatdot.quine.app.v2api.definitions.{
  ApiIngest,
  CreateIngestApiCmd,
  CustomError,
  ErrorEnvelope,
  IngestStatusApiCmd,
  ObjectEnvelope,
  PauseIngestApiCmd,
  UnpauseIngestApiCmd,
  V2QuineEndpointDefinitions,
}
import com.thatdot.quine.graph.NamespaceId

trait V2IngestEndpoints extends V2QuineEndpointDefinitions {

  import com.thatdot.quine.app.v2api.definitions.ApiToIngest.OssConversions._

  private val ingestStreamNameElement: EndpointInput.PathCapture[String] =
    path[String]("name").description("Ingest stream name").default("NumbersStream")

  private def ingestEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): Endpoint[Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T], Any] =
    baseEndpoint[T]("ingests")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")

  private val ingestExample = ApiIngest.Oss.QuineIngestConfiguration(
    ApiIngest.NumberIteratorIngest(0, None),
    query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that ",
    onRecordError = ApiIngest.LogRecordErrorHandler,
    onStreamError = ApiIngest.LogStreamError,
    maxPerSecond = Some(100),
  )

  private val createIngestEndpoint = ingestEndpoint[Boolean]
    .name("Create Ingest Stream")
    .description("""Create an [ingest stream](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
                   |that connects a streaming event source to Quine and loads data into the graph.
                   |
                   |An ingest stream is defined by selecting a source `type`, then an appropriate data `format`,
                   |and must be created with a unique name. Many ingest stream types allow a Cypher query to operate
                   |on the event stream data to create nodes and relationships in the graph.""".stripMargin)
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[ApiIngest.Oss.QuineIngestConfiguration](Some(ingestExample)))
    .post
    .out(statusCode(StatusCode.Created))
    .serverLogic { case (memberIdx, ingestStreamName, ns, ingestStreamConfig) =>
      runServerLogicFromEither[(String, ApiIngest.Oss.QuineIngestConfiguration, NamespaceId), Boolean](
        CreateIngestApiCmd,
        memberIdx,
        (ingestStreamName, ingestStreamConfig, namespaceFromParam(ns)),
        t => Future.successful(appMethods.createIngestStream(t._1, t._2, t._3)),
      )
    }

  private val pauseIngestEndpoint = ingestEndpoint[Option[ApiIngest.IngestStreamInfoWithName]]
    .name("Pause Ingest Stream")
    .description("Temporarily pause processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("pause")
    .in(namespaceParameter)
    .post
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogicFromEither[(String, NamespaceId), Option[ApiIngest.IngestStreamInfoWithName]](
        PauseIngestApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.pauseIngestStream(t._1, t._2),
      )
    }

  private val unpauseIngestEndpoint = ingestEndpoint[Option[ApiIngest.IngestStreamInfoWithName]]
    .name("Unpause Ingest Stream")
    .description("Resume processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("start")
    .in(namespaceParameter)
    .post
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogicFromEither[(String, NamespaceId), Option[ApiIngest.IngestStreamInfoWithName]](
        UnpauseIngestApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.unpauseIngestStream(t._1, t._2),
      )
    }

  private val deleteIngestEndpoint = ingestEndpoint[Option[ApiIngest.IngestStreamInfoWithName]]
    .name("Delete Ingest Stream")
    .description("""Immediately halt and remove the named ingest stream from Quine.
                    |
                    |The ingest stream will complete any pending operations and return stream information
                    |once the operation is complete.""".stripMargin)
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .delete
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogic[(String, NamespaceId), Option[ApiIngest.IngestStreamInfoWithName]](
        CreateIngestApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.deleteIngestStream(t._1, t._2),
      )
    }

  private val ingestStatusEndpoint = ingestEndpoint[Option[ApiIngest.IngestStreamInfoWithName]]
    .name("Ingest Stream Status")
    .description("Return the ingest stream status information for a configured ingest stream by name.")
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .get
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogic[(String, NamespaceId), Option[ApiIngest.IngestStreamInfoWithName]](
        IngestStatusApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.ingestStreamStatus(t._1, t._2),
      )
    }

  private val listIngestEndpoint = ingestEndpoint[Map[String, ApiIngest.IngestStreamInfo]]
    .name("List Ingest Streams")
    .description(
      """Return a JSON object containing the configured [ingest streams](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
        |and their associated stream metrics keyed by the stream name. """.stripMargin,
    )
    .in(namespaceParameter)
    .get
    .serverLogic { case (memberIdx, ns) =>
      runServerLogic[NamespaceId, Map[String, ApiIngest.IngestStreamInfo]](
        IngestStatusApiCmd,
        memberIdx,
        namespaceFromParam(ns),
        ns => appMethods.listIngestStreams(ns),
      )
    }

  val ingestEndpoints: List[ServerEndpoint[Any, Future]] = List(
    createIngestEndpoint,
    pauseIngestEndpoint,
    unpauseIngestEndpoint,
    deleteIngestEndpoint,
    ingestStatusEndpoint,
    listIngestEndpoint,
  )

}

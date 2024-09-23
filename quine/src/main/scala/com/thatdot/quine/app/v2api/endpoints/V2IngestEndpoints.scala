package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.auto._
import io.circe.{Decoder, Encoder}
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, EndpointInput, Schema, path}

import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.{IngestConfiguration => V2IngestConfiguration}
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.routes.{IngestStreamInfo, IngestStreamInfoWithName}
trait V2IngestEndpoints extends V2QuineEndpointDefinitions with V2IngestSchemas {

  private val ingestStreamNameElement: EndpointInput.PathCapture[String] =
    path[String]("name").description("Ingest stream name")

  private def ingestEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): Endpoint[Unit, Option[Int], ErrorEnvelope[_ <: CustomError], ObjectEnvelope[T], Any] =
    baseEndpoint[T]("ingest")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")

  private val createIngestEndpoint = ingestEndpoint[Unit]
    .name("Create Ingest Stream")
    .description("""Create an [ingest stream](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
                   |that connects a streaming event source to Quine and loads data into the graph.
                   |
                   |An ingest stream is defined by selecting a source `type`, then an appropriate data `format`,
                   |and must be created with a unique name. Many ingest stream types allow a Cypher query to operate
                   |on the event stream data to create nodes and relationships in the graph.""".stripMargin)
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[V2IngestConfiguration])
    .post
    .serverLogic { case (memberIdx, ingestStreamName, ns, ingestStreamConfig) =>
      runServerLogicWithError[(String, V2IngestConfiguration, NamespaceId), Unit](
        CreateIngestApiCmd,
        memberIdx,
        (ingestStreamName, ingestStreamConfig, namespaceFromParam(ns)),
        t => Future.successful(appMethods.createIngestStream(t._1, t._2, t._3)),
      )
    }

  private val pauseIngestEndpoint = ingestEndpoint[Option[IngestStreamInfoWithName]]
    .name("Pause Ingest Stream")
    .description("Temporarily pause processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("pause")
    .in(namespaceParameter)
    .put
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogicWithError[(String, NamespaceId), Option[IngestStreamInfoWithName]](
        PauseIngestApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.pauseIngestStream(t._1, t._2),
      )
    }

  private val unpauseIngestEndpoint = ingestEndpoint[Option[IngestStreamInfoWithName]]
    .name("Unpause Ingest Stream")
    .description("Resume processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("start")
    .in(namespaceParameter)
    .put
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogicWithError[(String, NamespaceId), Option[IngestStreamInfoWithName]](
        UnpauseIngestApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.unpauseIngestStream(t._1, t._2),
      )
    }

  private val deleteIngestEndpoint = ingestEndpoint[Option[IngestStreamInfoWithName]]
    .name("Delete Ingest Stream")
    .description("""Immediately halt and remove the named ingest stream from Quine.
                    |
                    |The ingest stream will complete any pending operations and return stream information
                    |once the operation is complete.""".stripMargin)
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .delete
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogic[(String, NamespaceId), Option[IngestStreamInfoWithName]](
        CreateIngestApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.deleteIngestStream(t._1, t._2),
      )
    }

  private val ingestStatusEndpoint = ingestEndpoint[Option[IngestStreamInfoWithName]]
    .name("Ingest Stream Status")
    .description("Return the ingest stream status information for a configured ingest stream by name.")
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .get
    .serverLogic { case (memberIdx, ingestStreamName, ns) =>
      runServerLogic[(String, NamespaceId), Option[IngestStreamInfoWithName]](
        IngestStatusApiCmd,
        memberIdx,
        (ingestStreamName, namespaceFromParam(ns)),
        t => appMethods.ingestStreamStatus(t._1, t._2),
      )
    }

  private val listIngestEndpoint = ingestEndpoint[Map[String, IngestStreamInfo]]
    .name("List Ingest Streams")
    .description(
      """Return a JSON object containing the configured [ingest streams](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
        |and their associated stream metrics keyed by the stream name. """.stripMargin,
    )
    .in(namespaceParameter)
    .get
    .serverLogic { case (memberIdx, ns) =>
      runServerLogic[NamespaceId, Map[String, IngestStreamInfo]](
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

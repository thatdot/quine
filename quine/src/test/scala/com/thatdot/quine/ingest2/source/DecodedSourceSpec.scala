package com.thatdot.quine.ingest2.source

import java.io.{BufferedOutputStream, File, FileOutputStream}

import scala.annotation.nowarn
import scala.util.Using

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.IngestTestGraph
import com.thatdot.quine.app.ingest2.source.{DecodedSource, QuineValueIngestQuery}
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.compiler.{cypher => cyComp}
import com.thatdot.quine.graph.cypher.RunningCypherQuery
import com.thatdot.quine.graph.{GraphService, MasterStream, cypher}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log.LogConfig

class DecodedSourceSpec extends AsyncFunSpec with Matchers with LazyLogging {

  implicit val logConfig: LogConfig = LogConfig.testing
  @nowarn implicit val protobufSchemaCache: ProtobufSchemaCache.Blocking.type = ProtobufSchemaCache.Blocking

  def fileFromString(s: String): File = {
    val tempFile = File.createTempFile(s"IngestStreamConfigurationToSource${System.currentTimeMillis()}", ".jsonl")
    Using(new BufferedOutputStream(new FileOutputStream(tempFile))) { bos =>
      bos.write(s.getBytes)
      bos.flush()
    }
    tempFile
  }

  describe("IngestStreamConfigurationToSource") {
    // Ignore until the awkward Thread.sleep is removed.
    it("runs one supported configuration") {
      val graph: GraphService = IngestTestGraph.makeGraph()
      val rawJson = 1.to(5).map(i => s"""{ "foo":$i }""").mkString("\n")
      val tempFile = fileFromString(rawJson)
      val fileIngest = FileIngest(
        format = FileIngestFormat.CypherJson("CREATE ($that)"),
        path = tempFile.getAbsolutePath,
        ingestLimit = None,
        maximumPerSecond = None,
        fileIngestMode = None,
      )

      val decodedSource = DecodedSource.apply("test", fileIngest, IngestMetered.ingestMeter(None, "test"), graph.system)
      val ingestQuery = QuineValueIngestQuery.build(graph, "CREATE ($that)", "that", None).get

      val ingestSource = decodedSource.toQuineIngestSource("test", ingestQuery, graph)
      val ingestStream: Source[MasterStream.IngestSrcExecToken, NotUsed] = ingestSource.stream(None, _ => ())
      ingestStream.runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)
      Thread.sleep(1000)

      val queryFuture: RunningCypherQuery = cyComp.queryCypherValues("match (n) return count(n.foo)", None)(graph)
      IngestTestGraph.collect(queryFuture.results)(graph.materializer).head shouldEqual Vector(cypher.Expr.Integer(5L))
    }
  }
}

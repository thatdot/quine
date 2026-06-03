package com.thatdot.quine.ingest2.sources

import java.io.File
import java.nio.file.Files

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import com.github.mjakubowski84.parquet4s.{
  BinaryValue,
  IntValue,
  ParquetWriter,
  Path => Parquet4sPath,
  RowParquetRecord,
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32}
import org.apache.parquet.schema.{MessageType, Types}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.model.ingest2.FileFormat
import com.thatdot.quine.app.model.ingest2.codec.{BufferedSeekableInput, FileSeekableInput, ParquetDecoder}
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.model.ingest2.sources.FileSource
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.exceptions.IngestSourceFormatException
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics

class ParquetIngestSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("ParquetIngestSpec")
  implicit val materializer: Materializer = Materializer(actorSystem)

  override def afterAll(): Unit = {
    val _ = actorSystem.terminate()
  }

  private def newMeter() =
    IngestMetered.ingestMeter(
      defaultNamespaceId,
      java.util.UUID.randomUUID().toString,
      HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
    )

  private val schema: MessageType =
    Types
      .buildMessage()
      .addField(Types.required(INT32).named("id"))
      .addField(Types.required(BINARY).as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("name"))
      .named("TestRecord")

  private def writeParquetFile(rows: Int): File = {
    val tmp = Files.createTempFile("parquet-ingest-test-", ".parquet").toFile
    tmp.delete() // ParquetWriter wants the file not to exist yet
    val writer = ParquetWriter.generic(schema).build(Parquet4sPath(tmp.getAbsolutePath))
    try (1 to rows).foreach { i =>
      val rec = RowParquetRecord(
        Seq(
          "id" -> IntValue(i),
          "name" -> BinaryValue(s"row_$i".getBytes("UTF-8")),
        ),
      )
      writer.write(rec)
    } finally writer.close()
    tmp
  }

  private def collectDecoded(ds: DecodedSource): List[scala.util.Try[Any]] = {
    val fut = ds.stream
      .map { case (decoded, _) => decoded() }
      .runWith(Sink.seq)
    Await.result(fut, 30.seconds).toList
  }

  describe("ParquetDecoder via FileSeekableInput") {
    it("reads all rows from a local parquet file") {
      val file = writeParquetFile(5)
      val meter = newMeter()
      val ds = FileSource.decodedSourceFromSeekableInput(
        new ParquetDecoder,
        new FileSeekableInput(file),
        IngestBounds(),
        meter,
      )
      val results = collectDecoded(ds)
      results.length shouldEqual 5
      results.foreach(_.isSuccess shouldEqual true)
      meter.counts.getCount shouldEqual 5
      meter.bytes.getCount shouldEqual file.length()
      file.delete()
    }

    it("respects ingest bounds") {
      val file = writeParquetFile(20)
      val meter = newMeter()
      val ds = FileSource.decodedSourceFromSeekableInput(
        new ParquetDecoder,
        new FileSeekableInput(file),
        IngestBounds(startAtOffset = 5, ingestLimit = Some(3)),
        meter,
      )
      val results = collectDecoded(ds)
      results.length shouldEqual 3
      meter.counts.getCount shouldEqual 3
      file.delete()
    }
  }

  describe("ParquetDecoder via BufferedSeekableInput") {
    it("reads all rows from in-memory parquet bytes") {
      val file = writeParquetFile(7)
      val bytes = Files.readAllBytes(file.toPath)
      file.delete()
      val meter = newMeter()
      // Simulating the S3-style path: byte source → buffer → seekable → decode
      import com.thatdot.quine.serialization.AvroSchemaCache
      import scala.concurrent.Future
      implicit val avroCache: AvroSchemaCache =
        (_: java.net.URL) => Future.failed(new UnsupportedOperationException("not used"))
      val ds = FileSource
        .decodedSourceFromFileStream(
          fileSource = Source.single(ByteString(bytes)),
          format = FileFormat.ParquetFormat,
          charset = java.nio.charset.StandardCharsets.UTF_8,
          maximumLineSize = 1000000,
          bounds = IngestBounds(),
          meter = meter,
        )
        .toOption
        .get
      val results = collectDecoded(ds)
      results.length shouldEqual 7
      meter.counts.getCount shouldEqual 7
    }
  }

  describe("Refusals") {
    it("websocket decodingFoldableFrom refuses Parquet") {
      import com.thatdot.quine.serialization.AvroSchemaCache
      import scala.concurrent.Future
      implicit val avroCache: AvroSchemaCache =
        (_: java.net.URL) => Future.failed(new UnsupportedOperationException("not used"))
      val thrown = intercept[IngestSourceFormatException] {
        FileSource.decodingFoldableFrom(FileFormat.ParquetFormat, newMeter(), Int.MaxValue)
      }
      thrown.getMessage should include("websocket")
    }

    it("BufferedSeekableInput round-trips identically to FileSeekableInput") {
      val file = writeParquetFile(4)
      val bytes = Files.readAllBytes(file.toPath)
      val decoder = new ParquetDecoder
      val fileRows = Await.result(
        decoder.decode(new FileSeekableInput(file)).map(_.get).runWith(Sink.seq),
        30.seconds,
      )
      val buffRows = Await.result(
        decoder.decode(new BufferedSeekableInput(bytes)).map(_.get).runWith(Sink.seq),
        30.seconds,
      )
      fileRows shouldEqual buffRows
      file.delete()
    }
  }
}

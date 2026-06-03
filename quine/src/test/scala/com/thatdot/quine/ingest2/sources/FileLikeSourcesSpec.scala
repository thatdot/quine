package com.thatdot.quine.ingest2.sources

import java.io.ByteArrayOutputStream
import java.nio.charset.{Charset, StandardCharsets}

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.FileFormat
import com.thatdot.quine.app.model.ingest2.FileFormat.{CsvFormat, JsonFormat, JsonLinesFormat, LineFormat}
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.model.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.model.ingest2.sources.{DEFAULT_CHARSET, DEFAULT_MAXIMUM_LINE_SIZE}
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.graph.cypher.{Expr, Value}
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.ingest2.IngestSourceTestSupport.{
  buildDecodedSource,
  randomString,
  srcFromString,
  streamedCypherValues,
}
import com.thatdot.quine.routes.CsvCharacter
import com.thatdot.quine.serialization.AvroSchemaCache

class FileLikeSourcesSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private def generateJsonSample(length: Int, delimiter: String = "\n"): String =
    1.to(length).map(n => s"""{"A":$n}""").mkString(delimiter)

  private def generateLineSample(length: Int): String = 1.to(length).map(i => s"ABCDEFG_$i").mkString("\n")

  /*note: sample length for metering does not include the delimiters */
  private def calculatedByteLength(sample: String, bounds: IngestBounds = IngestBounds()): Int = {
    val sizes = sample.split("\n").map(_.length).drop(bounds.startAtOffset.intValue())
    val bounded = bounds.ingestLimit.fold(sizes)(limit => sizes.take(limit.intValue()))
    bounded.sum
  }

  type TestResult = (IngestMeter, List[Value])
  implicit val actorSystem: ActorSystem = ActorSystem("StreamDecodersSpec")
  implicit val ec: ExecutionContext = actorSystem.getDispatcher
  implicit val avroSchemaCache: AvroSchemaCache = (_: java.net.URL) =>
    Future.failed(new UnsupportedOperationException("AvroSchemaCache not available in tests"))

  private def generateValues(
    sample: String,
    format: FileFormat,
    bounds: IngestBounds = IngestBounds(),
    maximumLineSize: Int = DEFAULT_MAXIMUM_LINE_SIZE,
    contentDecoders: Seq[ContentDecoder] = Seq(),
  ): TestResult = {

    val src: Source[ByteString, NotUsed] = srcFromString(sample).via(ContentDecoder.encoderFlow(contentDecoders))

    val decodedSource = buildDecodedSource(
      src,
      format,
      bounds,
      maximumLineSize,
      contentDecoders,
    )

    (decodedSource.meter, streamedCypherValues(decodedSource).toList)
  }

  private def generateCsvValues(
    sample: String,
    format: CsvFormat,
    bounds: IngestBounds = IngestBounds(),
    maximumLineSize: Int = DEFAULT_MAXIMUM_LINE_SIZE,
    contentDecoders: Seq[ContentDecoder] = Seq(),
  ): TestResult = {
    val src = srcFromString(sample).via(ContentDecoder.encoderFlow(contentDecoders))
    val meter = IngestMetered.ingestMeter(
      defaultNamespaceId,
      randomString(),
      HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
    )

    val decodedSource: DecodedSource = decodedSourceFromFileStream(
      src,
      format,
      Charset.defaultCharset(),
      maximumLineSize,
      bounds,
      meter: IngestMeter,
      contentDecoders,
    ).toOption.get

    (meter, streamedCypherValues(decodedSource).toList)

  }

  describe("CypherJson Stream") {
    val jsonSample = generateJsonSample(50)

    it("reads all values") {
      val (meter, values) = generateValues(jsonSample, JsonLinesFormat)
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1),
        ),
      )

      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample)
      meter.counts.getCount shouldBe 50
    }

    it("reads all values w/o line delimiter") {
      val undelimitedSample = generateJsonSample(50, "")
      val (meter, values) = generateValues(undelimitedSample, JsonFormat)
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1),
        ),
      )

      meter.bytes.getCount shouldBe calculatedByteLength(undelimitedSample)
      meter.counts.getCount shouldBe 50
    }

    it("respects value bounds") {
      val resultCount = 11 + Random.nextInt(30)
      val bounds = IngestBounds(10, Some(resultCount.longValue()))
      val (meter, values) = generateValues(jsonSample, JsonLinesFormat, bounds)
      values.length shouldEqual resultCount
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(11),
        ),
      )

      meter.counts.getCount shouldBe resultCount
      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample, bounds)
    }

    it("uses gzip,base64 decoders") {
      val (meter, values) = generateValues(
        jsonSample,
        JsonLinesFormat,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1),
        ),
      )

      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample)
      meter.counts.getCount shouldBe 50
    }

    it("uses zlib,base64 decoders") {
      val (meter, values) = generateValues(
        jsonSample,
        JsonLinesFormat,
        contentDecoders = Seq(ContentDecoder.ZlibDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1),
        ),
      )

      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample)
      meter.counts.getCount shouldBe 50
    }

    // TODO json does not respect maxLine bounds ("bounds at maxLine") {
  }

  describe("CypherLine Stream") {
    val lineSample = generateLineSample(50)

    it("reads all values") {

      val (meter, values) = generateValues(lineSample, LineFormat)
      values.length shouldEqual 50
      values.head shouldEqual Expr.Str("ABCDEFG_1")

      meter.bytes.getCount shouldBe calculatedByteLength(lineSample)
      meter.counts.getCount shouldBe 50
    }

    it("respects value bounds") {
      val resultCount = 11 + Random.nextInt(30)
      val bounds = IngestBounds(10, Some(resultCount.longValue()))
      val (meter, values) = generateValues(lineSample, LineFormat, bounds)
      values.length shouldEqual resultCount
      values.head shouldEqual Expr.Str("ABCDEFG_11")

      meter.counts.getCount shouldBe resultCount
      meter.bytes.getCount shouldBe calculatedByteLength(lineSample, bounds)
    }

    it("exceeding maxLineBounds fails the stream") {
      val testValues = List("ABC", "ABCDEFGHIJK", "ABCDEF").mkString("\n")
      //this will generate a FramingException: "Read 8 bytes which is more than 7 without seeing a line terminator"
      val v = Try {
        generateValues(testValues, LineFormat, maximumLineSize = 7)
      }
      v shouldBe a[Failure[_]]
    }

    it("uses gzip,base64 decoders") {
      val (meter, values) = generateValues(
        lineSample,
        LineFormat,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Str("ABCDEFG_1")

      meter.bytes.getCount shouldBe calculatedByteLength(lineSample)
      meter.counts.getCount shouldBe 50
    }

    it("uses zlib,base64 decoders") {
      val (meter, values) = generateValues(
        lineSample,
        LineFormat,
        contentDecoders = Seq(ContentDecoder.ZlibDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Str("ABCDEFG_1")

      meter.bytes.getCount shouldBe calculatedByteLength(lineSample)
      meter.counts.getCount shouldBe 50
    }

  }

  describe("CypherCSV Stream") {

    /** CSV sample test data:
      * e.g.
      * "A1","B1","C1",
      * "A2","B2","C2",
      * ...
      * with configurable delimiter, separators
      */
    val csvSample: String = 1.to(3).map(n => s"A$n,B$n,C$n").mkString("\n")

    it("reads a stream of values w/o headers") {

      val format =
        CsvFormat(Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)
      val (meter, values) = generateCsvValues(csvSample, format)

      values.length shouldEqual 3
      values.head shouldEqual Expr.List(Vector(Expr.Str("A1"), Expr.Str("B1"), Expr.Str("C1")))

      meter.counts.getCount shouldBe 3
      // byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))
    }

    it("reads a stream of values with first line as headers") {
      val format =
        CsvFormat(Left(true), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)
      val (meter, values) = generateCsvValues(csvSample, format)

      values.length shouldEqual 2
      values.head shouldEqual Expr.Map(
        SortedMap("A1" -> Expr.Str("A2"), "B1" -> Expr.Str("B2"), "C1" -> Expr.Str("C2")),
      )
      meter.counts.getCount shouldBe 3
      // byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))
    }

    it("reads a stream of values with specified headers") {
      val format = CsvFormat(
        Right(List("X", "Y", "Z")),
        CsvCharacter.Comma,
        CsvCharacter.DoubleQuote,
        CsvCharacter.Backslash,
      )
      val (meter, values) = generateCsvValues(csvSample, format)

      values.length shouldEqual 3
      values.head shouldEqual Expr.Map(SortedMap("X" -> Expr.Str("A1"), "Y" -> Expr.Str("B1"), "Z" -> Expr.Str("C1")))

      meter.counts.getCount shouldBe 3
      //byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))
    }

    it("respects value bounds") {
      val format =
        CsvFormat(Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)

      val bounds = IngestBounds(1, Some(1L))
      val (meter, values) = generateValues(csvSample, format, bounds)
      values.length shouldEqual 1
      values.head shouldEqual Expr.List(Vector(Expr.Str("A2"), Expr.Str("B2"), Expr.Str("C2")))

      meter.counts.getCount shouldBe 1
      //byte meter ignores field delimiter
      //only meters 1 value of the 3
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", "")) / 3
    }

    it("exceeding maxLineBounds fails the stream") {
      val format =
        CsvFormat(Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)

      val v = Try {
        generateCsvValues(csvSample, format, maximumLineSize = 7)
      }
      v shouldBe a[Failure[_]] //MalformedCsvException
    }

    it("uses gzip,base64 decoders") {
      val format =
        CsvFormat(Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)

      val (meter, values) = generateCsvValues(
        csvSample,
        format,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 3
      values.head shouldEqual Expr.List(Vector(Expr.Str("A1"), Expr.Str("B1"), Expr.Str("C1")))

      meter.counts.getCount shouldBe 3
      // byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))

    }

  }

  describe("Avro Container Stream") {

    val avroSchema: Schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "fields": [
        |    {"name": "id", "type": "int"},
        |    {"name": "name", "type": "string"}
        |  ]
        |}""".stripMargin,
    )

    def makeAvroContainerBytes(count: Int, codec: CodecFactory = CodecFactory.nullCodec()): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](avroSchema))
      writer.setCodec(codec)
      writer.create(avroSchema, baos)
      for (i <- 1 to count) {
        val record = new GenericData.Record(avroSchema)
        record.put("id", i)
        record.put("name", s"record_$i")
        writer.append(record)
      }
      writer.close()
      baos.toByteArray
    }

    def avroSource(bytes: Array[Byte]): Source[ByteString, NotUsed] =
      Source.single(ByteString(bytes))

    def generateAvroValues(
      bytes: Array[Byte],
      bounds: IngestBounds = IngestBounds(),
      contentDecoders: Seq[ContentDecoder] = Seq(),
    ): TestResult = {
      val src = avroSource(bytes).via(ContentDecoder.encoderFlow(contentDecoders))
      val decodedSource = buildDecodedSource(
        src,
        FileFormat.AvroContainerFormat(None),
        bounds,
        contentDecoders = contentDecoders,
      )
      (decodedSource.meter, streamedCypherValues(decodedSource).toList)
    }

    it("reads all values") {
      val bytes = makeAvroContainerBytes(5)
      val (meter, values) = generateAvroValues(bytes)
      values.length shouldEqual 5
      values.head shouldEqual Expr.Map(
        TreeMap(
          "id" -> Expr.Integer(1),
          "name" -> Expr.Str("record_1"),
        ),
      )
      values.last shouldEqual Expr.Map(
        TreeMap(
          "id" -> Expr.Integer(5),
          "name" -> Expr.Str("record_5"),
        ),
      )
      meter.counts.getCount shouldBe 5
    }

    it("reads empty container") {
      val bytes = makeAvroContainerBytes(0)
      val (meter, values) = generateAvroValues(bytes)
      values.length shouldEqual 0
      meter.counts.getCount shouldBe 0
    }

    it("respects value bounds") {
      val bytes = makeAvroContainerBytes(20)
      val bounds = IngestBounds(5, Some(3L))
      val (meter, values) = generateAvroValues(bytes, bounds)
      values.length shouldEqual 3
      values.head shouldEqual Expr.Map(
        TreeMap(
          "id" -> Expr.Integer(6),
          "name" -> Expr.Str("record_6"),
        ),
      )
      meter.counts.getCount shouldBe 3
    }

    it("reads deflate-compressed container") {
      val bytes = makeAvroContainerBytes(10, CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL))
      val (meter, values) = generateAvroValues(bytes)
      values.length shouldEqual 10
      meter.counts.getCount shouldBe 10
    }

    it("uses gzip,base64 decoders") {
      val bytes = makeAvroContainerBytes(5)
      val (meter, values) = generateAvroValues(
        bytes,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 5
      values.head shouldEqual Expr.Map(
        TreeMap(
          "id" -> Expr.Integer(1),
          "name" -> Expr.Str("record_1"),
        ),
      )
      meter.counts.getCount shouldBe 5
    }

    it("uses zlib,base64 decoders") {
      val bytes = makeAvroContainerBytes(5)
      val (meter, values) = generateAvroValues(
        bytes,
        contentDecoders = Seq(ContentDecoder.ZlibDecoder, ContentDecoder.Base64Decoder),
      )
      values.length shouldEqual 5
      values.head shouldEqual Expr.Map(
        TreeMap(
          "id" -> Expr.Integer(1),
          "name" -> Expr.Str("record_1"),
        ),
      )
      meter.counts.getCount shouldBe 5
    }

    it("mid-block corruption: post-success decode failure is wrapped as Failure and terminates iteration") {
      // Write block 1 (5 small records), force a block boundary, then a large block 2
      // (100 records). DataFileStream tolerates simple truncation as a clean stream end
      // (it catches EOFException in hasNext), so we instead OVERWRITE the second half of
      // the file — well past block 2's count/length varints — with 0xFF. Block 2's header
      // varints still read cleanly (claiming 100 records and a large data section), but
      // record decoding inside the data buffer hits a string-length varint that never
      // terminates with a low-bit byte, and the parser throws.
      //
      // Per SafeAvroIterator: that thrown exception — once we've had any success — must
      // be returned as a Failure element, and the iterator must terminate. This validates
      // the two-mode design (pre-success failures throw; post-success failures wrap to DLQ).
      val baos = new ByteArrayOutputStream()
      val writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](avroSchema))
      writer.create(avroSchema, baos)
      for (i <- 1 to 5) {
        val r = new GenericData.Record(avroSchema)
        r.put("id", i); r.put("name", s"record_$i")
        writer.append(r)
      }
      writer.sync() // close block 1; appends below go into block 2
      for (i <- 6 to 105) {
        val r = new GenericData.Record(avroSchema)
        r.put("id", i); r.put("name", s"record_$i")
        writer.append(r)
      }
      writer.close()
      val full = baos.toByteArray
      // Overwrite the second half of the file with 0xFF. For a file with this layout, half
      // the way through is deep inside block 2's data (well past the header / block 1 /
      // block 2's count+length varints), so the count+length still parse but the records
      // can't be decoded.
      val corrupted = full.clone()
      for (i <- (corrupted.length / 2) until corrupted.length) corrupted(i) = 0xFF.toByte

      val decodedSource = buildDecodedSource(
        Source.single(ByteString(corrupted)),
        FileFormat.AvroContainerFormat(None),
      )
      val results = Await
        .result(
          decodedSource.stream.map { case (tryDecoded, _) => tryDecoded() }.runWith(Sink.seq),
          30.seconds,
        )
        .toList

      val successCount = results.count(_.isSuccess)
      val failureCount = results.count(_.isFailure)
      // Block 1's 5 records succeed; some prefix of block 2 may also succeed before hitting
      // the corruption. We just assert block 1 fully decoded, the iterator emitted exactly
      // one wrapped failure, and terminated immediately (no further elements after it).
      successCount should be >= 5
      failureCount shouldEqual 1
      decodedSource.meter.counts.getCount shouldEqual successCount.toLong
    }

    it("pre-first-success failure: non-Avro bytes propagate as a stream exception") {
      // No record has decoded yet, so SafeAvroIterator must throw rather than emit a Failure
      // element. That lets a surrounding retry stage re-materialize the byte source — useful
      // when the failure is a header / schema-fetch problem that may resolve on a retry.
      val notAvro = "This is not an Avro container file".getBytes(StandardCharsets.UTF_8)
      val decodedSource = buildDecodedSource(
        Source.single(ByteString(notAvro)),
        FileFormat.AvroContainerFormat(None),
      )
      an[Exception] should be thrownBy Await.result(
        decodedSource.stream.runWith(Sink.ignore),
        30.seconds,
      )
    }

    it("reader schema evolution: a defaulted field added by the reader appears on every record") {
      // Writer schema has (id, name); reader adds `active: boolean = true`. Records decoded
      // against the reader schema should carry the defaulted field, proving the schemaUrl
      // actually drives the GenericDatumReader rather than being silently ignored.
      val evolvedSchema = new Schema.Parser().parse(
        """{
          |  "type": "record",
          |  "name": "TestRecord",
          |  "fields": [
          |    {"name": "id", "type": "int"},
          |    {"name": "name", "type": "string"},
          |    {"name": "active", "type": "boolean", "default": true}
          |  ]
          |}""".stripMargin,
      )
      val cacheForEvolution: AvroSchemaCache =
        (_: java.net.URL) => Future.successful(evolvedSchema)

      val meter = IngestMetered.ingestMeter(
        defaultNamespaceId,
        randomString(),
        HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
      )
      val decodedSource = decodedSourceFromFileStream(
        Source.single(ByteString(makeAvroContainerBytes(3))),
        FileFormat.AvroContainerFormat(Some("evolved-schema-url")),
        DEFAULT_CHARSET,
        DEFAULT_MAXIMUM_LINE_SIZE,
        IngestBounds(),
        meter,
        Seq.empty,
      )(cacheForEvolution).toOption.get

      val values = streamedCypherValues(decodedSource).toList
      values.length shouldEqual 3
      values.head shouldEqual Expr.Map(
        TreeMap(
          "id" -> Expr.Integer(1),
          "name" -> Expr.Str("record_1"),
          "active" -> Expr.True,
        ),
      )
      meter.counts.getCount shouldBe 3
    }
  }
}

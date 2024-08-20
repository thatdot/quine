package com.thatdot.quine.ingest2.sources

import java.nio.charset.Charset

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.ingest2.sources.{DEFAULT_CHARSET, DEFAULT_MAXIMUM_LINE_SIZE, FileSource}
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.graph.cypher.{Expr, Value}
import com.thatdot.quine.ingest2.IngestSourceTestSupport.{randomString, srcFromString, streamedCypherValues}
import com.thatdot.quine.routes.FileIngestFormat.{CypherCsv, CypherJson, CypherLine}
import com.thatdot.quine.routes.{CsvCharacter, FileIngestFormat}

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

  private def generateValues(
    sample: String,
    format: FileIngestFormat,
    bounds: IngestBounds = IngestBounds(),
    maximumLineSize: Int = DEFAULT_MAXIMUM_LINE_SIZE,
    contentDecoders: Seq[ContentDecoder] = Seq()
  ): TestResult = {
    val meter = IngestMetered.ingestMeter(None, randomString())

    val src: Source[ByteString, NotUsed] = srcFromString(sample).via(ContentDecoder.encoderFlow(contentDecoders))
    val decodedSource = FileSource.decodedSourceFromFileStream(
      src,
      format,
      DEFAULT_CHARSET,
      maximumLineSize,
      bounds,
      meter,
      contentDecoders
    )

    (meter, streamedCypherValues(decodedSource).toList)
  }

  private def generateCsvValues(
    sample: String,
    format: CypherCsv,
    bounds: IngestBounds = IngestBounds(),
    maximumLineSize: Int = DEFAULT_MAXIMUM_LINE_SIZE,
    contentDecoders: Seq[ContentDecoder] = Seq()
  ): TestResult = {
    val src = srcFromString(sample).via(ContentDecoder.encoderFlow(contentDecoders))
    val meter = IngestMetered.ingestMeter(None, randomString())

    val decodedSource: DecodedSource = FileSource.decodedSourceFromFileStream(
      src,
      format,
      Charset.defaultCharset(),
      maximumLineSize,
      bounds,
      meter: IngestMeter,
      contentDecoders
    )

    (meter, streamedCypherValues(decodedSource).toList)

  }

  describe("CypherJson Stream") {
    val jsonSample = generateJsonSample(50)

    val format = CypherJson("MATCH (p) RETURN (p)")

    it("reads all values") {
      val (meter, values) = generateValues(jsonSample, format)
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1)
        )
      )

      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample)
      meter.counts.getCount shouldBe 50
    }

    it("reads all values w/o line delimiter") {
      val undelimitedSample = generateJsonSample(50, "")
      val (meter, values) = generateValues(undelimitedSample, format)
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1)
        )
      )

      meter.bytes.getCount shouldBe calculatedByteLength(undelimitedSample)
      meter.counts.getCount shouldBe 50
    }

    it("respects value bounds") {
      val resultCount = 11 + Random.nextInt(30)
      val bounds = IngestBounds(10, Some(resultCount.longValue()))
      val (meter, values) = generateValues(jsonSample, format, bounds)
      values.length shouldEqual resultCount
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(11)
        )
      )

      meter.counts.getCount shouldBe resultCount
      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample, bounds)
    }

    it("uses gzip,base64 decoders") {
      val (meter, values) = generateValues(
        jsonSample,
        format,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder)
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1)
        )
      )

      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample)
      meter.counts.getCount shouldBe 50
    }

    it("uses zlib,base64 decoders") {
      val (meter, values) = generateValues(
        jsonSample,
        format,
        contentDecoders = Seq(ContentDecoder.ZlibDecoder, ContentDecoder.Base64Decoder)
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Map(
        TreeMap(
          "A" -> Expr.Integer(1)
        )
      )

      meter.bytes.getCount shouldBe calculatedByteLength(jsonSample)
      meter.counts.getCount shouldBe 50
    }

    // TODO json does not respect maxLine bounds ("bounds at maxLine") {
  }

  describe("CypherLine Stream") {
    val lineSample = generateLineSample(50)

    val format = CypherLine("MATCH (p) RETURN (p)")

    it("reads all values") {

      val (meter, values) = generateValues(lineSample, format)
      values.length shouldEqual 50
      values.head shouldEqual Expr.Str("ABCDEFG_1")

      meter.bytes.getCount shouldBe calculatedByteLength(lineSample)
      meter.counts.getCount shouldBe 50
    }

    it("respects value bounds") {
      val resultCount = 11 + Random.nextInt(30)
      val bounds = IngestBounds(10, Some(resultCount.longValue()))
      val (meter, values) = generateValues(lineSample, format, bounds)
      values.length shouldEqual resultCount
      values.head shouldEqual Expr.Str("ABCDEFG_11")

      meter.counts.getCount shouldBe resultCount
      meter.bytes.getCount shouldBe calculatedByteLength(lineSample, bounds)
    }

    it("exceeding maxLineBounds fails the stream") {
      val testValues = List("ABC", "ABCDEFGHIJK", "ABCDEF").mkString("\n")
      //this will generate a FramingException: "Read 8 bytes which is more than 7 without seeing a line terminator"
      val v = Try {
        generateValues(testValues, format, maximumLineSize = 7)
      }
      v shouldBe a[Failure[_]]
    }

    it("uses gzip,base64 decoders") {
      val (meter, values) = generateValues(
        lineSample,
        format,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder)
      )
      values.length shouldEqual 50
      values.head shouldEqual Expr.Str("ABCDEFG_1")

      meter.bytes.getCount shouldBe calculatedByteLength(lineSample)
      meter.counts.getCount shouldBe 50
    }

    it("uses zlib,base64 decoders") {
      val (meter, values) = generateValues(
        lineSample,
        format,
        contentDecoders = Seq(ContentDecoder.ZlibDecoder, ContentDecoder.Base64Decoder)
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
        CypherCsv("", "that", Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)
      val (meter, values) = generateCsvValues(csvSample, format)

      values.length shouldEqual 3
      values.head shouldEqual Expr.List(Vector(Expr.Str("A1"), Expr.Str("B1"), Expr.Str("C1")))

      meter.counts.getCount shouldBe 3
      //byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))
    }

    it("reads a stream of values with first line as headers") {
      val format =
        CypherCsv("", "that", Left(true), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)
      val (meter, values) = generateCsvValues(csvSample, format)

      values.length shouldEqual 2
      values.head shouldEqual Expr.Map(
        SortedMap("A1" -> Expr.Str("A2"), "B1" -> Expr.Str("B2"), "C1" -> Expr.Str("C2"))
      )
      meter.counts.getCount shouldBe 3
      //byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))
    }

    it("reads a stream of values with specified headers") {
      val format = CypherCsv(
        "",
        "that",
        Right(List("X", "Y", "Z")),
        CsvCharacter.Comma,
        CsvCharacter.DoubleQuote,
        CsvCharacter.Backslash
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
        CypherCsv("", "that", Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)

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
        CypherCsv("", "that", Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)

      val v = Try {
        generateCsvValues(csvSample, format, maximumLineSize = 7)
      }
      v shouldBe a[Failure[_]] //MalformedCsvException
    }

    it("uses gzip,base64 decoders") {
      val format =
        CypherCsv("", "that", Left(false), CsvCharacter.Comma, CsvCharacter.DoubleQuote, CsvCharacter.Backslash)

      val (meter, values) = generateCsvValues(
        csvSample,
        format,
        contentDecoders = Seq(ContentDecoder.GzipDecoder, ContentDecoder.Base64Decoder)
      )
      values.length shouldEqual 3
      values.head shouldEqual Expr.List(Vector(Expr.Str("A1"), Expr.Str("B1"), Expr.Str("C1")))

      meter.counts.getCount shouldBe 3
      //byte meter ignores field delimiter
      meter.bytes.getCount shouldBe calculatedByteLength(csvSample.replace(",", ""))

    }

  }

}

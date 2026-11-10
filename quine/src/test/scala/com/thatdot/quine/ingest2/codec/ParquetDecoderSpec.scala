package com.thatdot.quine.ingest2.codec

import java.nio.charset.MalformedInputException
import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder
import com.thatdot.quine.app.model.ingest2.codec.{
  BufferedSeekableInput,
  FileSeekableInput,
  ParquetDecoder,
  ParquetRecord,
  SeekableInput,
}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr => ce}

/** Tests [[ParquetDecoder]] against files written by parquet-java. See `src/test/resources/parquet/README.md`. */
class ParquetDecoderSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("ParquetDecoderSpec")

  override def afterAll(): Unit = {
    val _ = system.terminate()
  }

  private def fixture(name: String): java.io.File =
    Paths.get(getClass.getResource(s"/parquet/$name").toURI).toFile

  private def decode(input: SeekableInput): Seq[Try[ParquetRecord]] =
    Await.result(new ParquetDecoder().decode(input).runWith(Sink.seq), 30.seconds)

  private def rowsAsCypher(name: String): Seq[cypher.Value] =
    decode(new FileSeekableInput(fixture(name))).map(r => ParquetRecord.foldable.fold(r.get, cypherValueFolder))

  private def fields(value: cypher.Value): Map[String, cypher.Value] = value match {
    case ce.Map(fields) => fields
    case other => fail(s"Not a map: $other")
  }

  it("reads simple.parquet") {
    rowsAsCypher("simple.parquet") shouldEqual Seq(
      ce.Map("id" -> ce.Integer(1L), "name" -> ce.Str("alice")),
      ce.Map("id" -> ce.Integer(2L), "name" -> ce.Str("bob")),
      ce.Map("id" -> ce.Integer(3L), "name" -> ce.Null),
    )
  }

  it("reads every column of all-types.parquet") {
    val (column, nullRow) = {
      val rows = rowsAsCypher("all-types.parquet").map(fields)
      rows should have size 2
      (rows(0), rows(1))
    }

    // Primitive columns.
    column("id") shouldEqual ce.Integer(1L)
    column("flag") shouldEqual ce.True
    column("f") shouldEqual ce.Floating(1.5)
    column("d") shouldEqual ce.Floating(2.5)
    column("i32") shouldEqual ce.Integer(42L)
    column("i64") shouldEqual ce.Integer(9876543210L)
    column("i8") shouldEqual ce.Integer(-5L)

    // DATE, and DECIMAL from all four physical types as strings.
    column("date32") shouldEqual ce.Date(LocalDate.of(2024, 1, 15))
    column("dec32") shouldEqual ce.Str("123.45")
    column("dec64") shouldEqual ce.Str("12.345")
    column("decbin") shouldEqual ce.Str("123.45")
    column("decfix") shouldEqual ce.Str("123.4567")

    // TIME in all three units.
    column("time_ms") shouldEqual ce.LocalTime(LocalTime.NOON)
    column("time_us") shouldEqual ce.LocalTime(LocalTime.NOON)
    column("time_ns") shouldEqual ce.LocalTime(LocalTime.of(12, 0, 0, 123))

    // UTC-adjusted TIMESTAMP in all three units, one before the Unix epoch, an unadjusted TIMESTAMP, and INT96.
    column("ts_ms_utc") shouldEqual ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 0, ZoneOffset.UTC))
    column("ts_us_utc") shouldEqual ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456000, ZoneOffset.UTC))
    column("ts_ns_utc") shouldEqual ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456789, ZoneOffset.UTC))
    column("ts_neg_us") shouldEqual
    ce.DateTime(ZonedDateTime.of(1969, 12, 31, 23, 59, 58, 500000000, ZoneOffset.UTC))
    column("ts_ms_local") shouldEqual ce.LocalDateTime(LocalDateTime.of(2024, 1, 15, 10, 40, 0))
    column("ts96") shouldEqual ce.LocalDateTime(LocalDateTime.of(2024, 1, 14, 12, 0, 0, 123456789))

    // STRING, ENUM, JSON (valid and invalid), BSON, and unannotated binary.
    column("str") shouldEqual ce.Str("héllo 日本語")
    column("en") shouldEqual ce.Str("RED")
    column("js") shouldEqual ce.Map("nested" -> ce.True, "n" -> ce.Integer(7L))
    column("badjs") shouldEqual ce.Str("not valid json {")
    column("bs") shouldEqual ce.Bytes(Array[Byte](1, 2, 3, 4))
    column("raw") shouldEqual ce.Bytes(Array[Byte](0, 0xFF.toByte, 0x80.toByte, 0x7F))
    column("fixed") shouldEqual ce.Bytes(Array[Byte](1, 2, 3))

    // UUID, FLOAT16, and INTERVAL.
    column("uuid") shouldEqual ce.Str("550e8400-e29b-41d4-a716-446655440000")
    column("f16") shouldEqual ce.Floating(1.0)
    column("iv") shouldEqual ce.Map("months" -> ce.Integer(14L), "days" -> ce.Integer(3L), "millis" -> ce.Integer(500L))

    // LIST, MAP (including non-string keys and no value field), nested structs, and an unannotated repeated field.
    column("tags") shouldEqual ce.List(ce.Str("x"), ce.Null, ce.Str("y"))
    column("dates") shouldEqual ce.List(ce.Date(LocalDate.of(2024, 1, 15)))
    column("attrs") shouldEqual ce.Map(
      "created" -> ce.DateTime(ZonedDateTime.of(1970, 1, 1, 0, 0, 1, 0, ZoneOffset.UTC)),
      "missing" -> ce.Null,
    )
    column("intkeys") shouldEqual ce.Map("42" -> ce.Str("value"))
    column("keys_only") shouldEqual ce.Map("a" -> ce.Null, "b" -> ce.Null)
    column("addr") shouldEqual ce.Map(
      "city" -> ce.Str("Portland"),
      "geo" -> ce.Map("lat" -> ce.Floating(45.5)),
      "day" -> ce.Date(LocalDate.of(2024, 1, 15)),
    )
    column("list_of_maps") shouldEqual ce.List(ce.Map("k" -> ce.Integer(1L)))
    column("legacy_repeated") shouldEqual ce.List(ce.Integer(1L), ce.Integer(2L))

    // A variant object holding every variant type.
    val field = fields(column("variant_object"))
    field("nul") shouldEqual ce.Null
    field("t") shouldEqual ce.True
    field("f") shouldEqual ce.False
    field("i8") shouldEqual ce.Integer(-5L)
    field("i16") shouldEqual ce.Integer(300L)
    field("i32") shouldEqual ce.Integer(70000L)
    field("i64") shouldEqual ce.Integer(9876543210L)
    field("dbl") shouldEqual ce.Floating(2.5)
    field("flt") shouldEqual ce.Floating(1.5)
    field("dec4") shouldEqual ce.Str("12.34")
    field("dec8") shouldEqual ce.Str("1234567890.12")
    field("dec16") shouldEqual ce.Str("12345678901234567890.1234")
    field("bin") shouldEqual ce.Bytes(Array[Byte](1, 2, 3))
    field("str") shouldEqual ce.Str("héllo 日本語")
    field("uuid") shouldEqual ce.Str("550e8400-e29b-41d4-a716-446655440000")
    field("date") shouldEqual ce.Date(LocalDate.of(2024, 1, 15))
    field("time") shouldEqual ce.LocalTime(LocalTime.NOON)
    field("ts") shouldEqual ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456000, ZoneOffset.UTC))
    field("tsn") shouldEqual ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456789, ZoneOffset.UTC))
    field("ntz") shouldEqual ce.LocalDateTime(LocalDateTime.of(2024, 1, 15, 10, 40, 0, 123456000))
    field("ntzn") shouldEqual ce.LocalDateTime(LocalDateTime.of(2024, 1, 15, 10, 40, 0, 123456789))
    field("nested") shouldEqual ce.Map("x" -> ce.Integer(1L))
    field("arr") shouldEqual ce.List(ce.Integer(1L), ce.Str("two"), ce.Null, ce.List(ce.True))

    // Variants holding a primitive, a variant null, or an array; shredded variants; and variants in a struct and a list.
    column("variant_primitive") shouldEqual ce.Integer(42L)
    column("variant_null") shouldEqual ce.Null
    column("variant_array") shouldEqual ce.List(ce.Integer(1L), ce.Str("x"))
    column("shredded_object") shouldEqual ce.Map("a" -> ce.Integer(7L), "b" -> ce.Str("x"), "c" -> ce.Integer(9L))
    column("shredded_primitive") shouldEqual ce.Integer(5L)
    column("shredded_partial") shouldEqual ce.Map("b" -> ce.Str("y"))
    column("variant_struct") shouldEqual ce.Map("inner" -> ce.Str("in struct"))
    column("variant_list") shouldEqual ce.List(ce.Str("p"), ce.Integer(3L))

    // The second row populates only the required column.
    nullRow("id") shouldEqual ce.Integer(2L)
    (nullRow - "id" - "legacy_repeated").values.toSet shouldEqual Set(ce.Null)
  }

  it("reads every row of each compression codec file in file order") {
    val expected = (0 until 2000).map { i =>
      ce.Map("id" -> ce.Integer(i.toLong), "name" -> (if (i % 10 == 0) ce.Null else ce.Str(s"row_${i % 3}")))
    }
    Seq("codec-snappy", "codec-gzip", "codec-zstd", "codec-lz4_raw").foreach { name =>
      withClue(s"$name.parquet: ") {
        rowsAsCypher(s"$name.parquet") shouldEqual expected
      }
    }
  }

  it("reads the same rows from memory as from a local file") {
    val file = fixture("all-types.parquet")
    val fromFile = decode(new FileSeekableInput(file)).map(_.get)
    decode(new BufferedSeekableInput(Files.readAllBytes(file.toPath))).map(_.get) shouldEqual fromFile
  }

  it("fails the stream when a file is unreadable") {
    // Bytes that are not Parquet, or a corrupt first data page, fail the stream.
    an[Exception] should be thrownBy decode(new BufferedSeekableInput("not parquet".getBytes))
    an[Exception] should be thrownBy decode(new FileSeekableInput(fixture("corrupt-first-page.parquet")))

    // A corrupt later data page produces rows, then fails the stream.
    val rows = ListBuffer.empty[Try[ParquetRecord]]
    val result = new ParquetDecoder()
      .decode(new FileSeekableInput(fixture("corrupt-later-page.parquet")))
      .runForeach(row => rows += row)
    an[Exception] should be thrownBy Await.result(result, 30.seconds)
    rows should not be empty
    rows.foreach(_.isSuccess shouldEqual true)
  }

  it("rejects malformed UTF-8 in a STRING map key when the record is folded") {
    val record = decode(new FileSeekableInput(fixture("malformed-map-key.parquet"))).head.get
    a[MalformedInputException] should be thrownBy ParquetRecord.foldable.fold(record, cypherValueFolder)
  }
}

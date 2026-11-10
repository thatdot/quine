package com.thatdot.quine.ingest2.codec

import java.nio.charset.MalformedInputException
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.data.DataFolderTo
import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder
import com.thatdot.quine.app.model.ingest2.codec.ParquetValue._
import com.thatdot.quine.app.model.ingest2.codec.{ParquetRecord, ParquetValue}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr => ce}

/** Tests `ParquetRecord.foldable` — the canonical fold path for a Parquet row.
  *
  * The bulk of these tests fold through `cypherValueFolder` rather than the JSON folder.
  * Cypher preserves typed dispatch — `Expr.Date`, `Expr.LocalTime`, `Expr.DateTime`,
  * `Expr.LocalDateTime`, `Expr.Bytes`, `Expr.Integer(Long)`, `Expr.Floating(Double)` — so
  * tests can assert which folder method the foldable actually called. JSON collapses all
  * of those to strings/numbers, masking the dispatch.
  *
  * A smaller `JSON folder` block at the bottom exercises the JSON-specific serialization
  * conventions (NaN-as-string, bytes-as-base64, ISO-8601 temporals) that the JSON folder
  * itself imposes — those are properties of the JSON folder, not of the Parquet decoder.
  *
  * `ParquetDecoderSpec` tests how values in real Parquet files become `ParquetValue`s.
  */
class ParquetRecordFoldableSpec extends AnyFunSpec with Matchers {

  private def asCypher(fields: (String, ParquetValue)*): cypher.Value =
    ParquetRecord.foldable.fold(ParquetRecord(fields.toVector), cypherValueFolder)

  private def asJson(fields: (String, ParquetValue)*): Json =
    ParquetRecord.foldable.fold(ParquetRecord(fields.toVector), DataFolderTo[Json])

  describe("Cypher folder") {
    it("Bool folds to Expr.True / Expr.False") {
      asCypher("a" -> Bool(true), "b" -> Bool(false)) shouldEqual ce.Map("a" -> ce.True, "b" -> ce.False)
    }
    it("Floating folds to Expr.Floating") {
      asCypher("d" -> Floating(2.718)) shouldEqual ce.Map("d" -> ce.Floating(2.718))
    }
    it("Long.MaxValue / Long.MinValue preserved exactly — JSON would degrade to string at this magnitude") {
      asCypher("hi" -> Integer(Long.MaxValue), "lo" -> Integer(Long.MinValue)) shouldEqual
      ce.Map("hi" -> ce.Integer(Long.MaxValue), "lo" -> ce.Integer(Long.MinValue))
    }
    it("Date folds to Expr.Date — not a string") {
      asCypher("dt" -> Date(LocalDate.of(2024, 1, 15))) shouldEqual ce.Map("dt" -> ce.Date(LocalDate.of(2024, 1, 15)))
    }
    it("Time folds to Expr.LocalTime") {
      asCypher("t" -> Time(LocalTime.NOON)) shouldEqual ce.Map("t" -> ce.LocalTime(LocalTime.NOON))
    }
    it("Timestamp folds to Expr.DateTime in UTC") {
      asCypher("ts" -> Timestamp(Instant.ofEpochSecond(1705315200L, 123456789L))) shouldEqual ce.Map(
        "ts" -> ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456789, ZoneOffset.UTC)),
      )
    }
    it("LocalTimestamp folds to Expr.LocalDateTime, not Expr.DateTime") {
      val value = LocalDateTime.of(2024, 1, 15, 10, 40, 0)
      asCypher("ts" -> LocalTimestamp(value)) shouldEqual ce.Map("ts" -> ce.LocalDateTime(value))
    }
    it("Text with multi-byte UTF-8 folds to Expr.Str") {
      val text = "héllo wörld 日本語"
      asCypher("s" -> Text(text)) shouldEqual ce.Map("s" -> ce.Str(text))
    }
    it("JsonText (valid) parses and re-folds through the destination — typed all the way down") {
      asCypher("j" -> JsonText("""{"nested":true,"n":7}""")) shouldEqual ce.Map(
        "j" -> ce.Map("nested" -> ce.True, "n" -> ce.Integer(7L)),
      )
    }
    it("JsonText (invalid) falls back to Expr.Str") {
      asCypher("j" -> JsonText("not valid json {")) shouldEqual ce.Map("j" -> ce.Str("not valid json {"))
    }
    it("Decimal folds to Expr.Str to preserve precision") {
      asCypher("d" -> Decimal(new java.math.BigDecimal("12345678901234567890.1234"))) shouldEqual
      ce.Map("d" -> ce.Str("12345678901234567890.1234"))
    }
    it("Bytes folds to Expr.Bytes preserving raw byte identity") {
      val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)
      asCypher("raw" -> Bytes(bytes)) shouldEqual ce.Map("raw" -> ce.Bytes(bytes))
    }
    it("Interval folds to a typed Expr.Map of Expr.Integer components — not stringified") {
      asCypher("iv" -> Interval(14L, 3L, 500L)) shouldEqual ce.Map(
        "iv" -> ce.Map("months" -> ce.Integer(14L), "days" -> ce.Integer(3L), "millis" -> ce.Integer(500L)),
      )
    }
    it("ListValue folds to Expr.List with typed elements") {
      asCypher("arr" -> ListValue(Vector(Integer(1L), Null, Text("x")))) shouldEqual
      ce.Map("arr" -> ce.List(ce.Integer(1L), ce.Null, ce.Str("x")))
    }
    it("MapValue with binary keys folds to Expr.Map") {
      val map = MapValue(Vector(Bytes("key1".getBytes("UTF-8")) -> Integer(10L), Text("key2") -> Integer(20L)))
      asCypher("m" -> map) shouldEqual ce.Map("m" -> ce.Map("key1" -> ce.Integer(10L), "key2" -> ce.Integer(20L)))
    }
    it("integer MAP keys are stringified") {
      asCypher("m" -> MapValue(Vector(Integer(42L) -> Text("value")))) shouldEqual
      ce.Map("m" -> ce.Map("42" -> ce.Str("value")))
    }
    it("Struct folds to Expr.Map") {
      asCypher("nested" -> Struct(Vector("a" -> Integer(1L), "b" -> Bool(true)))) shouldEqual
      ce.Map("nested" -> ce.Map("a" -> ce.Integer(1L), "b" -> ce.True))
    }
    it("Null folds to Expr.Null") {
      asCypher("x" -> Null) shouldEqual ce.Map("x" -> ce.Null)
    }
    it("empty record folds to empty Expr.Map") {
      asCypher() shouldEqual ce.Map()
    }
    it("deeply nested LIST of MAP preserves type identity at every level") {
      asCypher("deep" -> ListValue(Vector(MapValue(Vector(Text("k") -> Integer(1L)))))) shouldEqual
      ce.Map("deep" -> ce.List(ce.Map("k" -> ce.Integer(1L))))
    }
  }

  // ---------------------------------------------------------------------------
  // Malformed UTF-8 in MAP keys — assertion is on the throw, independent of folder
  // ---------------------------------------------------------------------------

  describe("malformed UTF-8 in MAP keys") {
    def mapWithKey(key: Array[Byte]) = "m" -> MapValue(Vector(Bytes(key) -> Integer(1L)))

    it("invalid UTF-8 bytes throw MalformedInputException") {
      a[MalformedInputException] should be thrownBy asCypher(mapWithKey(Array(0xFF.toByte, 0xFE.toByte)))
    }
    it("truncated multi-byte UTF-8 throws MalformedInputException") {
      a[MalformedInputException] should be thrownBy asCypher(mapWithKey(Array(0xC0.toByte)))
    }
    it("overlong UTF-8 encoding throws MalformedInputException") {
      a[MalformedInputException] should be thrownBy asCypher(mapWithKey(Array(0xC0.toByte, 0xAF.toByte)))
    }
    it("valid UTF-8 keys (including multi-byte) round-trip") {
      asCypher(mapWithKey("日本語".getBytes("UTF-8"))) shouldEqual ce.Map("m" -> ce.Map("日本語" -> ce.Integer(1L)))
    }
  }

  // ---------------------------------------------------------------------------
  // JSON folder: assertions that are properties of the JSON folder itself
  // (NaN→string, bytes→hex, temporals→ISO-8601), not of the Parquet decoder
  // ---------------------------------------------------------------------------

  describe("JSON folder: JSON-specific serialization") {
    it("non-finite Double serializes as a JSON string (Json.fromDoubleOrString)") {
      asJson("d" -> Floating(Double.NaN)) shouldEqual Json.obj("d" -> Json.fromString("NaN"))
    }
    it("bytes serialize as standard base64") {
      val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)
      asJson("raw" -> Bytes(bytes)) shouldEqual Json.obj("raw" -> Json.fromString("AP+Afw=="))
    }
    it("Date serializes as ISO-8601 string") {
      asJson("dt" -> Date(LocalDate.of(2024, 1, 15))) shouldEqual Json.obj("dt" -> Json.fromString("2024-01-15"))
    }
    it("ZonedDateTime serializes as ISO-8601 string") {
      asJson("ts" -> Timestamp(Instant.ofEpochSecond(1705315200L))) shouldEqual
      Json.obj("ts" -> Json.fromString("2024-01-15T10:40:00Z"))
    }
  }
}

package com.thatdot.quine.ingest2.codec

import java.nio.charset.MalformedInputException
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}

import com.github.mjakubowski84.parquet4s.{
  BinaryValue,
  BooleanValue,
  DoubleValue,
  FloatValue,
  IntValue,
  ListParquetRecord,
  LongValue,
  MapParquetRecord,
  NullValue,
  RowParquetRecord,
  Value => PqValue,
}
import io.circe.Json
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.data.DataFolderTo
import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder
import com.thatdot.quine.app.model.ingest2.codec.ParquetRecord
import com.thatdot.quine.app.model.ingest2.codec.ParquetRecord.ColumnDescriptor
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
  */
class ParquetRecordFoldableSpec extends AnyFunSpec with Matchers {

  private def asCypher(record: RowParquetRecord, columnTypes: Map[String, ColumnDescriptor] = Map.empty): cypher.Value =
    ParquetRecord.foldable.fold(ParquetRecord(record, columnTypes), cypherValueFolder)

  private def asJson(record: RowParquetRecord, columnTypes: Map[String, ColumnDescriptor] = Map.empty): Json =
    ParquetRecord.foldable.fold(ParquetRecord(record, columnTypes), DataFolderTo[Json])

  private def cti(
    physical: PrimitiveTypeName,
    logical: Option[LogicalTypeAnnotation] = None,
  ): ColumnDescriptor =
    ColumnDescriptor(physical, logical)

  private def mkMap(entries: (PqValue, PqValue)*): MapParquetRecord = MapParquetRecord(entries: _*)
  private def mkList(values: PqValue*): ListParquetRecord = ListParquetRecord(values: _*)

  // ---------------------------------------------------------------------------
  // Schema-driven dispatch through the Cypher folder
  // ---------------------------------------------------------------------------

  describe("Cypher folder: schema-driven dispatch") {

    describe("BOOLEAN") {
      it("true folds to Expr.True") {
        asCypher(
          RowParquetRecord(Seq("flag" -> BooleanValue(true))),
          Map("flag" -> cti(PrimitiveTypeName.BOOLEAN)),
        ) shouldEqual ce.Map("flag" -> ce.True)
      }
      it("false folds to Expr.False") {
        asCypher(
          RowParquetRecord(Seq("flag" -> BooleanValue(false))),
          Map("flag" -> cti(PrimitiveTypeName.BOOLEAN)),
        ) shouldEqual ce.Map("flag" -> ce.False)
      }
    }

    describe("FLOAT / DOUBLE") {
      it("FLOAT folds to Expr.Floating (widened to Double)") {
        asCypher(
          RowParquetRecord(Seq("f" -> FloatValue(3.14F))),
          Map("f" -> cti(PrimitiveTypeName.FLOAT)),
        ) shouldEqual ce.Map("f" -> ce.Floating(3.14F.toDouble))
      }
      it("DOUBLE folds to Expr.Floating") {
        asCypher(
          RowParquetRecord(Seq("d" -> DoubleValue(2.718))),
          Map("d" -> cti(PrimitiveTypeName.DOUBLE)),
        ) shouldEqual ce.Map("d" -> ce.Floating(2.718))
      }
    }

    describe("INT32") {
      it("plain INT32 folds to Expr.Integer (widened to Long)") {
        asCypher(
          RowParquetRecord(Seq("i" -> IntValue(42))),
          Map("i" -> cti(PrimitiveTypeName.INT32)),
        ) shouldEqual ce.Map("i" -> ce.Integer(42L))
      }
      it("Int.MaxValue / Int.MinValue preserved exactly") {
        asCypher(
          RowParquetRecord(Seq("a" -> IntValue(Int.MaxValue), "b" -> IntValue(Int.MinValue))),
          Map("a" -> cti(PrimitiveTypeName.INT32), "b" -> cti(PrimitiveTypeName.INT32)),
        ) shouldEqual ce.Map("a" -> ce.Integer(Int.MaxValue.toLong), "b" -> ce.Integer(Int.MinValue.toLong))
      }
      it("INT32 + DATE folds to Expr.Date — not a string") {
        asCypher(
          RowParquetRecord(Seq("dt" -> IntValue(19737))),
          Map("dt" -> cti(PrimitiveTypeName.INT32, Some(dateType()))),
        ) shouldEqual ce.Map("dt" -> ce.Date(LocalDate.of(2024, 1, 15)))
      }
      it("INT32 + DATE day 0 is the Unix epoch") {
        asCypher(
          RowParquetRecord(Seq("dt" -> IntValue(0))),
          Map("dt" -> cti(PrimitiveTypeName.INT32, Some(dateType()))),
        ) shouldEqual ce.Map("dt" -> ce.Date(LocalDate.of(1970, 1, 1)))
      }
      it("INT32 + TIME(MILLIS) folds to Expr.LocalTime") {
        asCypher(
          RowParquetRecord(Seq("t" -> IntValue(43200000))),
          Map("t" -> cti(PrimitiveTypeName.INT32, Some(timeType(true, TimeUnit.MILLIS)))),
        ) shouldEqual ce.Map("t" -> ce.LocalTime(LocalTime.NOON))
      }
      it("INT32 + INT(8, signed) folds to Expr.Integer") {
        asCypher(
          RowParquetRecord(Seq("x" -> IntValue(127))),
          Map("x" -> cti(PrimitiveTypeName.INT32, Some(intType(8, true)))),
        ) shouldEqual ce.Map("x" -> ce.Integer(127L))
      }
    }

    describe("INT64") {
      it("plain INT64 folds to Expr.Integer at full Long precision") {
        asCypher(
          RowParquetRecord(Seq("l" -> LongValue(9876543210L))),
          Map("l" -> cti(PrimitiveTypeName.INT64)),
        ) shouldEqual ce.Map("l" -> ce.Integer(9876543210L))
      }
      it("Long.MaxValue / Long.MinValue preserved exactly — JSON would degrade to string at this magnitude") {
        asCypher(
          RowParquetRecord(Seq("hi" -> LongValue(Long.MaxValue), "lo" -> LongValue(Long.MinValue))),
          Map("hi" -> cti(PrimitiveTypeName.INT64), "lo" -> cti(PrimitiveTypeName.INT64)),
        ) shouldEqual ce.Map("hi" -> ce.Integer(Long.MaxValue), "lo" -> ce.Integer(Long.MinValue))
      }
      it("INT64 + TIMESTAMP(MILLIS, UTC) folds to Expr.DateTime") {
        val millis = 1705315200000L
        asCypher(
          RowParquetRecord(Seq("ts" -> LongValue(millis))),
          Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.MILLIS)))),
        ) shouldEqual ce.Map(
          "ts" -> ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 0, ZoneOffset.UTC)),
        )
      }
      it("INT64 + TIMESTAMP(MICROS, UTC) preserves sub-millisecond precision") {
        val micros = 1705315200000000L + 123456L // +123.456 ms
        asCypher(
          RowParquetRecord(Seq("ts" -> LongValue(micros))),
          Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.MICROS)))),
        ) shouldEqual ce.Map(
          "ts" -> ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456000, ZoneOffset.UTC)),
        )
      }
      it("INT64 + TIMESTAMP(NANOS, UTC) preserves nanosecond precision") {
        val nanos = 1705315200000000000L + 123456789L
        asCypher(
          RowParquetRecord(Seq("ts" -> LongValue(nanos))),
          Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.NANOS)))),
        ) shouldEqual ce.Map(
          "ts" -> ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456789, ZoneOffset.UTC)),
        )
      }
      it("INT64 + TIMESTAMP without UTC adjustment folds to Expr.LocalDateTime, not Expr.DateTime") {
        val millis = 1705315200000L
        asCypher(
          RowParquetRecord(Seq("ts" -> LongValue(millis))),
          Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(false, TimeUnit.MILLIS)))),
        ) shouldEqual ce.Map("ts" -> ce.LocalDateTime(LocalDateTime.of(2024, 1, 15, 10, 40, 0)))
      }
      it("INT64 + TIME(MICROS) folds to Expr.LocalTime") {
        val micros = 43200000000L
        asCypher(
          RowParquetRecord(Seq("t" -> LongValue(micros))),
          Map("t" -> cti(PrimitiveTypeName.INT64, Some(timeType(true, TimeUnit.MICROS)))),
        ) shouldEqual ce.Map("t" -> ce.LocalTime(LocalTime.NOON))
      }
    }

    describe("INT96") {
      it("folds to Expr.LocalDateTime with nanosecond precision") {
        val julianDay = 2460324 // 2024-01-14
        val nanosOfDay = 43200000000000L + 123456789L
        val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putLong(nanosOfDay); buf.putInt(julianDay)
        asCypher(
          RowParquetRecord(Seq("ts" -> BinaryValue(buf.array()))),
          Map("ts" -> cti(PrimitiveTypeName.INT96)),
        ) shouldEqual ce.Map(
          "ts" -> ce.LocalDateTime(LocalDateTime.of(2024, 1, 14, 12, 0, 0, 123456789)),
        )
      }
    }

    describe("BINARY") {
      it("BINARY + STRING folds to Expr.Str") {
        asCypher(
          RowParquetRecord(Seq("s" -> BinaryValue("hello world"))),
          Map("s" -> cti(PrimitiveTypeName.BINARY, Some(stringType()))),
        ) shouldEqual ce.Map("s" -> ce.Str("hello world"))
      }
      it("BINARY + STRING with multi-byte UTF-8") {
        val text = "héllo wörld 日本語"
        asCypher(
          RowParquetRecord(Seq("s" -> BinaryValue(text))),
          Map("s" -> cti(PrimitiveTypeName.BINARY, Some(stringType()))),
        ) shouldEqual ce.Map("s" -> ce.Str(text))
      }
      it("BINARY + ENUM folds to Expr.Str") {
        asCypher(
          RowParquetRecord(Seq("e" -> BinaryValue("RED"))),
          Map("e" -> cti(PrimitiveTypeName.BINARY, Some(enumType()))),
        ) shouldEqual ce.Map("e" -> ce.Str("RED"))
      }
      it("BINARY + JSON (valid) parses and re-folds through the destination — typed all the way down") {
        val jsonStr = """{"nested":true,"n":7}"""
        asCypher(
          RowParquetRecord(Seq("j" -> BinaryValue(jsonStr))),
          Map("j" -> cti(PrimitiveTypeName.BINARY, Some(jsonType()))),
        ) shouldEqual ce.Map(
          "j" -> ce.Map("nested" -> ce.True, "n" -> ce.Integer(7L)),
        )
      }
      it("BINARY + JSON (invalid) falls back to Expr.Str") {
        val bad = "not valid json {"
        asCypher(
          RowParquetRecord(Seq("j" -> BinaryValue(bad))),
          Map("j" -> cti(PrimitiveTypeName.BINARY, Some(jsonType()))),
        ) shouldEqual ce.Map("j" -> ce.Str(bad))
      }
      it("BINARY + BSON folds to Expr.Bytes preserving raw byte identity") {
        val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04)
        asCypher(
          RowParquetRecord(Seq("b" -> BinaryValue(bytes))),
          Map("b" -> cti(PrimitiveTypeName.BINARY, Some(bsonType()))),
        ) shouldEqual ce.Map("b" -> ce.Bytes(bytes))
      }
      it("unannotated BINARY folds to Expr.Bytes — disambiguated to bytes, not string") {
        val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)
        asCypher(
          RowParquetRecord(Seq("raw" -> BinaryValue(bytes))),
          Map("raw" -> cti(PrimitiveTypeName.BINARY, None)),
        ) shouldEqual ce.Map("raw" -> ce.Bytes(bytes))
      }
    }

    describe("FIXED_LEN_BYTE_ARRAY") {
      it("UUID folds to Expr.Str") {
        val uuid = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
        val buf = java.nio.ByteBuffer.allocate(16)
        buf.putLong(uuid.getMostSignificantBits); buf.putLong(uuid.getLeastSignificantBits)
        asCypher(
          RowParquetRecord(Seq("id" -> BinaryValue(buf.array()))),
          Map("id" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(uuidType()))),
        ) shouldEqual ce.Map("id" -> ce.Str("550e8400-e29b-41d4-a716-446655440000"))
      }
      it("FLOAT16 folds to Expr.Floating") {
        val buf = java.nio.ByteBuffer.allocate(2).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putShort(0x3C00.toShort) // 1.0 in half-precision
        asCypher(
          RowParquetRecord(Seq("h" -> BinaryValue(buf.array()))),
          Map("h" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(float16Type()))),
        ) shouldEqual ce.Map("h" -> ce.Floating(1.0))
      }
      it("INTERVAL folds to a typed Expr.Map of Expr.Integer components — not stringified") {
        val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putInt(14); buf.putInt(3); buf.putInt(500)
        asCypher(
          RowParquetRecord(Seq("iv" -> BinaryValue(buf.array()))),
          Map("iv" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(IntervalLogicalTypeAnnotation.getInstance()))),
        ) shouldEqual ce.Map(
          "iv" -> ce.Map(
            "months" -> ce.Integer(14L),
            "days" -> ce.Integer(3L),
            "millis" -> ce.Integer(500L),
          ),
        )
      }
      it("unannotated FIXED_LEN_BYTE_ARRAY folds to Expr.Bytes") {
        val bytes = Array[Byte](0x01, 0x02, 0x03)
        asCypher(
          RowParquetRecord(Seq("fixed" -> BinaryValue(bytes))),
          Map("fixed" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, None)),
        ) shouldEqual ce.Map("fixed" -> ce.Bytes(bytes))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Schema-less fallback (nested values inside LIST/MAP, or missing schema)
  // ---------------------------------------------------------------------------

  describe("Cypher folder: schema-less fallback") {
    it("NullValue folds to Expr.Null") {
      asCypher(RowParquetRecord(Seq("x" -> NullValue))) shouldEqual ce.Map("x" -> ce.Null)
    }
    it("IntValue folds to Expr.Integer") {
      asCypher(RowParquetRecord(Seq("x" -> IntValue(99)))) shouldEqual ce.Map("x" -> ce.Integer(99L))
    }
    it("LongValue folds to Expr.Integer at full precision") {
      asCypher(RowParquetRecord(Seq("x" -> LongValue(123456789L)))) shouldEqual
      ce.Map("x" -> ce.Integer(123456789L))
    }
    it("BooleanValue folds to Expr.True / Expr.False") {
      asCypher(RowParquetRecord(Seq("a" -> BooleanValue(true), "b" -> BooleanValue(false)))) shouldEqual
      ce.Map("a" -> ce.True, "b" -> ce.False)
    }
    it("BinaryValue folds to Expr.Bytes even when contents happen to be valid UTF-8") {
      // Without schema info, BINARY is ambiguous between STRING and raw bytes; the
      // foldable resolves this to bytes (consistent typed identity, no silent
      // re-interpretation as text).
      asCypher(RowParquetRecord(Seq("x" -> BinaryValue("hello")))) shouldEqual
      ce.Map("x" -> ce.Bytes("hello".getBytes("UTF-8")))
    }
    it("BinaryValue of arbitrary bytes folds to Expr.Bytes") {
      val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte)
      asCypher(RowParquetRecord(Seq("x" -> BinaryValue(bytes)))) shouldEqual ce.Map("x" -> ce.Bytes(bytes))
    }
    it("ListParquetRecord folds to Expr.List with typed elements") {
      val list = mkList(IntValue(1), IntValue(2), IntValue(3))
      asCypher(RowParquetRecord(Seq("arr" -> list))) shouldEqual
      ce.Map("arr" -> ce.List(ce.Integer(1L), ce.Integer(2L), ce.Integer(3L)))
    }
    it("MapParquetRecord with string keys folds to Expr.Map") {
      val map = mkMap(BinaryValue("key1") -> IntValue(10), BinaryValue("key2") -> IntValue(20))
      asCypher(RowParquetRecord(Seq("m" -> map))) shouldEqual
      ce.Map("m" -> ce.Map("key1" -> ce.Integer(10L), "key2" -> ce.Integer(20L)))
    }
    it("nested RowParquetRecord (struct) folds to Expr.Map") {
      val inner = RowParquetRecord(Seq("a" -> IntValue(1), "b" -> BooleanValue(true)))
      asCypher(RowParquetRecord(Seq("nested" -> inner))) shouldEqual
      ce.Map("nested" -> ce.Map("a" -> ce.Integer(1L), "b" -> ce.True))
    }
    it("integer MAP keys are stringified") {
      val map = mkMap(IntValue(42) -> BinaryValue("value"))
      asCypher(RowParquetRecord(Seq("m" -> map))) shouldEqual
      ce.Map("m" -> ce.Map("42" -> ce.Bytes("value".getBytes("UTF-8"))))
    }
  }

  // ---------------------------------------------------------------------------
  // Multi-column / mixed-schema records
  // ---------------------------------------------------------------------------

  describe("multi-column records") {
    it("mixed-schema row folds with each column dispatched independently") {
      asCypher(
        RowParquetRecord(
          Seq(
            "name" -> BinaryValue("Alice"),
            "age" -> IntValue(30),
            "active" -> BooleanValue(true),
            "score" -> DoubleValue(95.5),
            "extra" -> NullValue,
          ),
        ),
        Map(
          "name" -> cti(PrimitiveTypeName.BINARY, Some(stringType())),
          "age" -> cti(PrimitiveTypeName.INT32),
          "active" -> cti(PrimitiveTypeName.BOOLEAN),
          "score" -> cti(PrimitiveTypeName.DOUBLE),
        ),
      ) shouldEqual ce.Map(
        "name" -> ce.Str("Alice"),
        "age" -> ce.Integer(30L),
        "active" -> ce.True,
        "score" -> ce.Floating(95.5),
        "extra" -> ce.Null,
      )
    }
    it("columns missing from the type map fall back to schema-less dispatch") {
      asCypher(RowParquetRecord(Seq("unknown" -> IntValue(7)))) shouldEqual ce.Map("unknown" -> ce.Integer(7L))
    }
  }

  // ---------------------------------------------------------------------------
  // Malformed UTF-8 in MAP keys — assertion is on the throw, independent of folder
  // ---------------------------------------------------------------------------

  describe("malformed UTF-8 in MAP keys") {
    it("invalid UTF-8 bytes throw MalformedInputException") {
      val map = mkMap(BinaryValue(Array[Byte](0xFF.toByte, 0xFE.toByte)) -> IntValue(1))
      a[MalformedInputException] should be thrownBy asCypher(RowParquetRecord(Seq("m" -> map)))
    }
    it("truncated multi-byte UTF-8 throws MalformedInputException") {
      val map = mkMap(BinaryValue(Array[Byte](0xC0.toByte)) -> IntValue(1))
      a[MalformedInputException] should be thrownBy asCypher(RowParquetRecord(Seq("m" -> map)))
    }
    it("overlong UTF-8 encoding throws MalformedInputException") {
      val map = mkMap(BinaryValue(Array[Byte](0xC0.toByte, 0xAF.toByte)) -> IntValue(1))
      a[MalformedInputException] should be thrownBy asCypher(RowParquetRecord(Seq("m" -> map)))
    }
    it("valid UTF-8 keys (including multi-byte) round-trip") {
      val map = mkMap(BinaryValue("valid_key") -> IntValue(1), BinaryValue("日本語") -> IntValue(2))
      asCypher(RowParquetRecord(Seq("m" -> map))) shouldEqual
      ce.Map("m" -> ce.Map("valid_key" -> ce.Integer(1L), "日本語" -> ce.Integer(2L)))
    }
  }

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  describe("edge cases") {
    it("empty RowParquetRecord folds to empty Expr.Map") {
      asCypher(RowParquetRecord(Seq.empty)) shouldEqual ce.Map()
    }
    it("NullValue overrides schema info") {
      asCypher(
        RowParquetRecord(Seq("x" -> NullValue)),
        Map("x" -> cti(PrimitiveTypeName.INT32)),
      ) shouldEqual ce.Map("x" -> ce.Null)
    }
    it("empty BINARY + STRING folds to empty Expr.Str") {
      asCypher(
        RowParquetRecord(Seq("s" -> BinaryValue(""))),
        Map("s" -> cti(PrimitiveTypeName.BINARY, Some(stringType()))),
      ) shouldEqual ce.Map("s" -> ce.Str(""))
    }
    it("empty unannotated BINARY folds to empty Expr.Bytes") {
      asCypher(
        RowParquetRecord(Seq("b" -> BinaryValue(Array.empty[Byte]))),
        Map("b" -> cti(PrimitiveTypeName.BINARY, None)),
      ) shouldEqual ce.Map("b" -> ce.Bytes(Array.empty[Byte]))
    }
    it("deeply nested LIST of MAP preserves type identity at every level") {
      val innerMap = mkMap(BinaryValue("k") -> IntValue(1))
      val list = mkList(innerMap)
      asCypher(RowParquetRecord(Seq("deep" -> list))) shouldEqual
      ce.Map("deep" -> ce.List(ce.Map("k" -> ce.Integer(1L))))
    }
  }

  // ---------------------------------------------------------------------------
  // JSON folder: assertions that are properties of the JSON folder itself
  // (NaN→string, bytes→hex, temporals→ISO-8601), not of the Parquet decoder
  // ---------------------------------------------------------------------------

  describe("JSON folder: JSON-specific serialization") {
    it("non-finite Double serializes as a JSON string (Json.fromDoubleOrString)") {
      asJson(
        RowParquetRecord(Seq("d" -> DoubleValue(Double.NaN))),
        Map("d" -> cti(PrimitiveTypeName.DOUBLE)),
      ) shouldEqual Json.obj("d" -> Json.fromString("NaN"))
    }
    it("non-finite Float serializes as a JSON string") {
      asJson(
        RowParquetRecord(Seq("f" -> FloatValue(Float.NaN))),
        Map("f" -> cti(PrimitiveTypeName.FLOAT)),
      ) shouldEqual Json.obj("f" -> Json.fromString("NaN"))
    }
    it("bytes serialize as standard base64") {
      val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)
      asJson(
        RowParquetRecord(Seq("raw" -> BinaryValue(bytes))),
        Map("raw" -> cti(PrimitiveTypeName.BINARY, None)),
      ) shouldEqual Json.obj("raw" -> Json.fromString("AP+Afw=="))
    }
    it("schema-less BinaryValue with UTF-8 content still serializes as base64, not text") {
      asJson(RowParquetRecord(Seq("x" -> BinaryValue("hello")))) shouldEqual
      Json.obj("x" -> Json.fromString("aGVsbG8="))
    }
    it("Date serializes as ISO-8601 string") {
      asJson(
        RowParquetRecord(Seq("dt" -> IntValue(19737))),
        Map("dt" -> cti(PrimitiveTypeName.INT32, Some(dateType()))),
      ) shouldEqual Json.obj("dt" -> Json.fromString("2024-01-15"))
    }
    it("ZonedDateTime serializes as ISO-8601 string") {
      asJson(
        RowParquetRecord(Seq("ts" -> LongValue(1705315200000L))),
        Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.MILLIS)))),
      ) shouldEqual Json.obj("ts" -> Json.fromString("2024-01-15T10:40:00Z"))
    }
  }
}

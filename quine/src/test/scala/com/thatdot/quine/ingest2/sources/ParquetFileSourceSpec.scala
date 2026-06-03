package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.MalformedInputException
import java.util.Base64

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
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.model.ingest2.sources.ParquetFileSource.ColumnTypeInfo

class ParquetFileSourceSpec extends AnyFunSpec with Matchers {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def convert(record: RowParquetRecord, columnTypes: Map[String, ColumnTypeInfo] = Map.empty): Json =
    ParquetFileSource.recordToJson(record, columnTypes)

  private def cti(
    physical: PrimitiveTypeName,
    logical: Option[org.apache.parquet.schema.LogicalTypeAnnotation] = None,
  ) =
    ColumnTypeInfo(physical, logical)

  /** Helper to build MapParquetRecord from key-value pairs. */
  private def mkMap(entries: (PqValue, PqValue)*): MapParquetRecord =
    MapParquetRecord(entries: _*)

  /** Helper to build ListParquetRecord from values. */
  private def mkList(values: PqValue*): ListParquetRecord =
    ListParquetRecord(values: _*)

  // ---------------------------------------------------------------------------
  // Schema-driven conversion: primitive types
  // ---------------------------------------------------------------------------

  describe("convertWithSchema") {

    describe("BOOLEAN") {
      it("converts BooleanValue to JSON boolean") {
        val record = RowParquetRecord(Seq("flag" -> BooleanValue(true)))
        val types = Map("flag" -> cti(PrimitiveTypeName.BOOLEAN))
        convert(record, types) shouldEqual Json.obj("flag" -> Json.True)
      }

      it("converts false BooleanValue") {
        val record = RowParquetRecord(Seq("flag" -> BooleanValue(false)))
        val types = Map("flag" -> cti(PrimitiveTypeName.BOOLEAN))
        convert(record, types) shouldEqual Json.obj("flag" -> Json.False)
      }
    }

    describe("FLOAT") {
      it("converts FloatValue to JSON number") {
        val record = RowParquetRecord(Seq("f" -> FloatValue(3.14F)))
        val types = Map("f" -> cti(PrimitiveTypeName.FLOAT))
        val result = convert(record, types)
        val num = result.hcursor.downField("f").as[Float].toOption.get
        num shouldEqual 3.14F
      }

      it("handles NaN") {
        val record = RowParquetRecord(Seq("f" -> FloatValue(Float.NaN)))
        val types = Map("f" -> cti(PrimitiveTypeName.FLOAT))
        val result = convert(record, types)
        val numStr = result.hcursor.downField("f").focus.flatMap(_.asNumber).map(_.toString).get
        numStr shouldEqual "NaN"
      }
    }

    describe("DOUBLE") {
      it("converts DoubleValue to JSON number") {
        val record = RowParquetRecord(Seq("d" -> DoubleValue(2.718)))
        val types = Map("d" -> cti(PrimitiveTypeName.DOUBLE))
        val result = convert(record, types)
        result.hcursor.downField("d").as[Double].toOption.get shouldEqual 2.718
      }

      it("handles Double.NaN as null") {
        val record = RowParquetRecord(Seq("d" -> DoubleValue(Double.NaN)))
        val types = Map("d" -> cti(PrimitiveTypeName.DOUBLE))
        convert(record, types) shouldEqual Json.obj("d" -> Json.Null)
      }
    }

    describe("INT32") {
      it("converts plain INT32 to JSON int") {
        val record = RowParquetRecord(Seq("i" -> IntValue(42)))
        val types = Map("i" -> cti(PrimitiveTypeName.INT32))
        convert(record, types) shouldEqual Json.obj("i" -> Json.fromInt(42))
      }

      it("converts negative INT32") {
        val record = RowParquetRecord(Seq("i" -> IntValue(-100)))
        val types = Map("i" -> cti(PrimitiveTypeName.INT32))
        convert(record, types) shouldEqual Json.obj("i" -> Json.fromInt(-100))
      }

      it("converts INT32 + DATE to ISO-8601 string") {
        val record = RowParquetRecord(Seq("dt" -> IntValue(19737)))
        val types = Map("dt" -> cti(PrimitiveTypeName.INT32, Some(dateType())))
        convert(record, types) shouldEqual Json.obj("dt" -> Json.fromString("2024-01-15"))
      }

      it("converts INT32 + DATE for Unix epoch (day 0)") {
        val record = RowParquetRecord(Seq("dt" -> IntValue(0)))
        val types = Map("dt" -> cti(PrimitiveTypeName.INT32, Some(dateType())))
        convert(record, types) shouldEqual Json.obj("dt" -> Json.fromString("1970-01-01"))
      }

      it("converts INT32 + TIME(MILLIS)") {
        val record = RowParquetRecord(Seq("t" -> IntValue(43200000)))
        val types = Map("t" -> cti(PrimitiveTypeName.INT32, Some(timeType(true, TimeUnit.MILLIS))))
        convert(record, types) shouldEqual Json.obj("t" -> Json.fromInt(43200000))
      }

      it("converts INT32 + INT annotation") {
        val record = RowParquetRecord(Seq("x" -> IntValue(127)))
        val types = Map("x" -> cti(PrimitiveTypeName.INT32, Some(intType(8, true))))
        convert(record, types) shouldEqual Json.obj("x" -> Json.fromInt(127))
      }
    }

    describe("INT64") {
      it("converts plain INT64 to JSON long") {
        val record = RowParquetRecord(Seq("l" -> LongValue(9876543210L)))
        val types = Map("l" -> cti(PrimitiveTypeName.INT64))
        convert(record, types) shouldEqual Json.obj("l" -> Json.fromLong(9876543210L))
      }

      it("converts INT64 + TIMESTAMP(MILLIS)") {
        val millis = 1705315200000L
        val record = RowParquetRecord(Seq("ts" -> LongValue(millis)))
        val types = Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.MILLIS))))
        convert(record, types) shouldEqual Json.obj("ts" -> Json.fromLong(millis))
      }

      it("converts INT64 + TIMESTAMP(MICROS) to millis") {
        val micros = 1705315200000000L
        val record = RowParquetRecord(Seq("ts" -> LongValue(micros)))
        val types = Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.MICROS))))
        convert(record, types) shouldEqual Json.obj("ts" -> Json.fromLong(micros / 1000L))
      }

      it("converts INT64 + TIMESTAMP(NANOS) to millis") {
        val nanos = 1705315200000000000L
        val record = RowParquetRecord(Seq("ts" -> LongValue(nanos)))
        val types = Map("ts" -> cti(PrimitiveTypeName.INT64, Some(timestampType(true, TimeUnit.NANOS))))
        convert(record, types) shouldEqual Json.obj("ts" -> Json.fromLong(nanos / 1000000L))
      }

      it("converts INT64 + TIME(MICROS)") {
        val micros = 43200000000L
        val record = RowParquetRecord(Seq("t" -> LongValue(micros)))
        val types = Map("t" -> cti(PrimitiveTypeName.INT64, Some(timeType(true, TimeUnit.MICROS))))
        convert(record, types) shouldEqual Json.obj("t" -> Json.fromLong(micros))
      }
    }

    describe("INT96") {
      it("converts INT96 timestamp to epoch millis") {
        val julianDay = 2460324
        val nanosOfDay = 43200000000000L
        val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putLong(nanosOfDay)
        buf.putInt(julianDay)
        val record = RowParquetRecord(Seq("ts" -> BinaryValue(buf.array())))
        val types = Map("ts" -> cti(PrimitiveTypeName.INT96))
        val result = convert(record, types)
        val millis = result.hcursor.downField("ts").as[Long].toOption.get
        val expected = (julianDay.toLong - 2440588L) * 86400000L + nanosOfDay / 1000000L
        millis shouldEqual expected
      }
    }

    describe("BINARY") {
      it("converts BINARY + STRING to UTF-8 string") {
        val record = RowParquetRecord(Seq("s" -> BinaryValue("hello world")))
        val types = Map("s" -> cti(PrimitiveTypeName.BINARY, Some(stringType())))
        convert(record, types) shouldEqual Json.obj("s" -> Json.fromString("hello world"))
      }

      it("converts BINARY + STRING with unicode") {
        val text = "héllo wörld 日本語"
        val record = RowParquetRecord(Seq("s" -> BinaryValue(text)))
        val types = Map("s" -> cti(PrimitiveTypeName.BINARY, Some(stringType())))
        convert(record, types) shouldEqual Json.obj("s" -> Json.fromString(text))
      }

      it("converts BINARY + ENUM to string") {
        val record = RowParquetRecord(Seq("e" -> BinaryValue("RED")))
        val types = Map("e" -> cti(PrimitiveTypeName.BINARY, Some(enumType())))
        convert(record, types) shouldEqual Json.obj("e" -> Json.fromString("RED"))
      }

      it("converts BINARY + JSON — parses valid JSON") {
        val jsonStr = """{"nested":true}"""
        val record = RowParquetRecord(Seq("j" -> BinaryValue(jsonStr)))
        val types = Map("j" -> cti(PrimitiveTypeName.BINARY, Some(jsonType())))
        val result = convert(record, types)
        result.hcursor.downField("j").downField("nested").as[Boolean].toOption.get shouldEqual true
      }

      it("converts BINARY + JSON — falls back to string for invalid JSON") {
        val badJson = "not valid json {"
        val record = RowParquetRecord(Seq("j" -> BinaryValue(badJson)))
        val types = Map("j" -> cti(PrimitiveTypeName.BINARY, Some(jsonType())))
        convert(record, types) shouldEqual Json.obj("j" -> Json.fromString(badJson))
      }

      it("converts BINARY + BSON to base64") {
        val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04)
        val record = RowParquetRecord(Seq("b" -> BinaryValue(bytes)))
        val types = Map("b" -> cti(PrimitiveTypeName.BINARY, Some(bsonType())))
        val expected = Base64.getEncoder.encodeToString(bytes)
        convert(record, types) shouldEqual Json.obj("b" -> Json.fromString(expected))
      }

      it("converts unannotated BINARY to base64") {
        val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)
        val record = RowParquetRecord(Seq("raw" -> BinaryValue(bytes)))
        val types = Map("raw" -> cti(PrimitiveTypeName.BINARY, None))
        val expected = Base64.getEncoder.encodeToString(bytes)
        convert(record, types) shouldEqual Json.obj("raw" -> Json.fromString(expected))
      }
    }

    describe("FIXED_LEN_BYTE_ARRAY") {
      it("converts FIXED_LEN_BYTE_ARRAY + UUID") {
        val uuid = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
        val buf = java.nio.ByteBuffer.allocate(16)
        buf.putLong(uuid.getMostSignificantBits)
        buf.putLong(uuid.getLeastSignificantBits)
        val record = RowParquetRecord(Seq("id" -> BinaryValue(buf.array())))
        val types = Map("id" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(uuidType())))
        convert(record, types) shouldEqual Json.obj("id" -> Json.fromString("550e8400-e29b-41d4-a716-446655440000"))
      }

      it("converts FIXED_LEN_BYTE_ARRAY + FLOAT16") {
        val buf = java.nio.ByteBuffer.allocate(2).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putShort(0x3C00.toShort) // 1.0 in half-precision
        val record = RowParquetRecord(Seq("h" -> BinaryValue(buf.array())))
        val types = Map("h" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(float16Type())))
        val result = convert(record, types)
        result.hcursor.downField("h").as[Double].toOption.get shouldEqual 1.0
      }

      it("converts FIXED_LEN_BYTE_ARRAY + FLOAT16 for negative zero") {
        val buf = java.nio.ByteBuffer.allocate(2).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putShort(0x8000.toShort) // -0.0 in half
        val record = RowParquetRecord(Seq("h" -> BinaryValue(buf.array())))
        val types = Map("h" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(float16Type())))
        val result = convert(record, types)
        result.hcursor.downField("h").as[Double].toOption.get shouldEqual -0.0
      }

      it("converts FIXED_LEN_BYTE_ARRAY + INTERVAL") {
        val buf = java.nio.ByteBuffer.allocate(12).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        buf.putInt(14) // months
        buf.putInt(3) // days
        buf.putInt(500) // millis
        val record = RowParquetRecord(Seq("iv" -> BinaryValue(buf.array())))
        val types =
          Map("iv" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(IntervalLogicalTypeAnnotation.getInstance())))
        convert(record, types) shouldEqual Json.obj(
          "iv" -> Json.obj("months" -> Json.fromInt(14), "days" -> Json.fromInt(3), "millis" -> Json.fromInt(500)),
        )
      }

      it("converts unannotated FIXED_LEN_BYTE_ARRAY to base64") {
        val bytes = Array[Byte](0x01, 0x02, 0x03)
        val record = RowParquetRecord(Seq("fixed" -> BinaryValue(bytes)))
        val types = Map("fixed" -> cti(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, None))
        val expected = Base64.getEncoder.encodeToString(bytes)
        convert(record, types) shouldEqual Json.obj("fixed" -> Json.fromString(expected))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Schema-less fallback conversion
  // ---------------------------------------------------------------------------

  describe("convertWithoutSchema") {

    it("converts NullValue to Json.Null") {
      val record = RowParquetRecord(Seq("x" -> NullValue))
      convert(record) shouldEqual Json.obj("x" -> Json.Null)
    }

    it("converts IntValue") {
      convert(RowParquetRecord(Seq("x" -> IntValue(99)))) shouldEqual Json.obj("x" -> Json.fromInt(99))
    }

    it("converts LongValue") {
      convert(RowParquetRecord(Seq("x" -> LongValue(123456789L)))) shouldEqual Json.obj(
        "x" -> Json.fromLong(123456789L),
      )
    }

    it("converts BooleanValue") {
      convert(RowParquetRecord(Seq("x" -> BooleanValue(true)))) shouldEqual Json.obj("x" -> Json.True)
    }

    it("converts BinaryValue to base64 (not UTF-8 string)") {
      val bytes = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte)
      val expected = Base64.getEncoder.encodeToString(bytes)
      convert(RowParquetRecord(Seq("x" -> BinaryValue(bytes)))) shouldEqual Json.obj("x" -> Json.fromString(expected))
    }

    it("converts BinaryValue containing valid UTF-8 to base64 (consistent treatment)") {
      val expected = Base64.getEncoder.encodeToString("hello".getBytes("UTF-8"))
      convert(RowParquetRecord(Seq("x" -> BinaryValue("hello")))) shouldEqual Json.obj("x" -> Json.fromString(expected))
    }

    it("converts nested ListParquetRecord") {
      val list = mkList(IntValue(1), IntValue(2), IntValue(3))
      convert(RowParquetRecord(Seq("arr" -> list))) shouldEqual Json.obj(
        "arr" -> Json.arr(Json.fromInt(1), Json.fromInt(2), Json.fromInt(3)),
      )
    }

    it("converts nested MapParquetRecord with string keys") {
      val map = mkMap(BinaryValue("key1") -> IntValue(10), BinaryValue("key2") -> IntValue(20))
      val result = convert(RowParquetRecord(Seq("m" -> map)))
      result.hcursor.downField("m").downField("key1").as[Int].toOption.get shouldEqual 10
      result.hcursor.downField("m").downField("key2").as[Int].toOption.get shouldEqual 20
    }

    it("converts nested RowParquetRecord (struct)") {
      val inner = RowParquetRecord(Seq("a" -> IntValue(1), "b" -> BooleanValue(true)))
      convert(RowParquetRecord(Seq("nested" -> inner))) shouldEqual Json.obj(
        "nested" -> Json.obj("a" -> Json.fromInt(1), "b" -> Json.True),
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Multi-column records
  // ---------------------------------------------------------------------------

  describe("recordToJson") {
    it("converts a record with multiple columns and mixed schema") {
      val record = RowParquetRecord(
        Seq(
          "name" -> BinaryValue("Alice"),
          "age" -> IntValue(30),
          "active" -> BooleanValue(true),
          "score" -> DoubleValue(95.5),
          "extra" -> NullValue,
        ),
      )
      val types = Map(
        "name" -> cti(PrimitiveTypeName.BINARY, Some(stringType())),
        "age" -> cti(PrimitiveTypeName.INT32),
        "active" -> cti(PrimitiveTypeName.BOOLEAN),
        "score" -> cti(PrimitiveTypeName.DOUBLE),
      )
      val result = convert(record, types)
      result.hcursor.downField("name").as[String].toOption.get shouldEqual "Alice"
      result.hcursor.downField("age").as[Int].toOption.get shouldEqual 30
      result.hcursor.downField("active").as[Boolean].toOption.get shouldEqual true
      result.hcursor.downField("score").as[Double].toOption.get shouldEqual 95.5
      result.hcursor.downField("extra").focus.get shouldEqual Json.Null
    }

    it("falls back to schema-less conversion for columns not in the type map") {
      convert(RowParquetRecord(Seq("unknown" -> IntValue(7)))) shouldEqual Json.obj("unknown" -> Json.fromInt(7))
    }
  }

  // ---------------------------------------------------------------------------
  // Malformed input: strict UTF-8 validation for MAP keys
  // ---------------------------------------------------------------------------

  describe("malformed UTF-8 in MAP keys") {
    it("throws MalformedInputException for invalid UTF-8 bytes in MAP key") {
      // 0xFF is never valid in UTF-8
      val map = mkMap(BinaryValue(Array[Byte](0xFF.toByte, 0xFE.toByte)) -> IntValue(1))
      a[MalformedInputException] should be thrownBy convert(RowParquetRecord(Seq("m" -> map)))
    }

    it("throws MalformedInputException for truncated multi-byte UTF-8 sequence in MAP key") {
      // 0xC0 starts a 2-byte sequence but is alone
      val map = mkMap(BinaryValue(Array[Byte](0xC0.toByte)) -> IntValue(1))
      a[MalformedInputException] should be thrownBy convert(RowParquetRecord(Seq("m" -> map)))
    }

    it("throws MalformedInputException for overlong UTF-8 encoding in MAP key") {
      // Overlong encoding of '/' (U+002F): 0xC0 0xAF
      val map = mkMap(BinaryValue(Array[Byte](0xC0.toByte, 0xAF.toByte)) -> IntValue(1))
      a[MalformedInputException] should be thrownBy convert(RowParquetRecord(Seq("m" -> map)))
    }

    it("accepts valid UTF-8 MAP keys") {
      val map = mkMap(BinaryValue("valid_key") -> IntValue(1), BinaryValue("日本語") -> IntValue(2))
      val result = convert(RowParquetRecord(Seq("m" -> map)))
      result.hcursor.downField("m").downField("valid_key").as[Int].toOption.get shouldEqual 1
      result.hcursor.downField("m").downField("日本語").as[Int].toOption.get shouldEqual 2
    }

    it("accepts integer MAP keys") {
      val map = mkMap(IntValue(42) -> BinaryValue("value"))
      val result = convert(RowParquetRecord(Seq("m" -> map)))
      val expected = Base64.getEncoder.encodeToString("value".getBytes("UTF-8"))
      result.hcursor.downField("m").downField("42").as[String].toOption.get shouldEqual expected
    }
  }

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  describe("edge cases") {
    it("converts empty RowParquetRecord to empty JSON object") {
      convert(RowParquetRecord(Seq.empty)) shouldEqual Json.obj()
    }

    it("converts NullValue regardless of schema info") {
      val types = Map("x" -> cti(PrimitiveTypeName.INT32))
      convert(RowParquetRecord(Seq("x" -> NullValue)), types) shouldEqual Json.obj("x" -> Json.Null)
    }

    it("handles Int.MaxValue") {
      val types = Map("x" -> cti(PrimitiveTypeName.INT32))
      convert(RowParquetRecord(Seq("x" -> IntValue(Int.MaxValue))), types) shouldEqual Json.obj(
        "x" -> Json.fromInt(Int.MaxValue),
      )
    }

    it("handles Int.MinValue") {
      val types = Map("x" -> cti(PrimitiveTypeName.INT32))
      convert(RowParquetRecord(Seq("x" -> IntValue(Int.MinValue))), types) shouldEqual Json.obj(
        "x" -> Json.fromInt(Int.MinValue),
      )
    }

    it("handles Long.MaxValue") {
      val types = Map("x" -> cti(PrimitiveTypeName.INT64))
      convert(RowParquetRecord(Seq("x" -> LongValue(Long.MaxValue))), types) shouldEqual Json.obj(
        "x" -> Json.fromLong(Long.MaxValue),
      )
    }

    it("handles Long.MinValue") {
      val types = Map("x" -> cti(PrimitiveTypeName.INT64))
      convert(RowParquetRecord(Seq("x" -> LongValue(Long.MinValue))), types) shouldEqual Json.obj(
        "x" -> Json.fromLong(Long.MinValue),
      )
    }

    it("handles empty string BINARY + STRING") {
      val types = Map("s" -> cti(PrimitiveTypeName.BINARY, Some(stringType())))
      convert(RowParquetRecord(Seq("s" -> BinaryValue(""))), types) shouldEqual Json.obj("s" -> Json.fromString(""))
    }

    it("handles empty bytes unannotated BINARY") {
      val types = Map("b" -> cti(PrimitiveTypeName.BINARY, None))
      convert(RowParquetRecord(Seq("b" -> BinaryValue(Array.empty[Byte]))), types) shouldEqual Json.obj(
        "b" -> Json.fromString(""),
      )
    }

    it("deeply nested LIST of MAPs") {
      val innerMap = mkMap(BinaryValue("k") -> IntValue(1))
      val list = mkList(innerMap)
      val result = convert(RowParquetRecord(Seq("deep" -> list)))
      result.hcursor.downField("deep").downArray.downField("k").as[Int].toOption.get shouldEqual 1
    }
  }
}

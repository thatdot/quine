package com.thatdot.quine.ingest2.sources

import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.util.zip.CRC32

import io.circe.Json
import org.roaringbitmap.RoaringBitmap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder
import com.thatdot.quine.app.model.ingest2.codec.{ParquetRecord, ParquetValue}
import com.thatdot.quine.app.model.ingest2.sources.{DeltaKernelDVEntry, DeltaKernelFileEntry, DeltaKernelReader}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr => ce}
import com.thatdot.quine.ingest2.TestParquetWriter
import com.thatdot.quine.ingest2.TestParquetWriter.{Column, Int64, Utf8}

/** Tests [[DeltaKernelReader]], including that Delta Kernel runs with no Hadoop or parquet-java on the classpath. */
class DeltaKernelReaderSpec extends AnyFunSpec with Matchers {

  private val protocol =
    """{"protocol":{"deltaProtocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors","columnMapping","timestampNtz"],"writerFeatures":["deletionVectors","columnMapping","timestampNtz"]}}}"""

  private def metadata(fields: Seq[Json], columnMapping: Boolean, partitionColumns: Seq[String] = Seq()): String = {
    val schema = Json.obj("type" -> Json.fromString("struct"), "fields" -> Json.fromValues(fields)).noSpaces
    val configuration =
      if (columnMapping)
        Json.obj(
          "delta.columnMapping.mode" -> Json.fromString("name"),
          "delta.columnMapping.maxColumnId" -> Json.fromString("100"),
          "delta.enableDeletionVectors" -> Json.fromString("true"),
        )
      else Json.obj("delta.enableDeletionVectors" -> Json.fromString("true"))
    Json
      .obj(
        "metaData" -> Json.obj(
          "deltaMetadata" -> Json.obj(
            "id" -> Json.fromString("test-table"),
            "format" -> Json.obj("provider" -> Json.fromString("parquet"), "options" -> Json.obj()),
            "schemaString" -> Json.fromString(schema),
            "partitionColumns" -> Json.fromValues(partitionColumns.map(Json.fromString)),
            "configuration" -> configuration,
            "createdTime" -> Json.fromLong(1L),
          ),
        ),
      )
      .noSpaces
  }

  /** Returns a schema field. `physicalName` makes it a column-mapped field, stored in Parquet under that name. */
  private def field(name: String, dataType: Json, physicalName: Option[String]): Json = {
    val fieldMetadata = physicalName.fold(Json.obj()) { physical =>
      Json.obj(
        "delta.columnMapping.id" -> Json.fromInt(math.abs(name.hashCode % 100)),
        "delta.columnMapping.physicalName" -> Json.fromString(physical),
      )
    }
    Json.obj(
      "name" -> Json.fromString(name),
      "type" -> dataType,
      "nullable" -> Json.True,
      "metadata" -> fieldMetadata,
    )
  }

  private def primitive(name: String, dataType: String, physicalName: Option[String] = None): Json =
    field(name, Json.fromString(dataType), physicalName)

  private def deletionVector(deletedRowPositions: Int*): DeltaKernelDVEntry = {
    val bitmap = RoaringBitmap.bitmapOf(deletedRowPositions: _*)
    val payload = ByteBuffer.allocate(4 + 8 + 4 + bitmap.serializedSizeInBytes).order(ByteOrder.LITTLE_ENDIAN)
    payload.putInt(1681511377).putLong(1L).putInt(0)
    bitmap.serialize(payload)
    val crc = new CRC32()
    crc.update(payload.array())
    val file = ByteBuffer.allocate(1 + 4 + payload.capacity + 4)
    file.put(1.toByte).putInt(payload.capacity).put(payload.array()).putInt(crc.getValue.toInt)
    DeltaKernelDVEntry(
      file.array(),
      offset = 1L,
      sizeInBytes = payload.capacity.toLong,
      deletedRowPositions.size.toLong,
    )
  }

  private def dataFile(
    bytes: Array[Byte],
    deletionVector: Option[DeltaKernelDVEntry] = None,
    partitionValues: Map[String, String] = Map.empty,
  ): DeltaKernelFileEntry =
    DeltaKernelFileEntry(bytes, bytes.length.toLong, partitionValues, 1L, deletionVector)

  private def fixture(name: String): Array[Byte] =
    Files.readAllBytes(Paths.get(getClass.getResource(s"/parquet/$name").toURI))

  private def asCypher(record: ParquetRecord): cypher.Value = ParquetRecord.foldable.fold(record, cypherValueFolder)

  private def ids(records: Seq[ParquetRecord]): Seq[ParquetValue] =
    records.map(_.fields.collectFirst { case ("id", id) => id }.get)

  private val usersFile = TestParquetWriter.write(
    Seq(Column("col-id", Int64, optional = false), Column("col-email", Utf8)),
    Seq(Seq(1L, "a@x"), Seq(2L, "b@x"), Seq(3L, "c@x")),
  )

  private val usersMetadata = metadata(
    Seq(
      primitive("id", "long", Some("col-id")),
      primitive("email", "string", Some("col-email")),
      primitive("region", "string", Some("region")),
    ),
    partitionColumns = Seq("region"),
    columnMapping = true,
  )

  describe("the classpath") {
    it("has no Hadoop, parquet-java, or parquet4s") {
      Seq(
        "org.apache.hadoop.conf.Configuration",
        "org.apache.parquet.hadoop.ParquetFileReader",
        "org.apache.parquet.schema.MessageType",
        "com.github.mjakubowski84.parquet4s.ParquetReader",
      ).foreach { className =>
        withClue(s"$className: ") {
          a[ClassNotFoundException] should be thrownBy Class.forName(className)
        }
      }
    }
  }

  describe("readWithDeletionVectors") {
    it("applies a deletion vector, column mapping, and partition values together") {
      val records = DeltaKernelReader.readWithDeletionVectors(
        protocol,
        usersMetadata,
        Seq(dataFile(usersFile, Some(deletionVector(1)), Map("region" -> "west"))),
      )
      records.map(asCypher) shouldEqual Seq(
        ce.Map("id" -> ce.Integer(1L), "email" -> ce.Str("a@x"), "region" -> ce.Str("west")),
        ce.Map("id" -> ce.Integer(3L), "email" -> ce.Str("c@x"), "region" -> ce.Str("west")),
      )
    }

    it("keeps every row of a file with no deletion vector") {
      val records = DeltaKernelReader.readWithDeletionVectors(
        protocol,
        usersMetadata,
        Seq(
          dataFile(usersFile, Some(deletionVector(0, 2)), Map("region" -> "west")),
          dataFile(usersFile, None, Map("region" -> "east")),
        ),
      )
      records.map(asCypher).toSet shouldEqual Set(
        ce.Map("id" -> ce.Integer(2L), "email" -> ce.Str("b@x"), "region" -> ce.Str("west")),
        ce.Map("id" -> ce.Integer(1L), "email" -> ce.Str("a@x"), "region" -> ce.Str("east")),
        ce.Map("id" -> ce.Integer(2L), "email" -> ce.Str("b@x"), "region" -> ce.Str("east")),
        ce.Map("id" -> ce.Integer(3L), "email" -> ce.Str("c@x"), "region" -> ce.Str("east")),
      )
    }

    it("counts deleted row positions across row groups") {
      val records = DeltaKernelReader.readWithDeletionVectors(
        protocol,
        metadata(Seq(primitive("id", "long")), columnMapping = false),
        Seq(dataFile(fixture("codec-snappy.parquet"), Some(deletionVector(0, 1500, 1999)))),
      )
      val expected = (0 until 2000).filterNot(Set(0, 1500, 1999)).map(i => ParquetValue.Integer(i.toLong))
      ids(records) shouldEqual expected
    }

    it("counts deleted row positions across the handler's internal batches") {
      val bytes = TestParquetWriter.write(
        Seq(Column("id", Int64, optional = false)),
        (0 until 10000).map(i => Seq(i.toLong)),
      )
      val records = DeltaKernelReader.readWithDeletionVectors(
        protocol,
        metadata(Seq(primitive("id", "long")), columnMapping = false),
        Seq(dataFile(bytes, Some(deletionVector(4095, 4096, 9999)))),
      )
      val expected = (0 until 10000).filterNot(Set(4095, 4096, 9999)).map(i => ParquetValue.Integer(i.toLong))
      ids(records) shouldEqual expected
    }

    it("reads every Delta column type, renaming nested fields by column mapping") {
      val struct = Json.obj(
        "type" -> Json.fromString("struct"),
        "fields" -> Json.arr(
          primitive("town", "string", Some("city")),
          field(
            "location",
            Json.obj(
              "type" -> Json.fromString("struct"),
              "fields" -> Json.arr(primitive("latitude", "double", Some("lat"))),
            ),
            Some("geo"),
          ),
          primitive("day", "date", Some("day")),
        ),
      )
      val array = Json.obj(
        "type" -> Json.fromString("array"),
        "elementType" -> Json.fromString("string"),
        "containsNull" -> Json.True,
      )
      val map = Json.obj(
        "type" -> Json.fromString("map"),
        "keyType" -> Json.fromString("string"),
        "valueType" -> Json.fromString("timestamp"),
        "valueContainsNull" -> Json.True,
      )
      val fields = Seq(
        primitive("id", "long", Some("id")),
        primitive("flag", "boolean", Some("flag")),
        primitive("f", "float", Some("f")),
        primitive("d", "double", Some("d")),
        primitive("i32", "integer", Some("i32")),
        primitive("i8", "byte", Some("i8")),
        primitive("date32", "date", Some("date32")),
        primitive("dec32", "decimal(9,2)", Some("dec32")),
        primitive("decfix", "decimal(20,4)", Some("decfix")),
        primitive("ts_us_utc", "timestamp", Some("ts_us_utc")),
        primitive("ts_ms_local", "timestamp_ntz", Some("ts_ms_local")),
        primitive("ts96", "timestamp", Some("ts96")),
        primitive("str", "string", Some("str")),
        primitive("raw", "binary", Some("raw")),
        field("tags", array, Some("tags")),
        field("attrs", map, Some("attrs")),
        field("address", struct, Some("addr")),
        primitive("absent", "string", Some("not-in-the-file")),
        primitive("region", "string", Some("region")),
      )

      val records = DeltaKernelReader.readWithDeletionVectors(
        protocol,
        metadata(fields, partitionColumns = Seq("region"), columnMapping = true),
        Seq(dataFile(fixture("all-types.parquet"), None, Map("region" -> "west"))),
      )

      val day = LocalDate.of(2024, 1, 15)
      records.map(asCypher) shouldEqual Seq(
        ce.Map(
          "id" -> ce.Integer(1L),
          "flag" -> ce.True,
          "f" -> ce.Floating(1.5),
          "d" -> ce.Floating(2.5),
          "i32" -> ce.Integer(42L),
          "i8" -> ce.Integer(-5L),
          "date32" -> ce.Date(day),
          "dec32" -> ce.Str("123.45"),
          "decfix" -> ce.Str("123.4567"),
          "ts_us_utc" -> ce.DateTime(ZonedDateTime.of(2024, 1, 15, 10, 40, 0, 123456000, ZoneOffset.UTC)),
          "ts_ms_local" -> ce.LocalDateTime(LocalDateTime.of(2024, 1, 15, 10, 40, 0)),
          "ts96" -> ce.DateTime(ZonedDateTime.of(2024, 1, 14, 12, 0, 0, 123456000, ZoneOffset.UTC)),
          "str" -> ce.Str("héllo 日本語"),
          "raw" -> ce.Bytes(Array[Byte](0, 0xFF.toByte, 0x80.toByte, 0x7F)),
          "tags" -> ce.List(ce.Str("x"), ce.Null, ce.Str("y")),
          "attrs" -> ce.Map(
            "created" -> ce.DateTime(ZonedDateTime.of(1970, 1, 1, 0, 0, 1, 0, ZoneOffset.UTC)),
            "missing" -> ce.Null,
          ),
          "address" -> ce.Map(
            "town" -> ce.Str("Portland"),
            "location" -> ce.Map("latitude" -> ce.Floating(45.5)),
            "day" -> ce.Date(day),
          ),
          "absent" -> ce.Null,
          "region" -> ce.Str("west"),
        ),
        ce.Map(
          "id" -> ce.Integer(2L),
          "flag" -> ce.Null,
          "f" -> ce.Null,
          "d" -> ce.Null,
          "i32" -> ce.Null,
          "i8" -> ce.Null,
          "date32" -> ce.Null,
          "dec32" -> ce.Null,
          "decfix" -> ce.Null,
          "ts_us_utc" -> ce.Null,
          "ts_ms_local" -> ce.Null,
          "ts96" -> ce.Null,
          "str" -> ce.Null,
          "raw" -> ce.Null,
          "tags" -> ce.Null,
          "attrs" -> ce.Null,
          "address" -> ce.Null,
          "absent" -> ce.Null,
          "region" -> ce.Str("west"),
        ),
      )
    }

    it("rejects a table that requires a reader feature Kernel does not support") {
      val unsupported =
        """{"protocol":{"deltaProtocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["noSuchFeature"],"writerFeatures":["noSuchFeature"]}}}"""
      an[Exception] should be thrownBy DeltaKernelReader.readWithDeletionVectors(
        unsupported,
        metadata(Seq(primitive("id", "long")), columnMapping = false),
        Seq(dataFile(usersFile)),
      )
    }
  }
}

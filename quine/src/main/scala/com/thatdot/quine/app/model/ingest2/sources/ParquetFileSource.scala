package com.thatdot.quine.app.model.ingest2.sources

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Base64

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import com.github.mjakubowski84.parquet4s.{
  BinaryValue,
  BooleanValue,
  DateTimeValue,
  DecimalValue,
  DoubleValue,
  FloatValue,
  IntValue,
  ListParquetRecord,
  LongValue,
  MapParquetRecord,
  NullValue,
  ParquetIterable,
  ParquetReader,
  RowParquetRecord,
  Value => PqValue,
}
import io.circe.{Json, JsonNumber}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import com.thatdot.data.DataFoldableFrom
import com.thatdot.data.DataFoldableFrom._
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.routes.IngestMeter

/** Parquet file ingest source. Collects the entire byte stream, then reads rows via parquet4s using an in-memory
  * InputFile adapter.
  *
  * Unlike text-based formats (Line, JSON, CSV), Parquet requires random access to the file (the footer is at the end),
  * so the full file must be buffered before any rows can be read.
  *
  * NOTE: This source reads the entire file into memory! That is deliberate because of the plan to read data from URLs
  * and the requirement to seek to the footer before reading the rest of the data. But this causes a problem for
  * reading large files from the local (or hadoop) filesystem. Support for large local files will require refactoring
  * to handle seeking on the input without actually loading it all into memory.
  */
case class ParquetFileSource(
  src: Source[ByteString, NotUsed],
  ingestBounds: IngestBounds,
  ingestMeter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
) {

  def decodedSource: DecodedSource =
    new DecodedSource(ingestMeter) {
      type Decoded = Json
      type Frame = ByteString

      override val foldableFrame: DataFoldableFrom[ByteString] = byteStringDataFoldable
      override val foldable: DataFoldableFrom[Json] = jsonDataFoldable

      override def content(input: ByteString): Array[Byte] = input.toArrayUnsafe()

      def stream: Source[(() => Try[Json], Frame), ShutdownSwitch] = {
        val parquetStream: Source[(() => Try[Json], ByteString), NotUsed] =
          src
            .via(decompressingFlow(decoders))
            .fold(ByteString.empty)(_ ++ _)
            .flatMapConcat { allBytes =>
              // Note: this flatMapConcat is executed only once because it follows a `.fold`. It's purpose is
              // essentially to collec the full contents of the file (because it's Parquet), and then emit elements in
              // a new stream after having processed the whole file.
              val inputFile =
                new ByteArrayInputFile(allBytes.toArrayUnsafe()) // UNSAFE = Must never modify the returned byte array!
              val columnTypes = ParquetFileSource.readColumnTypes(inputFile)
              Source
                .unfoldResource[Json, (ParquetIterable[RowParquetRecord], Iterator[RowParquetRecord])](
                  create = () => {
                    val iterable = ParquetReader.generic.read(inputFile)
                    (iterable, iterable.iterator)
                  },
                  read = { case (_, iter) =>
                    if (iter.hasNext) Some(ParquetFileSource.recordToJson(iter.next(), columnTypes))
                    else None
                  },
                  close = { case (iterable, _) => iterable.close() },
                )
            }
            .via(boundingFlow(ingestBounds))
            .wireTap(json => meter.mark(json.noSpaces.length))
            .map(json => (() => Success(json), ByteString.empty))

        withKillSwitches(parquetStream)
      }
    }
}

/** Schema-driven Parquet-to-JSON conversion.
  *
  * == Parquet type system (two layers) ==
  *
  * '''Layer 1 — Physical types''' define the on-disk binary encoding.
  * There are exactly 8, fixed by the Parquet specification:
  *   - BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BINARY, FIXED_LEN_BYTE_ARRAY
  *
  * '''Layer 2 — Logical type annotations''' are optional metadata on each column
  * that tell readers how to interpret the physical bytes. The set is fixed by the
  * spec (no user-defined custom annotations):
  *   - String, Enum, UUID — annotate BINARY or FIXED_LEN_BYTE_ARRAY as text/identifiers
  *   - Int(bitWidth, signed) — annotate INT32/INT64 as sized integers (INT_8 through INT_64)
  *   - Decimal(precision, scale) — annotate INT32/INT64/BINARY/FIXED_LEN_BYTE_ARRAY
  *   - Date — annotate INT32 as days since Unix epoch
  *   - Time(unit, isAdjustedToUTC) — annotate INT32 (MILLIS) or INT64 (MICROS/NANOS)
  *   - Timestamp(unit, isAdjustedToUTC) — annotate INT64 as epoch millis/micros/nanos
  *   - Json — annotate BINARY as embedded JSON text
  *   - Bson — annotate BINARY as BSON bytes
  *   - Float16 — annotate FIXED_LEN_BYTE_ARRAY(2) as half-precision float
  *   - Interval — annotate FIXED_LEN_BYTE_ARRAY(12) as time interval
  *   - List, Map, MapKeyValue — annotate GROUP types for nested structures
  *
  * A column with no logical annotation is interpreted by its physical type alone.
  * Notably, BINARY with no annotation is '''raw opaque bytes''' (not a string).
  *
  * INT96 is a deprecated physical type used exclusively as a nanosecond timestamp
  * (Julian day + nanoseconds-of-day). It has no logical type annotation.
  *
  * == Why we need schema-driven conversion ==
  *
  * parquet4s's generic reader maps physical types to Value subclasses (IntValue,
  * LongValue, BinaryValue, etc.) but does not preserve the logical type annotation.
  * For example, both a UTF-8 STRING column and a raw BINARY column produce the same
  * BinaryValue type. INT96 columns also produce BinaryValue. Without the schema, we
  * cannot distinguish these cases.
  *
  * We read the Parquet file schema once before iterating rows, build a column-name-to-type
  * descriptor map, and use that to drive the JSON conversion for each field.
  */
object ParquetFileSource {

  // Constants for INT96 timestamp decoding.
  // INT96 stores: bytes 0-7 = nanoseconds within the day (little-endian),
  //               bytes 8-11 = Julian day number (little-endian).
  private val JulianDayOfUnixEpoch = 2440588L
  private val MillisPerDay = 86400000L
  private val NanosPerMilli = 1000000L
  private val MicrosPerMilli = 1000L

  /** Descriptor for a single Parquet column, capturing both physical and logical type. */
  private[sources] case class ColumnTypeInfo(
    physicalType: PrimitiveTypeName,
    logicalType: Option[LogicalTypeAnnotation],
  )

  /** Read the Parquet file schema and return a map from column name to its type descriptor.
    * Only top-level primitive columns are included; nested group types (LIST, MAP, struct)
    * are handled structurally by parquet4s and do not need schema-level dispatch.
    */
  private[sources] def readColumnTypes(inputFile: ByteArrayInputFile): Map[String, ColumnTypeInfo] = {
    val reader = ParquetFileReader.open(inputFile)
    try {
      val schema = reader.getFileMetaData.getSchema
      schema.getFields.asScala.collect {
        case f if f.isPrimitive =>
          val pt = f.asPrimitiveType()
          f.getName -> ColumnTypeInfo(pt.getPrimitiveTypeName, Option(pt.getLogicalTypeAnnotation))
      }.toMap
    } finally reader.close()
  }

  /** Convert a parquet4s RowParquetRecord to a circe Json object, using the schema-derived
    * column type map to correctly interpret each field's physical bytes.
    */
  private[sources] def recordToJson(record: RowParquetRecord, columnTypes: Map[String, ColumnTypeInfo]): Json =
    Json.obj(record.iterator.map { case (name, value) =>
      name -> convertValue(value, columnTypes.get(name))
    }.toSeq: _*)

  /** Convert a parquet4s Value to circe Json using schema type information.
    *
    * Dispatch priority:
    *   1. If schema info is available, use (physicalType, logicalType) to determine conversion.
    *   2. If no schema info (e.g. nested values from LIST/MAP), fall back to Value-type dispatch.
    *   3. NullValue is always Json.Null regardless of schema.
    */
  private def convertValue(value: PqValue, typeInfo: Option[ColumnTypeInfo]): Json = value match {
    case NullValue => Json.Null
    case _ =>
      typeInfo match {
        case Some(info) => convertWithSchema(value, info)
        case None => convertWithoutSchema(value)
      }
  }

  // ---------------------------------------------------------------------------
  // Schema-driven conversion: dispatch on (physical type, logical annotation)
  // ---------------------------------------------------------------------------

  private def convertWithSchema(value: PqValue, info: ColumnTypeInfo): Json =
    (info.physicalType, info.logicalType) match {

      // -- BOOLEAN: no logical annotations defined by the spec --
      case (PrimitiveTypeName.BOOLEAN, _) =>
        value match {
          case BooleanValue(v) => Json.fromBoolean(v)
          case _ => convertWithoutSchema(value)
        }

      // -- FLOAT / DOUBLE: no logical annotations defined by the spec --
      case (PrimitiveTypeName.FLOAT, _) =>
        value match {
          case FloatValue(v) => Json.fromJsonNumber(JsonNumber.fromDecimalStringUnsafe(v.toString))
          case _ => convertWithoutSchema(value)
        }

      case (PrimitiveTypeName.DOUBLE, _) =>
        value match {
          case DoubleValue(v) => Json.fromDoubleOrNull(v)
          case _ => convertWithoutSchema(value)
        }

      // -- INT32 --

      // INT32 + DATE: days since Unix epoch → ISO-8601 date string
      case (PrimitiveTypeName.INT32, Some(_: DateLogicalTypeAnnotation)) =>
        value match {
          case IntValue(days) =>
            Json.fromString(java.time.LocalDate.ofEpochDay(days.toLong).toString)
          case _ => convertWithoutSchema(value)
        }

      // INT32 + DECIMAL: parquet4s already decodes this as DecimalValue
      case (PrimitiveTypeName.INT32, Some(_: DecimalLogicalTypeAnnotation)) =>
        value match {
          case d: DecimalValue => Json.fromBigDecimal(d.toBigDecimal)
          case IntValue(v) => Json.fromInt(v) // fallback if parquet4s didn't decode
          case _ => convertWithoutSchema(value)
        }

      // INT32 + TIME(MILLIS): milliseconds since midnight → emit as integer
      case (PrimitiveTypeName.INT32, Some(_: TimeLogicalTypeAnnotation)) =>
        value match {
          case IntValue(v) => Json.fromInt(v)
          case _ => convertWithoutSchema(value)
        }

      // INT32 + INT(8/16/32, signed/unsigned): sized integer → emit as number
      // INT32 with no annotation: plain 32-bit integer
      case (PrimitiveTypeName.INT32, None | Some(_: IntLogicalTypeAnnotation)) =>
        value match {
          case IntValue(v) => Json.fromInt(v)
          case _ => convertWithoutSchema(value)
        }

      // -- INT64 --

      //
      // WARNING: This entire section has many instances of conversion from `Long` to `JSON`. For high-valued Longs,
      // JSON truncates the precision leading to the wrong number being returned. This depends on downstream decisions,
      // so it's not necessarily a problem at this point in the code, but it could become a problem later on depending
      // on what happens downstream of this conversion.
      //

      // INT64 + TIMESTAMP: epoch timestamp with unit (MILLIS, MICROS, or NANOS).
      // Normalize to epoch milliseconds for consistent downstream consumption.
      case (PrimitiveTypeName.INT64, Some(ts: TimestampLogicalTypeAnnotation)) =>
        value match {
          case LongValue(v) =>
            val millis = ts.getUnit match {
              case TimeUnit.MILLIS => v
              case TimeUnit.MICROS => v / MicrosPerMilli
              case TimeUnit.NANOS => v / NanosPerMilli
            }
            Json.fromLong(millis)
          // parquet4s may decode INT64+TIMESTAMP as DateTimeValue
          case DateTimeValue(v, _) => Json.fromLong(v)
          case _ => convertWithoutSchema(value)
        }

      // INT64 + TIME(MICROS/NANOS): microseconds or nanoseconds since midnight
      case (PrimitiveTypeName.INT64, Some(_: TimeLogicalTypeAnnotation)) =>
        value match {
          case LongValue(v) => Json.fromLong(v)
          case DateTimeValue(v, _) => Json.fromLong(v)
          case _ => convertWithoutSchema(value)
        }

      // INT64 + DECIMAL
      case (PrimitiveTypeName.INT64, Some(_: DecimalLogicalTypeAnnotation)) =>
        value match {
          case d: DecimalValue => Json.fromBigDecimal(d.toBigDecimal)
          case LongValue(v) => Json.fromLong(v)
          case _ => convertWithoutSchema(value)
        }

      // INT64 + INT(64, signed/unsigned) or no annotation: plain 64-bit integer
      case (PrimitiveTypeName.INT64, None | Some(_: IntLogicalTypeAnnotation)) =>
        value match {
          case LongValue(v) => Json.fromLong(v)
          case DateTimeValue(v, _) => Json.fromLong(v)
          case _ => convertWithoutSchema(value)
        }

      // -- INT96: deprecated timestamp, no logical annotation --
      // Always a nanosecond timestamp: bytes 0-7 = nanos-of-day (LE), bytes 8-11 = Julian day (LE).
      // parquet4s emits this as BinaryValue since it has no specialized INT96 converter.
      case (PrimitiveTypeName.INT96, _) =>
        value match {
          case BinaryValue(v) =>
            val bytes = v.getBytes
            val nanosOfDay = java.nio.ByteBuffer.wrap(bytes, 0, 8).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong
            val julianDay = java.nio.ByteBuffer.wrap(bytes, 8, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
            Json.fromLong((julianDay.toLong - JulianDayOfUnixEpoch) * MillisPerDay + nanosOfDay / NanosPerMilli)
          case _ => convertWithoutSchema(value)
        }

      // -- BINARY --

      // BINARY + STRING or ENUM: UTF-8 encoded text
      case (PrimitiveTypeName.BINARY, Some(_: StringLogicalTypeAnnotation | _: EnumLogicalTypeAnnotation)) =>
        value match {
          case BinaryValue(v) => Json.fromString(new String(v.getBytes, java.nio.charset.StandardCharsets.UTF_8))
          case _ => convertWithoutSchema(value)
        }

      // BINARY + JSON: embedded JSON text — parse and inline
      case (PrimitiveTypeName.BINARY, Some(_: JsonLogicalTypeAnnotation)) =>
        value match {
          case BinaryValue(v) =>
            val text = new String(v.getBytes, java.nio.charset.StandardCharsets.UTF_8)
            io.circe.parser.parse(text).getOrElse(Json.fromString(text))
          case _ => convertWithoutSchema(value)
        }

      // BINARY + DECIMAL
      case (PrimitiveTypeName.BINARY, Some(_: DecimalLogicalTypeAnnotation)) =>
        value match {
          case d: DecimalValue => Json.fromBigDecimal(d.toBigDecimal)
          case _ => convertWithoutSchema(value)
        }

      // BINARY + BSON: opaque binary — base64 encode
      case (PrimitiveTypeName.BINARY, Some(_: BsonLogicalTypeAnnotation)) =>
        value match {
          case BinaryValue(v) => Json.fromString(Base64.getEncoder.encodeToString(v.getBytes))
          case _ => convertWithoutSchema(value)
        }

      // BINARY with no annotation: raw opaque bytes per the Parquet spec.
      // Base64 encode since JSON cannot represent raw binary.
      case (PrimitiveTypeName.BINARY, None) =>
        value match {
          case BinaryValue(v) => Json.fromString(Base64.getEncoder.encodeToString(v.getBytes))
          case _ => convertWithoutSchema(value)
        }

      // -- FIXED_LEN_BYTE_ARRAY --

      // FIXED_LEN_BYTE_ARRAY(16) + UUID: 128-bit UUID → standard string representation
      case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: UUIDLogicalTypeAnnotation)) =>
        value match {
          case BinaryValue(v) =>
            val buf = java.nio.ByteBuffer.wrap(v.getBytes)
            val uuid = new java.util.UUID(buf.getLong, buf.getLong)
            Json.fromString(uuid.toString)
          case _ => convertWithoutSchema(value)
        }

      // FIXED_LEN_BYTE_ARRAY + DECIMAL
      case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: DecimalLogicalTypeAnnotation)) =>
        value match {
          case d: DecimalValue => Json.fromBigDecimal(d.toBigDecimal)
          case _ => convertWithoutSchema(value)
        }

      // FIXED_LEN_BYTE_ARRAY + FLOAT16: half-precision float
      case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: Float16LogicalTypeAnnotation)) =>
        value match {
          case BinaryValue(v) =>
            val half = java.nio.ByteBuffer.wrap(v.getBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).getShort
            Json.fromDoubleOrNull(halfToDouble(half))
          case _ => convertWithoutSchema(value)
        }

      // FIXED_LEN_BYTE_ARRAY + INTERVAL: 12 bytes (months, days, millis) — emit as object
      case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: IntervalLogicalTypeAnnotation)) =>
        value match {
          case BinaryValue(v) =>
            val buf = java.nio.ByteBuffer.wrap(v.getBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            val months = buf.getInt
            val days = buf.getInt
            val millis = buf.getInt
            Json.obj("months" -> Json.fromInt(months), "days" -> Json.fromInt(days), "millis" -> Json.fromInt(millis))
          case _ => convertWithoutSchema(value)
        }

      // FIXED_LEN_BYTE_ARRAY with no annotation or unrecognized annotation: base64
      case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, _) =>
        value match {
          case BinaryValue(v) => Json.fromString(Base64.getEncoder.encodeToString(v.getBytes))
          case _ => convertWithoutSchema(value)
        }

      // Catch-all for any unrecognized (physical, logical) combination
      case _ => convertWithoutSchema(value)
    }

  // ---------------------------------------------------------------------------
  // Fallback conversion: dispatch on parquet4s Value type alone.
  // Used for nested values (inside LIST/MAP) where per-field schema info is not readily available, and as a fallback
  // for unrecognized schema types. Note that the parquet4s Value type does not handle all required cases (`INT96`).
  // ---------------------------------------------------------------------------

  private def convertWithoutSchema(value: PqValue): Json = value match {
    case NullValue => Json.Null
    case IntValue(v) => Json.fromInt(v)
    case LongValue(v) => Json.fromLong(v)
    case FloatValue(v) => Json.fromJsonNumber(JsonNumber.fromDecimalStringUnsafe(v.toString))
    case DoubleValue(v) => Json.fromDoubleOrNull(v)
    case BooleanValue(v) => Json.fromBoolean(v)
    case BinaryValue(v) =>
      // Without schema info we cannot distinguish STRING (which use BinaryValue) from raw BINARY.
      // Base64 encoding represents a safe choice to be not-incorrect. It is lossless for both cases and consistent
      // with how convertWithSchema handles unannotated BINARY.
      Json.fromString(Base64.getEncoder.encodeToString(v.getBytes))
    case d: DecimalValue => Json.fromBigDecimal(d.toBigDecimal)
    case DateTimeValue(v, _) => Json.fromLong(v)
    case list: ListParquetRecord => Json.arr(list.iterator.map(v => convertValue(v, None)).toSeq: _*)
    case map: MapParquetRecord =>
      Json.obj(map.iterator.map { case (k, v) =>
        valueToString(k) -> convertValue(v, None)
      }.toSeq: _*)
    case row: RowParquetRecord => recordToJson(row, Map.empty)
    case other => Json.fromString(other.toString)
  }

  /** Convert a parquet4s Value to a string, for use as a JSON object key in MAP records.
    * Uses strict UTF-8 decoding for binary values — malformed bytes will throw rather than
    * silently produce replacement characters, since corrupted map keys can cause subtle bugs.
    */
  private def valueToString(value: PqValue): String = value match {
    case BinaryValue(v) => decodeStrictUtf8(v.getBytes)
    case IntValue(v) => v.toString
    case LongValue(v) => v.toString
    case other => other.toString
  }

  /** Decode bytes as UTF-8, throwing on malformed input rather than silently replacing. */
  private def decodeStrictUtf8(bytes: Array[Byte]): String = {
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT) // THROWS!
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    decoder.decode(ByteBuffer.wrap(bytes)).toString
  }

  /** Convert an IEEE 754 half-precision (16-bit) float to a double. */
  private def halfToDouble(half: Short): Double = {
    val h = half & 0xFFFF
    val sign = (h >>> 15) & 1
    val exponent = (h >>> 10) & 0x1F
    val mantissa = h & 0x3FF
    val value =
      if (exponent == 0) {
        // subnormal
        math.scalb(mantissa.toDouble / 1024.0, -14)
      } else if (exponent == 31) {
        // infinity or NaN
        if (mantissa == 0) Double.PositiveInfinity
        else Double.NaN
      } else {
        // normal
        math.scalb((mantissa.toDouble / 1024.0) + 1.0, exponent - 15)
      }
    if (sign == 1) -value else value
  }
}

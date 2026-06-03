package com.thatdot.quine.app.model.ingest2.codec

import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}

import scala.jdk.CollectionConverters._

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
  RowParquetRecord,
  Value => PqValue,
}
import io.circe.parser
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}

/** A decoded Parquet row paired with the column-type descriptors needed to interpret its
  * physical bytes through a [[DataFolderTo]].
  *
  * parquet4s's `RowParquetRecord` carries only physical-type-tagged values (`IntValue`,
  * `BinaryValue`, etc.) and not the logical-type annotations from the file's schema. The
  * schema lives in the file footer and is read once per file. Pairing each emitted row
  * with its schema descriptors lets the foldable dispatch correctly on `(physical, logical)`
  * without needing extra context at fold time.
  *
  * Use [[ParquetRecord.readColumnTypes]] to extract the schema map from a `SeekableInput`
  * once, then wrap each decoded row with that map.
  */
final case class ParquetRecord(
  record: RowParquetRecord,
  columnTypes: Map[String, ParquetRecord.ColumnDescriptor],
)

object ParquetRecord {

  /** Descriptor for a single Parquet column, capturing both physical and logical type. */
  final case class ColumnDescriptor(
    physicalType: PrimitiveTypeName,
    logicalType: Option[LogicalTypeAnnotation],
  )

  /** Read the Parquet file schema and return a map from column name to its type descriptor.
    * Only top-level primitive columns are included; nested group types (LIST, MAP, struct)
    * are handled structurally by parquet4s and do not need schema-level dispatch.
    */
  def readColumnTypes(input: SeekableInput): Map[String, ColumnDescriptor] = {
    val reader = ParquetFileReader.open(ParquetDecoder.toParquetInputFile(input))
    try {
      val schema = reader.getFileMetaData.getSchema
      schema.getFields.asScala.collect {
        case f if f.isPrimitive =>
          val pt = f.asPrimitiveType()
          f.getName -> ColumnDescriptor(pt.getPrimitiveTypeName, Option(pt.getLogicalTypeAnnotation))
      }.toMap
    } finally reader.close()
  }

  // Constants for INT96 timestamp decoding.
  // INT96 stores: bytes 0-7 = nanoseconds within the day (little-endian),
  //               bytes 8-11 = Julian day number (little-endian).
  private val JulianDayOfUnixEpoch = 2440588L
  private val SecondsPerDay = 86400L
  private val NanosPerSecond = 1000000000L
  private val MicrosPerSecond = 1000000L
  private val MillisPerSecond = 1000L

  implicit val foldable: DataFoldableFrom[ParquetRecord] = new DataFoldableFrom[ParquetRecord] {

    def fold[B](value: ParquetRecord, folder: DataFolderTo[B]): B = {
      val mb = folder.mapBuilder()
      value.record.iterator.foreach { case (name, pq) =>
        mb.add(name, foldValue(pq, value.columnTypes.get(name), folder))
      }
      mb.finish()
    }

    /** Dispatch a parquet4s `Value` to the appropriate `DataFolderTo` method, using schema
      * info when available. Nested values from `LIST` / `MAP` aren't carried with their own
      * schema info (parquet4s flattens this), so they fall back to type-only dispatch.
      */
    private def foldValue[B](pq: PqValue, info: Option[ColumnDescriptor], folder: DataFolderTo[B]): B = pq match {
      case NullValue => folder.nullValue
      case _ =>
        info match {
          case Some(c) => foldWithSchema(pq, c, folder)
          case None => foldWithoutSchema(pq, folder)
        }
    }

    // -------------------------------------------------------------------------
    // Schema-driven dispatch on (physical type, logical annotation)
    // -------------------------------------------------------------------------

    private def foldWithSchema[B](pq: PqValue, info: ColumnDescriptor, folder: DataFolderTo[B]): B =
      (info.physicalType, info.logicalType) match {

        // BOOLEAN: no logical annotations defined by the spec
        case (PrimitiveTypeName.BOOLEAN, _) =>
          pq match {
            case BooleanValue(v) => if (v) folder.trueValue else folder.falseValue
            case _ => foldWithoutSchema(pq, folder)
          }

        // FLOAT / DOUBLE: no logical annotations defined by the spec
        case (PrimitiveTypeName.FLOAT, _) =>
          pq match {
            case FloatValue(v) => folder.floating(v.toDouble)
            case _ => foldWithoutSchema(pq, folder)
          }
        case (PrimitiveTypeName.DOUBLE, _) =>
          pq match {
            case DoubleValue(v) => folder.floating(v)
            case _ => foldWithoutSchema(pq, folder)
          }

        // -- INT32 --

        // INT32 + DATE: days since Unix epoch
        case (PrimitiveTypeName.INT32, Some(_: DateLogicalTypeAnnotation)) =>
          pq match {
            case IntValue(days) => folder.date(LocalDate.ofEpochDay(days.toLong))
            case _ => foldWithoutSchema(pq, folder)
          }

        // INT32 + DECIMAL — no native DataFolderTo decimal; emit as string to preserve precision
        case (PrimitiveTypeName.INT32, Some(_: DecimalLogicalTypeAnnotation)) =>
          pq match {
            case d: DecimalValue => folder.string(d.toBigDecimal.toString)
            case IntValue(v) => folder.integer(v.toLong)
            case _ => foldWithoutSchema(pq, folder)
          }

        // INT32 + TIME(MILLIS): milliseconds since midnight
        case (PrimitiveTypeName.INT32, Some(_: TimeLogicalTypeAnnotation)) =>
          pq match {
            case IntValue(v) => folder.localTime(LocalTime.ofNanoOfDay(v.toLong * 1000000L))
            case _ => foldWithoutSchema(pq, folder)
          }

        // INT32 + INT(8/16/32, signed/unsigned), or no annotation
        case (PrimitiveTypeName.INT32, None | Some(_: IntLogicalTypeAnnotation)) =>
          pq match {
            case IntValue(v) => folder.integer(v.toLong)
            case _ => foldWithoutSchema(pq, folder)
          }

        // -- INT64 --

        // INT64 + TIMESTAMP: epoch timestamp with unit and zone-adjustment flag
        case (PrimitiveTypeName.INT64, Some(ts: TimestampLogicalTypeAnnotation)) =>
          pq match {
            case LongValue(v) =>
              val (epochSeconds, nanoOfSecond) = ts.getUnit match {
                case TimeUnit.MILLIS =>
                  (Math.floorDiv(v, MillisPerSecond), (Math.floorMod(v, MillisPerSecond) * 1000000L).toInt)
                case TimeUnit.MICROS =>
                  (Math.floorDiv(v, MicrosPerSecond), (Math.floorMod(v, MicrosPerSecond) * 1000L).toInt)
                case TimeUnit.NANOS => (Math.floorDiv(v, NanosPerSecond), Math.floorMod(v, NanosPerSecond).toInt)
              }
              if (ts.isAdjustedToUTC)
                folder.zonedDateTime(
                  ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanoOfSecond.toLong), ZoneOffset.UTC),
                )
              else
                folder.localDateTime(LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, ZoneOffset.UTC))
            case DateTimeValue(v, _) => folder.integer(v) // parquet4s pre-decoded (rare)
            case _ => foldWithoutSchema(pq, folder)
          }

        // INT64 + TIME(MICROS / NANOS)
        case (PrimitiveTypeName.INT64, Some(t: TimeLogicalTypeAnnotation)) =>
          pq match {
            case LongValue(v) =>
              val nanos = t.getUnit match {
                case TimeUnit.MILLIS => v * 1000000L
                case TimeUnit.MICROS => v * 1000L
                case TimeUnit.NANOS => v
              }
              folder.localTime(LocalTime.ofNanoOfDay(nanos))
            case _ => foldWithoutSchema(pq, folder)
          }

        // INT64 + DECIMAL — preserve precision via string
        case (PrimitiveTypeName.INT64, Some(_: DecimalLogicalTypeAnnotation)) =>
          pq match {
            case d: DecimalValue => folder.string(d.toBigDecimal.toString)
            case LongValue(v) => folder.integer(v)
            case _ => foldWithoutSchema(pq, folder)
          }

        // INT64 + INT or no annotation
        case (PrimitiveTypeName.INT64, None | Some(_: IntLogicalTypeAnnotation)) =>
          pq match {
            case LongValue(v) => folder.integer(v)
            case DateTimeValue(v, _) => folder.integer(v)
            case _ => foldWithoutSchema(pq, folder)
          }

        // -- INT96: deprecated nanosecond timestamp --
        case (PrimitiveTypeName.INT96, _) =>
          pq match {
            case BinaryValue(v) =>
              val bytes = v.getBytes
              val nanosOfDay = ByteBuffer.wrap(bytes, 0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong
              val julianDay = ByteBuffer.wrap(bytes, 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt
              val epochSeconds = (julianDay.toLong - JulianDayOfUnixEpoch) * SecondsPerDay + nanosOfDay / NanosPerSecond
              val nanoOfSecond = (nanosOfDay % NanosPerSecond).toInt
              folder.localDateTime(LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, ZoneOffset.UTC))
            case _ => foldWithoutSchema(pq, folder)
          }

        // -- BINARY --

        // BINARY + STRING / ENUM: UTF-8 text
        case (PrimitiveTypeName.BINARY, Some(_: StringLogicalTypeAnnotation | _: EnumLogicalTypeAnnotation)) =>
          pq match {
            case BinaryValue(v) => folder.string(new String(v.getBytes, StandardCharsets.UTF_8))
            case _ => foldWithoutSchema(pq, folder)
          }

        // BINARY + JSON: embedded JSON — parse and re-fold through the destination folder
        case (PrimitiveTypeName.BINARY, Some(_: JsonLogicalTypeAnnotation)) =>
          pq match {
            case BinaryValue(v) =>
              val text = new String(v.getBytes, StandardCharsets.UTF_8)
              parser.parse(text) match {
                case Right(json) => DataFoldableFrom.jsonDataFoldable.fold(json, folder)
                case Left(_) => folder.string(text)
              }
            case _ => foldWithoutSchema(pq, folder)
          }

        // BINARY + DECIMAL — preserve precision via string
        case (PrimitiveTypeName.BINARY, Some(_: DecimalLogicalTypeAnnotation)) =>
          pq match {
            case d: DecimalValue => folder.string(d.toBigDecimal.toString)
            case _ => foldWithoutSchema(pq, folder)
          }

        // BINARY + BSON, or BINARY with no annotation: raw bytes
        case (PrimitiveTypeName.BINARY, _) =>
          pq match {
            case BinaryValue(v) => folder.bytes(v.getBytes)
            case _ => foldWithoutSchema(pq, folder)
          }

        // -- FIXED_LEN_BYTE_ARRAY --

        // FIXED_LEN_BYTE_ARRAY(16) + UUID
        case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: UUIDLogicalTypeAnnotation)) =>
          pq match {
            case BinaryValue(v) =>
              val buf = ByteBuffer.wrap(v.getBytes)
              folder.string(new java.util.UUID(buf.getLong, buf.getLong).toString)
            case _ => foldWithoutSchema(pq, folder)
          }

        // FIXED_LEN_BYTE_ARRAY + DECIMAL
        case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: DecimalLogicalTypeAnnotation)) =>
          pq match {
            case d: DecimalValue => folder.string(d.toBigDecimal.toString)
            case _ => foldWithoutSchema(pq, folder)
          }

        // FIXED_LEN_BYTE_ARRAY + FLOAT16: half-precision float
        case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: Float16LogicalTypeAnnotation)) =>
          pq match {
            case BinaryValue(v) =>
              val half = ByteBuffer.wrap(v.getBytes).order(ByteOrder.LITTLE_ENDIAN).getShort
              folder.floating(halfToDouble(half))
            case _ => foldWithoutSchema(pq, folder)
          }

        // FIXED_LEN_BYTE_ARRAY + INTERVAL: 12 bytes (months, days, millis).
        // No clean folder method covers a Parquet INTERVAL (months+days are calendrical, not
        // a simple Duration). Emit as a map to preserve all three components.
        case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Some(_: IntervalLogicalTypeAnnotation)) =>
          pq match {
            case BinaryValue(v) =>
              val buf = ByteBuffer.wrap(v.getBytes).order(ByteOrder.LITTLE_ENDIAN)
              val months = buf.getInt
              val days = buf.getInt
              val millis = buf.getInt
              val mb = folder.mapBuilder()
              mb.add("months", folder.integer(months.toLong))
              mb.add("days", folder.integer(days.toLong))
              mb.add("millis", folder.integer(millis.toLong))
              mb.finish()
            case _ => foldWithoutSchema(pq, folder)
          }

        // FIXED_LEN_BYTE_ARRAY with no annotation or unrecognized annotation: raw bytes
        case (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, _) =>
          pq match {
            case BinaryValue(v) => folder.bytes(v.getBytes)
            case _ => foldWithoutSchema(pq, folder)
          }

        case _ => foldWithoutSchema(pq, folder)
      }

    // -------------------------------------------------------------------------
    // Fallback dispatch on parquet4s Value type alone — used for nested values
    // (inside LIST / MAP) where per-field schema info is not readily available,
    // and as a fallback for unrecognized schema combinations. Note that the
    // Value-only dispatch cannot handle INT96 — that requires schema info.
    // -------------------------------------------------------------------------

    private def foldWithoutSchema[B](pq: PqValue, folder: DataFolderTo[B]): B = pq match {
      case NullValue => folder.nullValue
      case IntValue(v) => folder.integer(v.toLong)
      case LongValue(v) => folder.integer(v)
      case FloatValue(v) => folder.floating(v.toDouble)
      case DoubleValue(v) => folder.floating(v)
      case BooleanValue(v) => if (v) folder.trueValue else folder.falseValue
      // Without schema info, BINARY is ambiguous between STRING and raw bytes. Emit raw bytes —
      // a folder that wants string semantics can decode them; the previous JSON-intermediate
      // implementation lost type identity here by base64-encoding everything.
      case BinaryValue(v) => folder.bytes(v.getBytes)
      case d: DecimalValue => folder.string(d.toBigDecimal.toString)
      case DateTimeValue(v, _) => folder.integer(v)
      case list: ListParquetRecord =>
        val vb = folder.vectorBuilder()
        list.iterator.foreach(v => vb.add(foldValue(v, None, folder)))
        vb.finish()
      case map: MapParquetRecord =>
        val mb = folder.mapBuilder()
        map.iterator.foreach { case (k, v) => mb.add(valueToString(k), foldValue(v, None, folder)) }
        mb.finish()
      case row: RowParquetRecord =>
        val mb = folder.mapBuilder()
        row.iterator.foreach { case (name, v) => mb.add(name, foldValue(v, None, folder)) }
        mb.finish()
      case other => folder.string(other.toString)
    }

    /** Convert a parquet4s Value to a string for use as a MAP key.
      * Strict UTF-8 decoding for binary keys — malformed bytes throw rather than silently
      * produce replacement characters, since corrupted map keys can cause subtle bugs.
      */
    private def valueToString(value: PqValue): String = value match {
      case BinaryValue(v) => decodeStrictUtf8(v.getBytes)
      case IntValue(v) => v.toString
      case LongValue(v) => v.toString
      case other => other.toString
    }

    private def decodeStrictUtf8(bytes: Array[Byte]): String = {
      val decoder = StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
      decoder.decode(ByteBuffer.wrap(bytes)).toString
    }

    /** IEEE 754 half-precision (16-bit) float to double. */
    private def halfToDouble(half: Short): Double = {
      val h = half & 0xFFFF
      val sign = (h >>> 15) & 1
      val exponent = (h >>> 10) & 0x1F
      val mantissa = h & 0x3FF
      val value =
        if (exponent == 0) math.scalb(mantissa.toDouble / 1024.0, -14) // subnormal
        else if (exponent == 31) if (mantissa == 0) Double.PositiveInfinity else Double.NaN
        else math.scalb((mantissa.toDouble / 1024.0) + 1.0, exponent - 15)
      if (sign == 1) -value else value
    }
  }
}

package com.thatdot.quine.app.model.ingest2.codec

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.ActorAttributes
import org.apache.pekko.stream.scaladsl.Source

import dev.hardwood.metadata.{LogicalType, PhysicalType}
import dev.hardwood.reader.{ParquetFileReader, RowReader}
import dev.hardwood.row.{PqInterval, PqList, PqMap, PqStruct, PqVariant, StructAccessor, VariantType}
import dev.hardwood.schema.SchemaNode
import dev.hardwood.{HardwoodContext, InputFile}

import com.thatdot.data.DataFoldableFrom

/** A [[RandomAccessDecoder]] for Apache Parquet files.
  *
  * Each decoded row is a [[ParquetRecord]], which downstream consumers can fold through any
  * [[com.thatdot.data.DataFolderTo]] without going through a JSON intermediate.
  *
  * Rows are read on the blocking-io dispatcher because the reader does blocking I/O against the input.
  */
final class ParquetDecoder extends RandomAccessDecoder[ParquetRecord] {

  override val dataFoldableFrom: DataFoldableFrom[ParquetRecord] = ParquetRecord.foldable

  /** Returns a Source of the rows in `input`. A row whose values do not match the file's schema is emitted as a
    * `Failure`, and the Source continues with the next row. The Source fails if `input` does not have a readable
    * Parquet footer or if any part of the file cannot be read or decoded.
    */
  override def decode(input: SeekableInput): Source[Try[ParquetRecord], NotUsed] =
    Source
      .unfoldResource[Try[ParquetRecord], ParquetDecoder.State](
        create = () => ParquetDecoder.newState(input),
        read = _.read(),
        close = _.close(),
      )
      .withAttributes(ActorAttributes.dispatcher("pekko.actor.default-blocking-io-dispatcher"))
}

object ParquetDecoder {

  /** Hardwood's shared resources for reading Parquet files, which include a pool of daemon threads (one per CPU core)
    * for parallel decoding. If Quine never reads Parquet files, these threads are never initialized (because of
    * `lazy val`). Also, there is no need to ever `close` this context because the idle threads cost almost nothing.
    */
  private lazy val hardwoodContext: HardwoodContext = HardwoodContext.create()

  /** Returns a reader for a Parquet file at a local path. The caller is responsible for closing it.
    * This is only used by Delta Sharing ingest sources, and otherwise we could delete this.
    */
  private[ingest2] def open(file: java.nio.file.Path): ParquetFileReader =
    ParquetFileReader.open(InputFile.of(file), hardwoodContext)

  /** Returns the read state of a newly opened `input`. Throws, leaving nothing open, if the rows cannot be read. Creating
    * a row reader starts decoding, so a corrupt data page can cause this.
    */
  private def newState(input: SeekableInput): State = {
    val parquetInputFile = input match {
      case fileInput: FileSeekableInput => InputFile.of(fileInput.file.toPath)
      case other => new SeekableInputFile(other)
    }
    val file = ParquetFileReader.open(parquetInputFile, hardwoodContext)
    try {
      val columns = file.getFileSchema.getRootNode.children().asScala.toVector
      new State(file, file.rowReader(), columns)
    } catch {
      case NonFatal(error) =>
        try file.close()
        catch { case NonFatal(closeError) => error.addSuppressed(closeError) }
        throw error
    }
  }

  /** The state maintained while reading a single parquet file.
    * @note This class is not meant to be thread-safe.
    */
  final private class State(file: ParquetFileReader, rows: RowReader, columns: Vector[SchemaNode]) {

    /** Attempts to read the next Parquet row. Here's what each result means:
      *   - `Some(Success(r))` — The next row `r` was successfully read and is valid.
      *   - `Some(Failure(…))` — The next row was successfully read, but it's invalid (e.g., it doesn't match
      *                          the file's schema). Subsequent rows can still be read though.
      *   - `None`             — The end of the file has been reached.
      *   - exception thrown   — Hardwood failed to read the next row, thus subsequent rows cannot be read.
      */
    def read(): Option[Try[ParquetRecord]] =
      if (rows.hasNext) {
        rows.next()
        Some(Try(currentRow(rows, columns)))
      } else None

    def close(): Unit =
      try rows.close()
      finally file.close()
  }

  /** Adapts a [[SeekableInput]] to Hardwood's `InputFile`. Hardwood may call `readRange` concurrently from its decoding
    * threads, so reads of the single underlying stream are serialized.
    */
  final private class SeekableInputFile(input: SeekableInput) extends InputFile {
    private var stream: Option[SeekableInputStream] = None

    override def open(): Unit = synchronized {
      stream = Some(input.newStream())
    }

    override def readRange(offset: Long, length: Int): ByteBuffer = synchronized {
      val in = stream.getOrElse(throw new IllegalStateException("Parquet input has not been opened"))
      if (offset < 0L || length < 0 || offset + length > input.length)
        throw new IndexOutOfBoundsException(s"Range [$offset, ${offset + length}) is outside [0, ${input.length})")
      in.seek(offset)
      val bytes = new Array[Byte](length)
      val bytesRead = in.readNBytes(bytes, 0, length)
      if (bytesRead != length)
        throw new java.io.EOFException(s"Read $bytesRead of $length bytes at offset $offset")
      ByteBuffer.wrap(bytes)
    }

    override def length(): Long = input.length

    override def name(): String = input.getClass.getSimpleName

    override def close(): Unit = synchronized {
      stream.foreach(_.close())
      stream = None
    }
  }

  /** Fetches the row that the given row reader is currently pointed at. */
  private def currentRow(row: RowReader, columnSchemas: Vector[SchemaNode]): ParquetRecord =
    ParquetRecord(columnSchemas.zipWithIndex.map { case (columnSchema, i) =>
      row.getFieldName(i) -> (
        if (isNullTyped(columnSchema)) rawToParquetValue(row.getRawValue(i), columnSchema)
        else logicalToParquetValue(row.getValue(i), columnSchema)
      )
    })

  /** Returns the fields of `struct`, whose schema is `group`. Throws if a field is not in `group`. */
  private def structFields(struct: StructAccessor, group: SchemaNode.GroupNode): Vector[(String, ParquetValue)] = {
    val fieldSchemas: Map[String, SchemaNode] = group.children().asScala.map(c => c.name() -> c).toMap
    (0 until struct.getFieldCount).map { i =>
      val fieldName = struct.getFieldName(i)
      val schema = fieldSchemas.getOrElse(fieldName, throw schemaMismatch(group))
      fieldName -> (
        if (isNullTyped(schema)) rawToParquetValue(struct.getRawValue(i), schema)
        else logicalToParquetValue(struct.getValue(i), schema)
      )
    }.toVector
  }

  /** Returns the exception for a value whose shape does not match `schema`. */
  private def schemaMismatch(schema: SchemaNode): IllegalArgumentException =
    new IllegalArgumentException(s"Parquet value does not match the schema of field '${schema.name()}'")

  /** Whether the column has the logical type that Parquet calls UNKNOWN and Hardwood calls `NullType`. Values of such a
    * column must be read through Hardwood's raw-value accessors and converted by [[rawToParquetValue]].
    */
  private def isNullTyped(schema: SchemaNode): Boolean = schema match {
    case primitive: SchemaNode.PrimitiveNode => primitive.logicalType().isInstanceOf[LogicalType.NullType]
    case _ => false
  }

  /** Converts a value from one of Hardwood's logical-value accessors, which has the column's logical type applied, to a
    * [[ParquetValue]]. `schema`, which describes the value, is needed only where Quine's interpretation differs from
    * Hardwood's: JSON text is parsed when folded, ENUM bytes are text, and INT96 timestamps have no time zone. It also
    * supplies the schemas of nested values. Throws if the shape of `value` does not match `schema`.
    *
    * Callers must use [[rawToParquetValue]] for columns with the UNKNOWN logical type, including INTERVAL columns
    * written by parquet-java, because Hardwood's logical-value accessors throw on their non-null values.
    */
  private def logicalToParquetValue(value: Object, schema: SchemaNode): ParquetValue =
    value match {
      case null => ParquetValue.Null
      case v: java.lang.Boolean => ParquetValue.Bool(v)
      case v: java.lang.Float => ParquetValue.Floating(v.doubleValue)
      case v: java.lang.Double => ParquetValue.Floating(v)
      case v: java.math.BigDecimal => ParquetValue.Decimal(v)
      case v: java.lang.Number => ParquetValue.Integer(v.longValue)
      case v: String =>
        schema match {
          case p: SchemaNode.PrimitiveNode if p.logicalType().isInstanceOf[LogicalType.JsonType] =>
            ParquetValue.JsonText(v)
          case _ => ParquetValue.Text(v)
        }
      case v: Array[Byte] =>
        schema match {
          case p: SchemaNode.PrimitiveNode if p.logicalType().isInstanceOf[LogicalType.EnumType] =>
            ParquetValue.Text(new String(v, StandardCharsets.UTF_8))
          case _ => ParquetValue.Bytes(v)
        }
      case v: LocalDate => ParquetValue.Date(v)
      case v: LocalTime => ParquetValue.Time(v)
      case v: LocalDateTime => ParquetValue.LocalTimestamp(v)
      case v: Instant =>
        schema match {
          case p: SchemaNode.PrimitiveNode if p.`type`() == PhysicalType.INT96 =>
            ParquetValue.LocalTimestamp(LocalDateTime.ofInstant(v, ZoneOffset.UTC))
          case _ => ParquetValue.Timestamp(v)
        }
      case v: UUID => ParquetValue.Text(v.toString)
      case v: PqInterval => ParquetValue.Interval(v.months(), v.days(), v.milliseconds())
      case v: PqStruct =>
        schema match {
          case group: SchemaNode.GroupNode => ParquetValue.Struct(structFields(v, group))
          case _ => throw schemaMismatch(schema)
        }
      case v: PqList =>
        val elementSchema = schema match {
          case group: SchemaNode.GroupNode if group.isList =>
            Option(group.getListElement).getOrElse(throw schemaMismatch(schema))
          case other => other // A repeated field with no LIST annotation is its own element.
        }
        ParquetValue.ListValue(
          if (isNullTyped(elementSchema)) v.rawValues().asScala.map(rawToParquetValue(_, elementSchema)).toVector
          else v.values().asScala.map(logicalToParquetValue(_, elementSchema)).toVector,
        )
      case v: PqMap =>
        val group = schema match {
          case group: SchemaNode.GroupNode => group
          case _ => throw schemaMismatch(schema)
        }
        val keySchema = Option(group.getMapKey).getOrElse(throw schemaMismatch(schema))
        val maybeValueSchema = Option(group.getMapValue) // Parquet allows a map with keys and no value field.
        val keyIsBinary = keySchema match {
          case p: SchemaNode.PrimitiveNode => p.`type`() == PhysicalType.BYTE_ARRAY
          case _ => false
        }
        ParquetValue.MapValue(v.getEntries.asScala.map { entry =>
          // String keys stay as bytes here so that malformed UTF-8 can be rejected when the map is folded.
          val key =
            if (keyIsBinary) rawToParquetValue(entry.getRawKey, keySchema)
            else logicalToParquetValue(entry.getKey, keySchema)
          val value = maybeValueSchema.fold[ParquetValue](ParquetValue.Null) { valueSchema =>
            if (isNullTyped(valueSchema)) rawToParquetValue(entry.getRawValue, valueSchema)
            else logicalToParquetValue(entry.getValue, valueSchema)
          }
          key -> value
        }.toVector)
      case v: PqVariant => variantToParquetValue(v)
      case other => throw new IllegalArgumentException(s"Unsupported Parquet value of type ${other.getClass.getName}")
    }

  /** Converts a value from one of Hardwood's raw-value accessors, which returns the physical value with no logical type
    * applied, to a [[ParquetValue]]. `schema` describes the value.
    *
    * The raw accessors are needed for columns with the UNKNOWN logical type (`NullType` in Hardwood), because Hardwood
    * throws when it applies that logical type to a non-null value. Parquet reserves UNKNOWN for columns whose values are
    * all null, but parquet-java also writes it on INTERVAL columns, which hold real values. A 12-byte fixed-length value
    * in such a column is therefore returned as an [[ParquetValue.Interval]]. All other bytes are returned as
    * [[ParquetValue.Bytes]].
    */
  private def rawToParquetValue(value: Object, schema: SchemaNode): ParquetValue =
    value match {
      case null => ParquetValue.Null
      case v: java.lang.Boolean => ParquetValue.Bool(v)
      case v: java.lang.Float => ParquetValue.Floating(v.doubleValue)
      case v: java.lang.Double => ParquetValue.Floating(v)
      case v: java.lang.Number => ParquetValue.Integer(v.longValue)
      case v: Array[Byte] =>
        schema match {
          case p: SchemaNode.PrimitiveNode
              if isNullTyped(p) && p.`type`() == PhysicalType.FIXED_LEN_BYTE_ARRAY && v.length == 12 =>
            val buffer = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN)
            ParquetValue.Interval(
              Integer.toUnsignedLong(buffer.getInt),
              Integer.toUnsignedLong(buffer.getInt),
              Integer.toUnsignedLong(buffer.getInt),
            )
          case _ => ParquetValue.Bytes(v)
        }
      case other =>
        throw new IllegalArgumentException(s"Unsupported raw Parquet value of type ${other.getClass.getName}")
    }

  /** Converts a Parquet VARIANT value to a [[ParquetValue]] according to its own type tag: objects become structs,
    * arrays become lists, and other values become the matching scalar. Throws if the variant encoding is malformed.
    */
  private def variantToParquetValue(variant: PqVariant): ParquetValue =
    variant.`type`() match {
      case VariantType.NULL => ParquetValue.Null
      case VariantType.BOOLEAN_TRUE | VariantType.BOOLEAN_FALSE => ParquetValue.Bool(variant.asBoolean())
      case VariantType.INT8 | VariantType.INT16 | VariantType.INT32 | VariantType.INT64 =>
        ParquetValue.Integer(variant.asLong())
      case VariantType.FLOAT => ParquetValue.Floating(variant.asFloat().toDouble)
      case VariantType.DOUBLE => ParquetValue.Floating(variant.asDouble())
      case VariantType.DECIMAL4 | VariantType.DECIMAL8 | VariantType.DECIMAL16 =>
        ParquetValue.Decimal(variant.asDecimal())
      case VariantType.DATE => ParquetValue.Date(variant.asDate())
      case VariantType.TIME_NTZ => ParquetValue.Time(variant.asTime())
      case VariantType.TIMESTAMP | VariantType.TIMESTAMP_NANOS => ParquetValue.Timestamp(variant.asTimestamp())
      case VariantType.TIMESTAMP_NTZ | VariantType.TIMESTAMP_NTZ_NANOS =>
        ParquetValue.LocalTimestamp(variant.asLocalTimestamp())
      case VariantType.BINARY => ParquetValue.Bytes(variant.asBinary())
      case VariantType.STRING => ParquetValue.Text(variant.asString())
      case VariantType.UUID => ParquetValue.Text(variant.asUuid().toString)
      case VariantType.OBJECT =>
        val fields = variant.asObject()
        ParquetValue.Struct((0 until fields.getFieldCount).map { i =>
          val name = fields.getFieldName(i)
          // A field holding a variant null is returned as a Java null.
          name -> Option(fields.getVariant(name)).fold[ParquetValue](ParquetValue.Null)(variantToParquetValue)
        }.toVector)
      case VariantType.ARRAY => ParquetValue.ListValue(variant.asArray().asScala.map(variantToParquetValue).toVector)
    }
}

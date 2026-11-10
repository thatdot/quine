package com.thatdot.quine.app.model.ingest2.codec

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}

import io.circe.parser

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}

/** A decoded Parquet value, interpreted according to its column's physical and logical type.
  *
  * Values hold no reference to the reader that produced them, so they remain valid after the reader advances.
  */
sealed trait ParquetValue extends Product with Serializable

object ParquetValue {
  case object Null extends ParquetValue
  final case class Bool(value: Boolean) extends ParquetValue
  final case class Integer(value: Long) extends ParquetValue
  final case class Floating(value: Double) extends ParquetValue

  /** A value that folds to a string: STRING, ENUM, and UUID columns. */
  final case class Text(value: String) extends ParquetValue

  /** The text of a JSON column. It folds to the parsed JSON, or to a string if the text is not valid JSON. */
  final case class JsonText(value: String) extends ParquetValue

  /** Raw bytes. Two `Bytes` are equal when their contents are equal. */
  final case class Bytes(value: Array[Byte]) extends ParquetValue {
    override def equals(other: Any): Boolean = other match {
      case Bytes(otherValue) => java.util.Arrays.equals(value, otherValue)
      case _ => false
    }
    override def hashCode: Int = java.util.Arrays.hashCode(value)
  }

  /** A decimal. It folds to a string because [[DataFolderTo]] has no decimal method that preserves precision. */
  final case class Decimal(value: java.math.BigDecimal) extends ParquetValue
  final case class Date(value: LocalDate) extends ParquetValue
  final case class Time(value: LocalTime) extends ParquetValue

  /** A timestamp adjusted to UTC. */
  final case class Timestamp(value: Instant) extends ParquetValue

  /** A timestamp with no time zone. */
  final case class LocalTimestamp(value: LocalDateTime) extends ParquetValue
  final case class Interval(months: Long, days: Long, millis: Long) extends ParquetValue
  final case class ListValue(values: Vector[ParquetValue]) extends ParquetValue
  final case class MapValue(entries: Vector[(ParquetValue, ParquetValue)]) extends ParquetValue
  final case class Struct(fields: Vector[(String, ParquetValue)]) extends ParquetValue
}

/** A decoded Parquet row: its top-level columns in file order. Fold it through [[ParquetRecord.foldable]] to convert
  * it to any [[DataFolderTo]] target while preserving its types.
  */
final case class ParquetRecord(fields: Vector[(String, ParquetValue)])

object ParquetRecord {

  val foldable: DataFoldableFrom[ParquetRecord] = new DataFoldableFrom[ParquetRecord] {

    def fold[B](value: ParquetRecord, folder: DataFolderTo[B]): B = foldFields(value.fields, folder)

    private def foldFields[B](fields: Vector[(String, ParquetValue)], folder: DataFolderTo[B]): B = {
      val mb = folder.mapBuilder()
      fields.foreach { case (name, field) => mb.add(name, foldValue(field, folder)) }
      mb.finish()
    }

    private def foldValue[B](value: ParquetValue, folder: DataFolderTo[B]): B = value match {
      case ParquetValue.Null => folder.nullValue
      case ParquetValue.Bool(v) => if (v) folder.trueValue else folder.falseValue
      case ParquetValue.Integer(v) => folder.integer(v)
      case ParquetValue.Floating(v) => folder.floating(v)
      case ParquetValue.Text(v) => folder.string(v)
      case ParquetValue.JsonText(v) =>
        parser.parse(v) match {
          case Right(json) => DataFoldableFrom.jsonDataFoldable.fold(json, folder)
          case Left(_) => folder.string(v)
        }
      case ParquetValue.Bytes(v) => folder.bytes(v)
      case ParquetValue.Decimal(v) => folder.string(v.toString)
      case ParquetValue.Date(v) => folder.date(v)
      case ParquetValue.Time(v) => folder.localTime(v)
      case ParquetValue.Timestamp(v) => folder.zonedDateTime(ZonedDateTime.ofInstant(v, ZoneOffset.UTC))
      case ParquetValue.LocalTimestamp(v) => folder.localDateTime(v)
      case ParquetValue.Interval(months, days, millis) =>
        // No folder method covers an INTERVAL because months and days are calendrical, so all three components are kept.
        val mb = folder.mapBuilder()
        mb.add("months", folder.integer(months))
        mb.add("days", folder.integer(days))
        mb.add("millis", folder.integer(millis))
        mb.finish()
      case ParquetValue.ListValue(values) =>
        val vb = folder.vectorBuilder()
        values.foreach(v => vb.add(foldValue(v, folder)))
        vb.finish()
      case ParquetValue.MapValue(entries) =>
        val mb = folder.mapBuilder()
        entries.foreach { case (k, v) => mb.add(keyToString(k), foldValue(v, folder)) }
        mb.finish()
      case ParquetValue.Struct(fields) => foldFields(fields, folder)
    }

    /** Returns the string form of a MAP key. Binary keys are decoded as strict UTF-8, so malformed bytes throw instead
      * of silently producing replacement characters, because corrupted map keys can cause subtle bugs.
      */
    private def keyToString(key: ParquetValue): String = key match {
      case ParquetValue.Text(v) => v
      case ParquetValue.Bytes(v) => decodeStrictUtf8(v)
      case ParquetValue.Bool(v) => v.toString
      case ParquetValue.Integer(v) => v.toString
      case ParquetValue.Floating(v) => v.toString
      case ParquetValue.Decimal(v) => v.toString
      case ParquetValue.Date(v) => v.toString
      case ParquetValue.Time(v) => v.toString
      case ParquetValue.Timestamp(v) => v.toString
      case ParquetValue.LocalTimestamp(v) => v.toString
      case other => other.toString
    }

    private def decodeStrictUtf8(bytes: Array[Byte]): String = {
      val decoder = StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
      decoder.decode(ByteBuffer.wrap(bytes)).toString
    }
  }
}

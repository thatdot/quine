package com.thatdot.quine.ingest2

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.{ByteBuffer, ByteOrder}

/** Writes small Parquet files for tests that need generated rows, because Quine has no Parquet writer on its classpath.
  *
  * Each file has flat columns, one uncompressed row group, and one PLAIN-encoded data page per column. Tests of richer
  * files should use the fixtures in `src/test/resources/parquet`, which were written by parquet-java.
  */
object TestParquetWriter {

  sealed abstract class ColumnType(val physicalTypeId: Int)
  case object Int32 extends ColumnType(1)
  case object Int64 extends ColumnType(2)
  case object Utf8 extends ColumnType(6)

  final case class Column(name: String, columnType: ColumnType, optional: Boolean = true)

  /** Returns the bytes of a Parquet file. Each row holds one value per column, in column order: an `Int` for `Int32`, a
    * `Long` for `Int64`, a `String` for `Utf8`, or `null` in an optional column.
    */
  def write(columns: Seq[Column], rows: Seq[Seq[Any]]): Array[Byte] = {
    val file = new ByteArrayOutputStream()
    file.write(Magic)

    val chunks = columns.zipWithIndex.map { case (column, index) =>
      val page = dataPage(column, rows.map(_(index)))
      val header = new CompactThrift()
      header.i32(1, 0) // type: DATA_PAGE
      header.i32(2, page.length)
      header.i32(3, page.length)
      header.struct(5) { dataPageHeader =>
        dataPageHeader.i32(1, rows.size)
        dataPageHeader.i32(2, PlainEncoding)
        dataPageHeader.i32(3, RleEncoding)
        dataPageHeader.i32(4, RleEncoding)
      }
      header.stop()

      val offset = file.size.toLong
      file.write(header.toByteArray)
      file.write(page)
      (column, offset, (header.toByteArray.length + page.length).toLong)
    }

    val footer = new CompactThrift()
    footer.i32(1, 1) // version
    footer.structList[Option[Column]](2, None :: columns.map(Some(_)).toList) {
      case (element, None) =>
        element.string(4, "schema")
        element.i32(5, columns.size)
      case (element, Some(column)) =>
        element.i32(1, column.columnType.physicalTypeId)
        element.i32(3, if (column.optional) 1 else 0) // repetition: OPTIONAL or REQUIRED
        element.string(4, column.name)
        if (column.columnType == Utf8) element.i32(6, 0) // converted type: UTF8
    }
    footer.i64(3, rows.size.toLong)
    footer.structList[Unit](4, List(())) { (rowGroup, _) =>
      rowGroup.structList[(Column, Long, Long)](1, chunks.toList) { case (chunk, (column, offset, size)) =>
        chunk.i64(2, offset)
        chunk.struct(3) { metadata =>
          metadata.i32(1, column.columnType.physicalTypeId)
          metadata.i32List(2, List(PlainEncoding, RleEncoding))
          metadata.stringList(3, List(column.name))
          metadata.i32(4, 0) // codec: UNCOMPRESSED
          metadata.i64(5, rows.size.toLong)
          metadata.i64(6, size)
          metadata.i64(7, size)
          metadata.i64(9, offset)
        }
      }
      rowGroup.i64(2, chunks.map(_._3).sum)
      rowGroup.i64(3, rows.size.toLong)
    }
    footer.stop()

    val footerBytes = footer.toByteArray
    file.write(footerBytes)
    file.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).array())
    file.write(Magic)
    file.toByteArray
  }

  private val Magic = "PAR1".getBytes(UTF_8)
  private val PlainEncoding = 0
  private val RleEncoding = 3

  private def dataPage(column: Column, values: Seq[Any]): Array[Byte] = {
    val page = new ByteArrayOutputStream()
    if (column.optional) {
      // Definition levels: one RLE run per value, each with a run length of 1 and a 1-byte level.
      val levels = values.flatMap(value => Seq[Byte](2, if (value == null) 0 else 1)).toArray
      page.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(levels.length).array())
      page.write(levels)
    }
    values.filter(_ != null).foreach {
      case value: Int => page.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array())
      case value: Long => page.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array())
      case value: String =>
        val bytes = value.getBytes(UTF_8)
        page.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(bytes.length).array())
        page.write(bytes)
      case other => throw new IllegalArgumentException(s"Unsupported test value: $other")
    }
    page.toByteArray
  }

  /** The subset of Thrift's compact protocol that Parquet metadata uses. Fields must be written in increasing order. */
  final private class CompactThrift {
    private val out = new ByteArrayOutputStream()
    private var lastField = 0

    def toByteArray: Array[Byte] = out.toByteArray

    def stop(): Unit = out.write(0)

    private def varint(start: Long): Unit = {
      var value = start
      while ((value & ~0x7FL) != 0L) {
        out.write(((value & 0x7F) | 0x80).toInt)
        value >>>= 7
      }
      out.write(value.toInt)
    }

    private def zigzag(value: Long): Unit = varint((value << 1) ^ (value >> 63))

    private def fieldHeader(id: Int, typeId: Int): Unit = {
      out.write(((id - lastField) << 4) | typeId)
      lastField = id
    }

    private def listHeader(size: Int, elementTypeId: Int): Unit =
      if (size < 15) out.write((size << 4) | elementTypeId)
      else {
        out.write(0xF0 | elementTypeId)
        varint(size.toLong)
      }

    private def rawString(value: String): Unit = {
      val bytes = value.getBytes(UTF_8)
      varint(bytes.length.toLong)
      out.write(bytes)
    }

    private def nested(write: CompactThrift => Unit): Unit = {
      val child = new CompactThrift()
      write(child)
      child.stop()
      out.write(child.toByteArray)
    }

    def i32(id: Int, value: Int): Unit = {
      fieldHeader(id, 5)
      zigzag(value.toLong)
    }

    def i64(id: Int, value: Long): Unit = {
      fieldHeader(id, 6)
      zigzag(value)
    }

    def string(id: Int, value: String): Unit = {
      fieldHeader(id, 8)
      rawString(value)
    }

    def struct(id: Int)(write: CompactThrift => Unit): Unit = {
      fieldHeader(id, 12)
      nested(write)
    }

    def i32List(id: Int, values: List[Int]): Unit = {
      fieldHeader(id, 9)
      listHeader(values.size, 5)
      values.foreach(value => zigzag(value.toLong))
    }

    def stringList(id: Int, values: List[String]): Unit = {
      fieldHeader(id, 9)
      listHeader(values.size, 8)
      values.foreach(rawString)
    }

    def structList[A](id: Int, values: List[A])(write: (CompactThrift, A) => Unit): Unit = {
      fieldHeader(id, 9)
      listHeader(values.size, 12)
      values.foreach(value => nested(write(_, value)))
    }
  }
}

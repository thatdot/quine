package com.thatdot.quine.app.model.ingest2.sources

import java.io.PrintWriter
import java.nio.file.{Files, Path}
import java.util.{Comparator => JComparator, Optional}

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.github.mjakubowski84.parquet4s.{
  BinaryValue,
  BooleanValue,
  DoubleValue,
  FloatValue,
  IntValue,
  LongValue,
  NullValue,
  RowParquetRecord,
  Value => PqValue,
}
import io.circe.{Json, parser => circeParser}
import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.{Utils => KernelUtils}
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import com.thatdot.common.logging.Log._
import com.thatdot.quine.app.model.ingest2.codec.ParquetRecord

/** Reads Delta table data using Delta Kernel with deletion vector and column
  * mapping support. Returns [[ParquetRecord]] for direct folding to Cypher values.
  */
/** A data file to be read through Delta Kernel, with optional deletion vector. */
case class DeltaKernelFileEntry(
  dataBytes: Array[Byte],
  size: Long,
  partitionValues: Map[String, String],
  modificationTime: Long,
  deletionVector: Option[DeltaKernelDVEntry],
)

/** Deletion vector binary data and metadata for a file entry. */
case class DeltaKernelDVEntry(
  bytes: Array[Byte],
  offset: Long,
  sizeInBytes: Long,
  cardinality: Long,
)

object DeltaKernelReader extends LazySafeLogging {

  private lazy val engine: Engine = DefaultEngine.create(new Configuration())

  /** Read files from a constructed Delta log, applying deletion vectors.
    *
    * @param rawProtocolJson  Raw protocol NDJSON line from the server (Delta Sharing format)
    * @param rawMetadataJson  Raw metaData NDJSON line from the server (Delta Sharing format)
    * @param files            Structured file entries with data bytes and optional DV info
    * @return Rows as [[ParquetRecord]]
    */
  def readWithDeletionVectors(
    rawProtocolJson: String,
    rawMetadataJson: String,
    files: Seq[DeltaKernelFileEntry],
  ): Seq[ParquetRecord] = {
    val tempDir = Files.createTempDirectory("delta-kernel-")
    try {
      // Write data files and DV files to temp directory
      val actionJsons = files.zipWithIndex.map { case (entry, idx) =>
        val dataFileName = s"data-$idx.parquet"
        Files.write(tempDir.resolve(dataFileName), entry.dataBytes)

        val dvJson = entry.deletionVector match {
          case Some(dv) =>
            val dvFileName = s"deletion_vector_dv$idx.bin"
            Files.write(tempDir.resolve(dvFileName), dv.bytes)
            val dvAbsoluteUri = tempDir.resolve(dvFileName).toUri.toString
            s""","deletionVector":{"storageType":"p","pathOrInlineDv":"$dvAbsoluteUri","offset":${dv.offset},"sizeInBytes":${dv.sizeInBytes},"cardinality":${dv.cardinality}}"""
          case None => ""
        }

        val partitionValuesJson = Json
          .obj(
            entry.partitionValues.view.mapValues(Json.fromString).toSeq: _*,
          )
          .noSpaces
        s"""{"add":{"path":"$dataFileName","size":${entry.size},"partitionValues":$partitionValuesJson,"modificationTime":${entry.modificationTime},"dataChange":true$dvJson}}"""
      }

      // Write delta log
      val logDir = tempDir.resolve("_delta_log")
      Files.createDirectories(logDir)
      val logFile = logDir.resolve("00000000000000000000.json")
      val pw = new PrintWriter(logFile.toFile)
      try {
        pw.println(unwrapDeltaSharingProtocol(rawProtocolJson))
        pw.println(unwrapDeltaSharingMetadata(rawMetadataJson))
        actionJsons.foreach(pw.println)
      } finally pw.close()

      val table = io.delta.kernel.Table.forPath(engine, tempDir.toString)
      val snapshot = table.getLatestSnapshot(engine)
      val kernelSchema = snapshot.getSchema
      val scan = snapshot.getScanBuilder().build()
      val scanState = scan.getScanState(engine)
      val scanFiles = scan.getScanFiles(engine)

      val columnTypes = kernelSchemaToColumnTypes(kernelSchema)

      val rows = Seq.newBuilder[ParquetRecord]
      try while (scanFiles.hasNext) {
        val scanFilesBatch = scanFiles.next()
        val fileRows = scanFilesBatch.getRows
        try while (fileRows.hasNext) {
          val scanFileRow = fileRows.next()
          val physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState)
          val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)
          val physicalDataIter: CloseableIterator[ColumnarBatch] = engine.getParquetHandler
            .readParquetFiles(
              KernelUtils.singletonCloseableIterator(fileStatus),
              physicalReadSchema,
              Optional.empty(),
            )
          val physicalData = io.delta.kernel.Scan.transformPhysicalData(
            engine,
            scanState,
            scanFileRow,
            physicalDataIter,
          )
          try while (physicalData.hasNext) {
            val dataBatch = physicalData.next()
            val dataRows = dataBatch.getRows
            try while (dataRows.hasNext) {
              val row = dataRows.next()
              rows += kernelRowToParquetRecord(row, kernelSchema, columnTypes)
            } finally dataRows.close()
          } finally physicalData.close()
        } finally fileRows.close()
      } finally scanFiles.close()

      rows.result()
    } finally deleteRecursively(tempDir)
  }

  private def kernelSchemaToColumnTypes(
    schema: StructType,
  ): Map[String, ParquetRecord.ColumnDescriptor] =
    schema
      .fields()
      .asScala
      .collect { case field =>
        val (physicalType, logicalType) = kernelTypeToParquetType(field.getDataType)
        field.getName -> ParquetRecord.ColumnDescriptor(physicalType, logicalType)
      }
      .toMap

  private def kernelTypeToParquetType(
    dt: DataType,
  ): (PrimitiveTypeName, Option[LogicalTypeAnnotation]) =
    dt match {
      case _: BooleanType => (PrimitiveTypeName.BOOLEAN, None)
      case _: ByteType => (PrimitiveTypeName.INT32, Some(LogicalTypeAnnotation.intType(8, true)))
      case _: ShortType => (PrimitiveTypeName.INT32, Some(LogicalTypeAnnotation.intType(16, true)))
      case _: IntegerType => (PrimitiveTypeName.INT32, None)
      case _: LongType => (PrimitiveTypeName.INT64, None)
      case _: FloatType => (PrimitiveTypeName.FLOAT, None)
      case _: DoubleType => (PrimitiveTypeName.DOUBLE, None)
      case _: StringType => (PrimitiveTypeName.BINARY, Some(LogicalTypeAnnotation.stringType()))
      case _: BinaryType => (PrimitiveTypeName.BINARY, None)
      case _: DateType => (PrimitiveTypeName.INT32, Some(LogicalTypeAnnotation.dateType()))
      case _: TimestampType =>
        (
          PrimitiveTypeName.INT64,
          Some(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)),
        )
      case _: TimestampNTZType =>
        (
          PrimitiveTypeName.INT64,
          Some(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)),
        )
      case d: DecimalType =>
        (PrimitiveTypeName.BINARY, Some(LogicalTypeAnnotation.decimalType(d.getScale, d.getPrecision)))
      case _ => (PrimitiveTypeName.BINARY, Some(LogicalTypeAnnotation.stringType()))
    }

  private def kernelRowToParquetRecord(
    row: Row,
    schema: StructType,
    columnTypes: Map[String, ParquetRecord.ColumnDescriptor],
  ): ParquetRecord = {
    var record = RowParquetRecord.emptyWithSchema(schema.fieldNames().asScala.toSeq: _*)
    schema.fields().asScala.zipWithIndex.foreach { case (field, ordinal) =>
      val value =
        if (row.isNullAt(ordinal)) NullValue
        else kernelValueToParquetValue(row, ordinal, field.getDataType)
      record = record.updated(field.getName, value)
    }
    ParquetRecord(record, columnTypes)
  }

  private def kernelValueToParquetValue(row: Row, ordinal: Int, dt: DataType): PqValue =
    dt match {
      case _: BooleanType => BooleanValue(row.getBoolean(ordinal))
      case _: ByteType => IntValue(row.getByte(ordinal).toInt)
      case _: ShortType => IntValue(row.getShort(ordinal).toInt)
      case _: IntegerType => IntValue(row.getInt(ordinal))
      case _: LongType => LongValue(row.getLong(ordinal))
      case _: FloatType => FloatValue(row.getFloat(ordinal))
      case _: DoubleType => DoubleValue(row.getDouble(ordinal))
      case _: StringType => BinaryValue(row.getString(ordinal))
      case _: BinaryType => BinaryValue(row.getBinary(ordinal))
      case _: DateType => IntValue(row.getInt(ordinal))
      case _: TimestampType => LongValue(row.getLong(ordinal))
      case _: TimestampNTZType => LongValue(row.getLong(ordinal))
      case _: DecimalType => BinaryValue(row.getDecimal(ordinal).toPlainString)
      case st: StructType =>
        val structRow = row.getStruct(ordinal)
        st.fields()
          .asScala
          .zipWithIndex
          .foldLeft(
            RowParquetRecord.emptyWithSchema(st.fieldNames().asScala.toSeq: _*),
          ) { case (rec, (f, i)) =>
            val v =
              if (structRow.isNullAt(i)) NullValue
              else kernelValueToParquetValue(structRow, i, f.getDataType)
            rec.updated(f.getName, v)
          }
      case _ => BinaryValue(row.getString(ordinal))
    }

  /** Convert Delta Sharing protocol JSON to standard Delta log format.
    * Delta Sharing: {"protocol":{"deltaProtocol":{"minReaderVersion":3,...}}}
    * Delta log:     {"protocol":{"minReaderVersion":3,...}}
    */
  private def unwrapDeltaSharingProtocol(raw: String): String =
    circeParser
      .parse(raw)
      .toOption
      .flatMap { json =>
        val inner = json.hcursor.downField("protocol").downField("deltaProtocol")
        if (inner.succeeded) inner.focus.map(dp => Json.obj("protocol" -> dp).noSpaces)
        else None
      }
      .getOrElse(raw)

  /** Convert Delta Sharing metadata JSON to standard Delta log format.
    * Delta Sharing: {"metaData":{"deltaMetadata":{"id":"...","schemaString":"...",...}}}
    * Delta log:     {"metaData":{"id":"...","schemaString":"...",...}}
    */
  private def unwrapDeltaSharingMetadata(raw: String): String =
    circeParser
      .parse(raw)
      .toOption
      .flatMap { json =>
        val inner = json.hcursor.downField("metaData").downField("deltaMetadata")
        if (inner.succeeded) inner.focus.map(dm => Json.obj("metaData" -> dm).noSpaces)
        else None
      }
      .getOrElse(raw)

  private def deleteRecursively(path: Path): Unit =
    Try {
      Files
        .walk(path)
        .sorted(JComparator.reverseOrder[Path]())
        .forEach { p =>
          val _ = Files.deleteIfExists(p)
        }
    }.failed.foreach { e =>
      logger.warn(safe"Failed to clean up temp directory ${Safe(path.toString)}: ${Safe(e.getMessage)}")
    }
}

package com.thatdot.quine.app.model.ingest2.sources

import java.io.{IOException, PrintWriter, RandomAccessFile}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.util.{Comparator => JComparator, Optional}

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import dev.hardwood.reader.{ParquetFileReader, RowReader}
import dev.hardwood.row.{PqList, PqMap, PqStruct, StructAccessor}
import io.circe.{Json, parser => circeParser}
import io.delta.kernel.data.{ArrayValue, ColumnVector, ColumnarBatch, FilteredColumnarBatch, MapValue, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.engine.fileio.{FileIO, InputFile, OutputFile, SeekableInputStream}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.engine.{Engine, ParquetHandler}
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.{Utils => KernelUtils}
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus, FileStatus}

import com.thatdot.common.logging.Log._
import com.thatdot.quine.app.model.ingest2.codec.{ParquetDecoder, ParquetRecord, ParquetValue}

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

  /** A Kernel engine that needs neither Hadoop nor parquet-java. It reads local files through [[LocalFileIO]] and
    * Parquet through [[HardwoodParquetHandler]], and inherits its JSON, expression, and file system handlers from
    * `DefaultEngine`.
    */
  private lazy val engine: Engine = new DefaultEngine(new LocalFileIO) {
    override def getParquetHandler: ParquetHandler = new HardwoodParquetHandler
  }

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
          try while (physicalData.hasNext)
            rows ++= batchToRecords(physicalData.next(), kernelSchema)
          finally physicalData.close()
        } finally fileRows.close()
      } finally scanFiles.close()

      rows.result()
    } finally deleteRecursively(tempDir)
  }

  /** Returns the rows of `batch` that its selection vector keeps. `schema` gives the column names to use. */
  private def batchToRecords(batch: FilteredColumnarBatch, schema: StructType): Seq[ParquetRecord] = {
    val data = batch.getData
    val selection = batch.getSelectionVector
    val fields = schema.fields().asScala.toVector
    val columns = fields.indices.map(data.getColumnVector).toVector
    (0 until data.getSize)
      .filter(row => !selection.isPresent || (!selection.get.isNullAt(row) && selection.get.getBoolean(row)))
      .map { row =>
        ParquetRecord(fields.zip(columns).map { case (field, column) =>
          field.getName -> kernelToParquetValue(column, row, field.getDataType)
        })
      }
  }

  /** Returns the value at position `i` of a Kernel vector whose element type is `dt`. */
  private def kernelToParquetValue(vector: ColumnVector, i: Int, dt: DataType): ParquetValue =
    if (vector.isNullAt(i)) ParquetValue.Null
    else
      dt match {
        case _: BooleanType => ParquetValue.Bool(vector.getBoolean(i))
        case _: ByteType => ParquetValue.Integer(vector.getByte(i).toLong)
        case _: ShortType => ParquetValue.Integer(vector.getShort(i).toLong)
        case _: IntegerType => ParquetValue.Integer(vector.getInt(i).toLong)
        case _: LongType => ParquetValue.Integer(vector.getLong(i))
        case _: FloatType => ParquetValue.Floating(vector.getFloat(i).toDouble)
        case _: DoubleType => ParquetValue.Floating(vector.getDouble(i))
        case _: StringType => ParquetValue.Text(vector.getString(i))
        case _: BinaryType => ParquetValue.Bytes(vector.getBinary(i))
        case _: DateType => ParquetValue.Date(LocalDate.ofEpochDay(vector.getInt(i).toLong))
        case _: TimestampType => ParquetValue.Timestamp(microsToInstant(vector.getLong(i)))
        case _: TimestampNTZType =>
          ParquetValue.LocalTimestamp(LocalDateTime.ofInstant(microsToInstant(vector.getLong(i)), ZoneOffset.UTC))
        case _: DecimalType => ParquetValue.Decimal(vector.getDecimal(i))
        case struct: StructType =>
          ParquetValue.Struct(struct.fields().asScala.toVector.zipWithIndex.map { case (field, ordinal) =>
            field.getName -> kernelToParquetValue(vector.getChild(ordinal), i, field.getDataType)
          })
        case array: ArrayType =>
          val value = vector.getArray(i)
          val elements = value.getElements
          ParquetValue.ListValue(
            (0 until value.getSize).map(j => kernelToParquetValue(elements, j, array.getElementType)).toVector,
          )
        case map: MapType =>
          val value = vector.getMap(i)
          val (keys, values) = (value.getKeys, value.getValues)
          ParquetValue.MapValue((0 until value.getSize).map { j =>
            kernelToParquetValue(keys, j, map.getKeyType) -> kernelToParquetValue(values, j, map.getValueType)
          }.toVector)
        case other => throw new UnsupportedOperationException(s"Unsupported Delta column type: $other")
      }

  private def microsToInstant(micros: Long): Instant =
    Instant.ofEpochSecond(Math.floorDiv(micros, 1000000L), Math.floorMod(micros, 1000000L) * 1000L)

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

  private def localPath(path: String): Path =
    if (path.startsWith("file:")) Paths.get(URI.create(path)) else Paths.get(path)

  /** The read-only local file access that Kernel's default handlers need. It replaces Kernel's `HadoopFileIO`. */
  final private class LocalFileIO extends FileIO {

    private def status(path: Path): FileStatus =
      FileStatus.of(path.toString, Files.size(path), Files.getLastModifiedTime(path).toMillis)

    /** Returns the files in the directory of `filePath` whose names sort at or after its name, sorted by name. */
    override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
      val from = localPath(filePath)
      val siblings = Files.list(from.getParent)
      val statuses =
        try siblings.iterator.asScala
          .filter(_.getFileName.toString >= from.getFileName.toString)
          .toVector
          .sortBy(_.getFileName.toString)
          .map(status)
        finally siblings.close()
      KernelUtils.toCloseableIterator(statuses.asJava.iterator)
    }

    override def getFileStatus(path: String): FileStatus = status(localPath(path))

    override def resolvePath(path: String): String = path

    override def newInputFile(filePath: String, fileSize: Long): InputFile = new InputFile {
      override def length(): Long = fileSize
      override def path(): String = filePath
      override def newStream(): SeekableInputStream = new SeekableInputStream {
        private val file = new RandomAccessFile(localPath(filePath).toFile, "r")
        override def read(): Int = file.read()
        override def read(b: Array[Byte], off: Int, len: Int): Int = file.read(b, off, len)
        override def getPos: Long = file.getFilePointer
        override def seek(pos: Long): Unit = file.seek(pos)
        override def readFully(b: Array[Byte], off: Int, len: Int): Unit = file.readFully(b, off, len)
        override def close(): Unit = file.close()
      }
    }

    override def getConf(confKey: String): Optional[String] = Optional.empty()

    override def mkdirs(path: String): Boolean = throw new IOException("Read-only file access: mkdirs")
    override def newOutputFile(path: String): OutputFile =
      throw new UnsupportedOperationException("Read-only file access: newOutputFile")
    override def delete(path: String): Boolean = throw new IOException("Read-only file access: delete")
  }

  /** Reads Parquet data files for Kernel through Hardwood. Rows are never filtered, so a row's position in the file,
    * which Kernel uses to apply deletion vectors, is its position in the output.
    */
  final private class HardwoodParquetHandler extends ParquetHandler {

    /** Returns batches holding the requested columns of `files`, in order. A column that is absent from a file is null
      * in that file's rows. `predicate` is only a hint to skip data, so it is ignored.
      */
    override def readParquetFiles(
      files: CloseableIterator[FileStatus],
      schema: StructType,
      predicate: Optional[Predicate],
    ): CloseableIterator[ColumnarBatch] = new CloseableIterator[ColumnarBatch] {
      private var current: Option[OpenDataFile] = None

      override def hasNext: Boolean = {
        while (!current.exists(_.rows.hasNext) && files.hasNext) {
          closeCurrent()
          current = Some(openDataFile(files.next().getPath))
        }
        current.exists(_.rows.hasNext)
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) throw new NoSuchElementException("No more Parquet rows")
        current.get.nextBatch(schema)
      }

      private def closeCurrent(): Unit = {
        current.foreach(_.close())
        current = None
      }

      override def close(): Unit =
        try closeCurrent()
        finally files.close()
    }

    override def writeParquetFiles(
      directory: String,
      data: CloseableIterator[FilteredColumnarBatch],
      statsColumns: java.util.List[Column],
    ): CloseableIterator[DataFileStatus] =
      throw new UnsupportedOperationException("Quine does not write Parquet files")

    override def writeParquetFileAtomically(path: String, data: CloseableIterator[FilteredColumnarBatch]): Unit =
      throw new UnsupportedOperationException("Quine does not write Parquet files")
  }

  /** Returns the read position at the start of the data file at `path`. Throws, leaving nothing open, if the rows
    * cannot be read. Creating a row reader starts decoding, so a corrupt data page can cause this.
    */
  private def openDataFile(path: String): OpenDataFile = {
    val file = ParquetDecoder.open(localPath(path))
    try {
      val columnNames = file.getFileSchema.getRootNode.children().asScala.map(_.name()).toSet
      new OpenDataFile(file, file.rowReader(), columnNames)
    } catch {
      case NonFatal(error) =>
        try file.close()
        catch { case NonFatal(closeError) => error.addSuppressed(closeError) }
        throw error
    }
  }

  /** The read position in one data file, whose top-level column names are `columnNames`. */
  final private class OpenDataFile(file: ParquetFileReader, val rows: RowReader, columnNames: Set[String]) {
    private var rowIndex: Long = 0L

    /** Returns a batch of up to [[BatchSize]] rows, advancing past them. */
    def nextBatch(schema: StructType): ColumnarBatch = {
      val fields = schema.fields().asScala.toVector
      val columns = fields.map(_ => new java.util.ArrayList[Object](BatchSize))
      var size = 0
      while (size < BatchSize && rows.hasNext) {
        rows.next()
        fields.zip(columns).foreach { case (field, column) =>
          val _ = column.add(
            if (field.getName == StructField.METADATA_ROW_INDEX_COLUMN_NAME) java.lang.Long.valueOf(rowIndex)
            else readField(rows, columnNames, field),
          )
        }
        rowIndex += 1L
        size += 1
      }
      val vectors: Array[ColumnVector] = fields
        .zip(columns)
        .map { case (field, column) => DefaultGenericVector.fromList(field.getDataType, column): ColumnVector }
        .toArray
      new DefaultColumnarBatch(size, schema, vectors)
    }

    def close(): Unit =
      try rows.close()
      finally file.close()
  }

  private val BatchSize = 4096

  private def fieldNames(struct: StructAccessor): Set[String] =
    (0 until struct.getFieldCount).map(struct.getFieldName).toSet

  private def readField(struct: StructAccessor, available: Set[String], field: StructField): Object =
    if (available.contains(field.getName)) hardwoodToKernelValue(struct.getValue(field.getName), field.getDataType)
    else null

  /** Converts a value returned by Hardwood to the representation Kernel's vectors expect for `dt`. Numbers are
    * converted to the exact boxed type of `dt` so that files written before a type-widening schema change stay readable.
    */
  private def hardwoodToKernelValue(value: Any, dt: DataType): Object =
    (value, dt) match {
      case (null, _) => null
      case (v: java.lang.Boolean, _: BooleanType) => v
      case (v: java.lang.Number, _: ByteType) => java.lang.Byte.valueOf(v.byteValue)
      case (v: java.lang.Number, _: ShortType) => java.lang.Short.valueOf(v.shortValue)
      case (v: java.lang.Number, _: IntegerType) => java.lang.Integer.valueOf(v.intValue)
      case (v: java.lang.Number, _: LongType) => java.lang.Long.valueOf(v.longValue)
      case (v: java.lang.Number, _: FloatType) => java.lang.Float.valueOf(v.floatValue)
      case (v: java.lang.Number, _: DoubleType) => java.lang.Double.valueOf(v.doubleValue)
      case (v: java.math.BigDecimal, _: DecimalType) => v
      case (v: String, _: StringType) => v
      case (v: Array[Byte], _: StringType) => new String(v, StandardCharsets.UTF_8) // An ENUM column.
      case (v: Array[Byte], _: BinaryType) => v
      case (v: LocalDate, _: DateType) => java.lang.Integer.valueOf(Math.toIntExact(v.toEpochDay))
      case (v: Instant, _: TimestampType | _: TimestampNTZType) => java.lang.Long.valueOf(instantToMicros(v))
      case (v: LocalDateTime, _: TimestampType | _: TimestampNTZType) =>
        java.lang.Long.valueOf(instantToMicros(v.toInstant(ZoneOffset.UTC)))
      case (v: java.lang.Long, _: TimestampType | _: TimestampNTZType) => v // A column with no timestamp annotation.
      case (v: PqStruct, struct: StructType) =>
        val available = fieldNames(v)
        new StructValue(struct, struct.fields().asScala.map(readField(v, available, _)).toArray)
      case (v: PqList, array: ArrayType) =>
        val elements = v.values().asScala.map(hardwoodToKernelValue(_, array.getElementType)).asJava
        val vector = DefaultGenericVector.fromList(array.getElementType, elements)
        new ArrayValue {
          override def getSize: Int = elements.size
          override def getElements: ColumnVector = vector
        }
      case (v: PqMap, map: MapType) =>
        val entries = v.getEntries.asScala.toVector
        val keys = entries.map(e => hardwoodToKernelValue(e.getKey, map.getKeyType)).asJava
        val values = entries.map(e => hardwoodToKernelValue(e.getValue, map.getValueType)).asJava
        val keyVector = DefaultGenericVector.fromList(map.getKeyType, keys)
        val valueVector = DefaultGenericVector.fromList(map.getValueType, values)
        new MapValue {
          override def getSize: Int = entries.size
          override def getKeys: ColumnVector = keyVector
          override def getValues: ColumnVector = valueVector
        }
      case (other, _) =>
        throw new UnsupportedOperationException(s"Cannot read Parquet value of type ${other.getClass.getName} as $dt")
    }

  /** A nested struct value. Kernel's own `GenericRow` cannot be used because it rejects `getInt` on a date field and
    * `getLong` on a timestamp field, which is how Kernel's vectors read those types.
    */
  final private class StructValue(schema: StructType, values: Array[Object]) extends Row {
    override def getSchema: StructType = schema
    override def isNullAt(ordinal: Int): Boolean = values(ordinal) == null
    override def getBoolean(ordinal: Int): Boolean = values(ordinal).asInstanceOf[Boolean]
    override def getByte(ordinal: Int): Byte = values(ordinal).asInstanceOf[Byte]
    override def getShort(ordinal: Int): Short = values(ordinal).asInstanceOf[Short]
    override def getInt(ordinal: Int): Int = values(ordinal).asInstanceOf[Int]
    override def getLong(ordinal: Int): Long = values(ordinal).asInstanceOf[Long]
    override def getFloat(ordinal: Int): Float = values(ordinal).asInstanceOf[Float]
    override def getDouble(ordinal: Int): Double = values(ordinal).asInstanceOf[Double]
    override def getString(ordinal: Int): String = values(ordinal).asInstanceOf[String]
    override def getDecimal(ordinal: Int): java.math.BigDecimal = values(ordinal).asInstanceOf[java.math.BigDecimal]
    override def getBinary(ordinal: Int): Array[Byte] = values(ordinal).asInstanceOf[Array[Byte]]
    override def getStruct(ordinal: Int): Row = values(ordinal).asInstanceOf[Row]
    override def getArray(ordinal: Int): ArrayValue = values(ordinal).asInstanceOf[ArrayValue]
    override def getMap(ordinal: Int): MapValue = values(ordinal).asInstanceOf[MapValue]
  }

  private def instantToMicros(instant: Instant): Long =
    Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L)
}

package com.thatdot.quine.app.model.ingest2.sources

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}

import com.github.mjakubowski84.parquet4s.{BinaryValue, ParquetIterable, ParquetReader, RowParquetRecord}
import io.circe.Json

import com.thatdot.common.logging.Log._
import com.thatdot.data.DataFoldableFrom
import com.thatdot.data.DataFoldableFrom.byteStringDataFoldable
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest2.codec.{BufferedSeekableInput, ParquetDecoder, ParquetRecord}
import com.thatdot.quine.app.model.ingest2.source.DecodedSource
import com.thatdot.quine.app.model.ingest2.{DeltaSharingAuth, OAuthCertificateAuth}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.MemberIdx
import com.thatdot.quine.persistor.PrimePersistor

/** State tracked across poll iterations. */
sealed private[sources] trait DeltaSharingPollPhase
private[sources] object DeltaSharingPollPhase {
  case object NeedsValidation extends DeltaSharingPollPhase
  case class EmittingSnapshot(version: Long, cdfEnabled: Boolean) extends DeltaSharingPollPhase
  case class Polling(lastConsumedVersion: Long) extends DeltaSharingPollPhase
  case object Completed extends DeltaSharingPollPhase
}

sealed private[sources] trait PollResult
private[sources] object PollResult {
  case class SnapshotFiles(
    files: Seq[DeltaSharingFileAction],
    snapshotVersion: Long,
    rawProtocolJson: Option[String] = None,
    rawMetadataJson: Option[String] = None,
    rawActionJsons: Seq[String] = Seq.empty,
  ) extends PollResult
  case class CdfFiles(
    files: Seq[DeltaSharingFileAction],
    endVersion: Long,
    rawProtocolJson: Option[String] = None,
    rawMetadataJson: Option[String] = None,
    rawActionJsons: Seq[String] = Seq.empty,
  ) extends PollResult
  case object Empty extends PollResult
}

/** Frame type that carries version information for ack tracking.
  *
  * Each row emitted from the source carries the version it belongs to.
  * The ack flow uses this to track which version was last fully confirmed
  * by the Cypher ingest query. The poll loop reads the confirmed version
  * to know where to resume from.
  */
private[sources] case class DeltaSharingFrame(
  version: Long,
  bytes: ByteString,
)

/** Ingest source that continuously polls a Delta Sharing server for CDF events.
  *
  * === Cursor advancement ===
  *
  * The version cursor is advanced via the `ackFlow`, which runs AFTER the
  * Cypher ingest query has confirmed success for each row. The ack flow
  * tracks the highest version for which all rows have been confirmed.
  * The poll loop reads this confirmed version to determine where to
  * request changes from next.
  *
  * This is analogous to Kafka offset commits: the offset is only committed
  * after the record has been fully processed.
  */
case class DeltaSharingCdfSource(
  ingestName: String,
  endpoint: String,
  auth: DeltaSharingAuth,
  shareName: String,
  schemaName: String,
  tableName: String,
  startingVersion: Option[Long],
  snapshotOnFirstRun: Boolean,
  pollIntervalMs: Long,
  maxVersionsPerPoll: Int,
  serverRequestTimeoutMs: Long,
  parquetFetchTimeoutMs: Long,
  maxRetries: Int,
  skipUnreadableVersions: Boolean,
  parquetDownloadParallelism: Int,
  responseFormat: Option[String] = None,
  meter: IngestMeter,
  persistor: Option[(PrimePersistor, MemberIdx)] = None,
  certTokenAcquirer: Option[OAuthCertificateAuth => Future[String]] = None,
)(implicit val system: ActorSystem)
    extends LazySafeLogging {

  implicit private val ec: ExecutionContext = system.dispatcher

  private val useDeltaFormat: Boolean = responseFormat.exists(_.equalsIgnoreCase("delta"))
  private val autoNegotiate: Boolean = responseFormat.isEmpty

  private val client = new DeltaSharingClient(
    endpoint = endpoint,
    auth = auth,
    serverTimeoutMs = serverRequestTimeoutMs,
    parquetFetchTimeoutMs = parquetFetchTimeoutMs,
    maxRetries = maxRetries,
    requestDeltaFormat = useDeltaFormat || autoNegotiate,
    certTokenAcquirer = certTokenAcquirer,
  )

  private val pollInterval: FiniteDuration = pollIntervalMs.millis
  private val effectiveParallelism: Int = math.max(1, parquetDownloadParallelism)

  /** Persistence key for the version cursor. Scoped by ingest stream coordinates
    * so multiple ingests against different tables don't collide.
    */
  private val cursorKey: String =
    s"delta-sharing-cursor:$ingestName:$shareName/$schemaName/$tableName"

  /** The last version confirmed by the ack flow (after Cypher execution).
    * The poll loop reads this to know which versions have been fully processed.
    * Initialized to -1 (no version confirmed yet); overwritten by persisted
    * value on startup if available.
    */
  private val confirmedVersion = new AtomicLong(-1L)

  /** Persist the confirmed version to the persistence agent, scoped by member index. */
  private def persistConfirmedVersion(version: Long): Unit =
    persistor.foreach { case (p, memberIdx) =>
      val bytes = ByteBuffer.allocate(8).putLong(version).array()
      p.setLocalMetaData(cursorKey, memberIdx, Some(bytes))
    // Fire-and-forget: persistence is best-effort. If it fails, the worst
    // case is re-processing some versions on restart (at-least-once).
    }

  /** Load the persisted confirmed version, if any. */
  private def loadPersistedVersion(): Future[Option[Long]] =
    persistor match {
      case Some((p, memberIdx)) =>
        p.getLocalMetaData(cursorKey, memberIdx)
          .map(_.map { bytes =>
            ByteBuffer.wrap(bytes).getLong
          })
      case None =>
        Future.successful(None)
    }

  def decodedSource: DecodedSource =
    new DecodedSource(meter) {
      type Decoded = ParquetRecord
      type Frame = DeltaSharingFrame

      override val foldableFrame: DataFoldableFrom[DeltaSharingFrame] = deltaFrameFoldable
      override val foldable: DataFoldableFrom[ParquetRecord] = ParquetRecord.foldable

      override def content(input: DeltaSharingFrame): Array[Byte] =
        input.bytes.toArrayUnsafe()

      def stream: Source[(() => Try[ParquetRecord], Frame), ShutdownSwitch] = {
        val initialPhase: DeltaSharingPollPhase = DeltaSharingPollPhase.NeedsValidation

        val elementStream: Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] =
          Source
            .unfoldAsync[DeltaSharingPollPhase, PollResult](initialPhase)(pollStep)
            .flatMapConcat {
              case PollResult.Empty =>
                Source.empty

              case PollResult.SnapshotFiles(files, version, rawProto, rawMeta, rawActions) =>
                processFiles(files, version, rawProto, rawMeta, rawActions)

              case PollResult.CdfFiles(fileActions, _, rawProto, rawMeta, rawActions) =>
                val orderedFiles: Seq[DeltaSharingFileAction] =
                  fileActions
                    .groupBy(_.version)
                    .toSeq
                    .sortBy(_._1)
                    .flatMap(_._2)
                processFilesOrdered(orderedFiles, rawProto, rawMeta, rawActions)
            }

        withKillSwitches(elementStream)
      }

      /** Ack flow: updates the confirmed version after each row is
        * successfully processed by the Cypher ingest query.
        *
        * This flow runs at DecodedSource line 210, AFTER the Cypher query
        * at line 191. Each frame carries its version number. We update the
        * confirmed version atomically — since rows are processed in version
        * order (enforced by flatMapConcat upstream), the version number
        * monotonically increases.
        */
      override val ack: Flow[DeltaSharingFrame, Done, NotUsed] =
        Flow[DeltaSharingFrame].map { frame =>
          val previous = confirmedVersion.getAndUpdate(current => math.max(current, frame.version))
          // Persist when the confirmed version advances
          if (frame.version > previous) {
            persistConfirmedVersion(frame.version)
          }
          Done
        }
    }

  /** DataFoldableFrom for DeltaSharingFrame — delegates to ByteString foldable. */
  private val deltaFrameFoldable: DataFoldableFrom[DeltaSharingFrame] =
    new DataFoldableFrom[DeltaSharingFrame] {
      import com.thatdot.data.DataFolderTo
      def fold[B](value: DeltaSharingFrame, folder: DataFolderTo[B]): B =
        byteStringDataFoldable.fold(value.bytes, folder)
    }

  /** Process files for snapshot (all files share the same version). */
  private def processFiles(
    files: Seq[DeltaSharingFileAction],
    version: Long,
    rawProtocolJson: Option[String],
    rawMetadataJson: Option[String],
    rawActionJsons: Seq[String],
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] = {
    // If any file has deletion vectors, use Delta Kernel for the batch
    val hasDVs = files.exists(DeltaSharingFileAction.hasDeletionVector)
    if (hasDVs && rawProtocolJson.isDefined && rawMetadataJson.isDefined) {
      processFilesWithKernel(files, version, rawProtocolJson.get, rawMetadataJson.get, rawActionJsons)
    } else {
      processFilesPlainParquet(files, version)
    }
  }

  /** Process CDF files in strict version order with prefetch.
    *
    * Handles three action types:
    *   - cdc/cdf: CDF change files (plain Parquet with _change_type columns)
    *   - add: data files (may have deletion vectors, need Kernel)
    *   - remove: deleted files — in delta format, a remove with dataChange=true
    *     means the rows in the referenced file were deleted. We download the file,
    *     read its rows, and emit them with _change_type="delete" added synthetically.
    */
  private def processFilesOrdered(
    orderedFiles: Seq[DeltaSharingFileAction],
    rawProtocolJson: Option[String],
    rawMetadataJson: Option[String],
    rawActionJsons: Seq[String],
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] = {
    // Partition by action type
    val cdfFiles = orderedFiles.filter(a => a.actionType == "cdc" || a.actionType == "cdf")
    val addFiles = orderedFiles.filter(a => a.actionType == "add" || a.actionType == "file")
    val removeFiles = orderedFiles.filter(a => a.actionType == "remove")

    // CDF files always use plain Parquet
    val cdfSource = processFilesPlainParquet(cdfFiles, -1L)

    // Remove actions: download the file, read rows, inject _change_type="delete"
    val removeSource = processRemoveActions(removeFiles)

    // Add files: use Kernel if any have DVs, otherwise plain Parquet
    val hasDVs = addFiles.exists(DeltaSharingFileAction.hasDeletionVector)
    val addSource =
      if (hasDVs && rawProtocolJson.isDefined && rawMetadataJson.isDefined) {
        val versionGroups = addFiles.groupBy(_.version).toSeq.sortBy(_._1)
        Source(versionGroups.toList).flatMapConcat { case (version, filesInVersion) =>
          processFilesWithKernel(
            filesInVersion,
            version,
            rawProtocolJson.get,
            rawMetadataJson.get,
            rawActionJsons.filter(line => filesInVersion.exists(f => line.contains(f.url))),
          )
        }
      } else {
        processFilesPlainParquet(addFiles, -1L)
      }

    // Process in order: CDF files, then remove actions, then add files
    cdfSource.concat(removeSource).concat(addSource)
  }

  /** Process remove actions by downloading the referenced files, reading their
    * rows, and emitting each row with _change_type="delete" injected.
    */
  private def processRemoveActions(
    removeFiles: Seq[DeltaSharingFileAction],
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] =
    if (removeFiles.isEmpty) Source.empty
    else {
      Source(removeFiles.toList)
        .mapAsync(effectiveParallelism) { action =>
          client
            .fetchParquetFile(action.url)
            .map(bytes => (action, Right(bytes): Either[Throwable, Array[Byte]]))
            .recover { case e: Throwable => (action, Left(e)) }
        }
        .flatMapConcat {
          case (action, Right(bytes)) =>
            // Read the Parquet file and inject _change_type="delete" into each row
            deserializeToElements(action, bytes, action.version).map { case (tryRecord, frame) =>
              val withDeleteType: () => Try[ParquetRecord] = () =>
                tryRecord().map { record =>
                  val updatedRow = record.record.updated("_change_type", BinaryValue("delete"))
                  record.copy(record = updatedRow)
                }
              (withDeleteType, frame)
            }
          case (action, Left(fetchError)) =>
            emitFileError(action, action.version, "fetch_remove", fetchError)
        }
    }

  /** Standard Parquet deserialization path (no deletion vectors). */
  private def processFilesPlainParquet(
    files: Seq[DeltaSharingFileAction],
    defaultVersion: Long,
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] =
    Source(files.toList)
      .mapAsync(effectiveParallelism) { action =>
        client
          .fetchParquetFile(action.url)
          .map(bytes => (action, Right(bytes): Either[Throwable, Array[Byte]]))
          .recover { case e: Throwable => (action, Left(e)) }
      }
      .flatMapConcat {
        case (action, Right(bytes)) =>
          this.meter.mark(bytes.length)
          val v = if (defaultVersion >= 0) defaultVersion else action.version
          deserializeToElements(action, bytes, v)
        case (action, Left(fetchError)) =>
          val v = if (defaultVersion >= 0) defaultVersion else action.version
          emitFileError(action, v, "fetch", fetchError)
      }

  /** Delta Kernel deserialization path for files with deletion vectors.
    * Downloads all files in the batch, constructs a temp Delta log, and
    * uses Kernel to read with DV filtering applied.
    */
  private def processFilesWithKernel(
    files: Seq[DeltaSharingFileAction],
    version: Long,
    rawProtocolJson: String,
    rawMetadataJson: String,
    rawActionJsons: Seq[String],
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] =
    Source
      .future {
        // Download all data files and DV files in parallel
        Future.sequence(files.map { action =>
          val dataFuture = client.fetchParquetFile(action.url)
          val dvFuture = action.deletionVectorFileUrl match {
            case Some(dvUrl) => client.fetchParquetFile(dvUrl).map(Some(_))
            case None => Future.successful(None)
          }
          for {
            dataBytes <- dataFuture
            dvBytes <- dvFuture
          } yield (action, dataBytes, dvBytes)
        })
      }
      .flatMapConcat { downloadedFiles =>
        try {
          val fileEntries = downloadedFiles.map { case (action, dataBytes, dvBytes) =>
            val dvEntry = dvBytes.map { bytes =>
              DeltaKernelDVEntry(
                bytes = bytes,
                offset = action.deletionVectorOffset.getOrElse(0L),
                sizeInBytes = action.deletionVectorSizeInBytes.getOrElse(0L),
                cardinality = action.deletionVectorCardinality.getOrElse(0L),
              )
            }
            DeltaKernelFileEntry(
              dataBytes = dataBytes,
              size = action.size,
              partitionValues = action.partitionValues,
              modificationTime = action.timestamp,
              deletionVector = dvEntry,
            )
          }

          val rows = DeltaKernelReader.readWithDeletionVectors(
            rawProtocolJson,
            rawMetadataJson,
            fileEntries,
          )

          Source(rows.toList).map { record =>
            val element: (() => Try[ParquetRecord], DeltaSharingFrame) =
              (() => Success(record), DeltaSharingFrame(version, ByteString.empty))
            element
          }
        } catch {
          case e: Exception =>
            val syntheticAction = files.headOption.getOrElse(
              DeltaSharingFileAction("add", "", 0, Map.empty, version, 0),
            )
            emitFileError(syntheticAction, version, "delta_kernel_read", e)
        }
      }

  /** Deserialize a Parquet file into stream elements, each tagged with
    * the version they belong to.
    */
  private def deserializeToElements(
    action: DeltaSharingFileAction,
    bytes: Array[Byte],
    version: Long,
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] = {
    val rowSource: Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] =
      try {
        val seekable = new BufferedSeekableInput(bytes)
        val columnTypes = ParquetRecord.readColumnTypes(seekable)
        val inputFile = ParquetDecoder.toParquetInputFile(seekable)

        Source
          .unfoldResource[ParquetRecord, (ParquetIterable[RowParquetRecord], Iterator[RowParquetRecord])](
            create = () => {
              val iterable = ParquetReader.generic.read(inputFile)
              (iterable, iterable.iterator)
            },
            read = { case (_, iter) =>
              if (iter.hasNext) Some(ParquetRecord(iter.next(), columnTypes))
              else None
            },
            close = { case (iterable, _) => iterable.close() },
          )
          .map[(() => Try[ParquetRecord], DeltaSharingFrame)](record =>
            (() => Success(record), DeltaSharingFrame(version, ByteString.empty)),
          )
      } catch {
        case e: Exception =>
          emitFileError(action, version, "deserialize", e)
      }

    if (skipUnreadableVersions) {
      rowSource.recoverWithRetries(
        1,
        { case e: Exception =>
          emitFileError(action, version, "deserialize_mid_stream", e)
        },
      )
    } else {
      rowSource
    }
  }

  /** Emit a single Failure record for a file-level error. The error detail
    * JSON is placed in the Frame so the DLQ receives full diagnostic context.
    *
    * If `skipUnreadableVersions` is false, the error record is emitted (for
    * the DLQ) and then the stream fails. If true, just the error record
    * is emitted and the stream continues.
    */
  private def emitFileError(
    action: DeltaSharingFileAction,
    version: Long,
    errorPhase: String,
    error: Throwable,
  ): Source[(() => Try[ParquetRecord], DeltaSharingFrame), NotUsed] = {
    val errorDetail = fileErrorJson(action, errorPhase, error)
    val frame = DeltaSharingFrame(version, ByteString(errorDetail.noSpaces))
    val element: (() => Try[ParquetRecord], DeltaSharingFrame) = (
      () => Failure(new DeltaSharingException(s"Failed to $errorPhase file: ${error.getMessage}", error)),
      frame,
    )
    if (skipUnreadableVersions) {
      Source.single(element)
    } else {
      Source
        .single(element)
        .concat(
          Source.failed(
            new DeltaSharingException(
              s"Failed to $errorPhase CDF file for version $version. " +
              s"Set 'skipUnreadableVersions' to true to skip and continue. " +
              s"Error: ${error.getMessage}",
              error,
            ),
          ),
        )
    }
  }

  private def fileErrorJson(
    action: DeltaSharingFileAction,
    errorPhase: String,
    error: Throwable,
  ): Json =
    Json.obj(
      "_error" -> Json.fromString(error.getMessage),
      "_errorPhase" -> Json.fromString(errorPhase),
      "_errorType" -> Json.fromString(error.getClass.getName),
      "_version" -> Json.fromLong(action.version),
      "_fileUrl" -> Json.fromString(action.url),
      "_fileSize" -> Json.fromLong(action.size),
      "_actionType" -> Json.fromString(action.actionType),
      "_shareName" -> Json.fromString(shareName),
      "_schemaName" -> Json.fromString(schemaName),
      "_tableName" -> Json.fromString(tableName),
      "_endpoint" -> Json.fromString(endpoint),
    )

  // ── Polling state machine ──

  private def pollStep(
    phase: DeltaSharingPollPhase,
  ): Future[Option[(DeltaSharingPollPhase, PollResult)]] =
    phase match {
      case DeltaSharingPollPhase.NeedsValidation =>
        validateAndInitialize()
      case DeltaSharingPollPhase.EmittingSnapshot(version, cdfEnabled) =>
        emitSnapshot(version, cdfEnabled)
      case DeltaSharingPollPhase.Polling(lastFetchedVersion) =>
        pollForChanges(lastFetchedVersion)
      case DeltaSharingPollPhase.Completed =>
        Future.successful(None)
    }

  private def validateAndInitialize(): Future[Option[(DeltaSharingPollPhase, PollResult)]] =
    loadPersistedVersion().flatMap { persistedVersion =>
      // If we have a persisted cursor, use it as the starting point
      // (overrides startingVersion config — the persisted value is more recent)
      persistedVersion.foreach { v =>
        confirmedVersion.set(v)
        logger.info(safe"Delta Sharing: resuming from persisted version ${Safe(v.toString)}")
      }

      client.queryTableMetadata(shareName, schemaName, tableName).flatMap { metadataResponse =>
        validateMetadata(metadataResponse)
        val cdfEnabled = isCdfEnabled(metadataResponse)

        // Determine the effective starting version:
        // 1. Persisted version (highest priority — represents confirmed progress)
        // 2. User-configured startingVersion
        // 3. Current live version from server
        val effectiveStartVersion: Future[Option[Long]] = persistedVersion match {
          case Some(v) => Future.successful(Some(v))
          case None =>
            startingVersion match {
              case some @ Some(_) => Future.successful(some)
              case None => Future.successful(None)
            }
        }

        effectiveStartVersion.flatMap { startVer =>
          if (snapshotOnFirstRun && persistedVersion.isEmpty) {
            // Only snapshot on first run if we have no persisted cursor
            // (if we're resuming, we don't need to re-snapshot)
            client.queryTableVersion(shareName, schemaName, tableName).map { currentVersion =>
              val v = startVer.getOrElse(currentVersion)
              Some((DeltaSharingPollPhase.EmittingSnapshot(v, cdfEnabled), PollResult.Empty))
            }
          } else if (cdfEnabled) {
            startVer match {
              case Some(v) =>
                confirmedVersion.compareAndSet(-1L, v)
                Future.successful(Some((DeltaSharingPollPhase.Polling(v), PollResult.Empty)))
              case None =>
                client.queryTableVersion(shareName, schemaName, tableName).map { currentVersion =>
                  confirmedVersion.compareAndSet(-1L, currentVersion)
                  Some((DeltaSharingPollPhase.Polling(currentVersion), PollResult.Empty))
                }
            }
          } else {
            logger.warn(
              safe"Delta Sharing: CDF is disabled and snapshotOnFirstRun is false; completing stream with no data.",
            )
            Future.successful(None)
          }
        }
      }
    }

  private def emitSnapshot(
    version: Long,
    cdfEnabled: Boolean,
  ): Future[Option[(DeltaSharingPollPhase, PollResult)]] =
    client.queryTableSnapshot(shareName, schemaName, tableName).map { response =>
      val nextPhase =
        if (cdfEnabled) DeltaSharingPollPhase.Polling(version)
        else DeltaSharingPollPhase.Completed
      Some(
        (
          nextPhase,
          PollResult.SnapshotFiles(
            response.files,
            version,
            response.rawProtocolJson,
            response.rawMetadataJson,
            response.rawActionJsons,
          ),
        ),
      )
    }

  /** Steady-state CDF polling.
    *
    * Uses `confirmedVersion` (updated by the ack flow after Cypher execution)
    * rather than the unfoldAsync state to determine which version to fetch
    * next. This ensures we only advance past versions whose rows have been
    * fully confirmed by the Cypher ingest query.
    *
    * The `lastFetchedVersion` in the Polling state tracks what we've fetched
    * from the server, but the CONFIRMED version (from the ack flow) is what
    * we use to determine the next startingVersion for `/changes` calls.
    */
  private def pollForChanges(
    lastFetchedVersion: Long,
  ): Future[Option[(DeltaSharingPollPhase, PollResult)]] = {
    // Use the confirmed version as the baseline — this is the last version
    // for which all rows have been confirmed by the Cypher query.
    val lastConfirmed = confirmedVersion.get()
    // Use the higher of confirmed and fetched (on first poll, confirmed may
    // not be set yet so we fall back to lastFetchedVersion)
    val baseline = math.max(lastConfirmed, lastFetchedVersion)

    client.queryTableVersion(shareName, schemaName, tableName).flatMap { remoteVersion =>
      if (remoteVersion <= baseline) {
        org.apache.pekko.pattern
          .after(pollInterval, system.scheduler)(
            Future.successful(Some((DeltaSharingPollPhase.Polling(baseline), PollResult.Empty))),
          )
      } else {
        val endVersion = math.min(remoteVersion, baseline + maxVersionsPerPoll)
        client
          .queryTableChanges(
            shareName,
            schemaName,
            tableName,
            startingVersion = baseline + 1,
            endingVersion = Some(endVersion),
          )
          .map { response =>
            Some(
              (
                DeltaSharingPollPhase.Polling(endVersion),
                PollResult.CdfFiles(
                  response.files,
                  endVersion,
                  response.rawProtocolJson,
                  response.rawMetadataJson,
                  response.rawActionJsons,
                ),
              ),
            )
          }
          .recoverWith {
            case e: DeltaSharingException if isDeletionVectorError(e) =>
              Future.failed(
                new DeltaSharingException(
                  s"Cannot read CDF changes for versions ${baseline + 1} to $endVersion " +
                  s"because some versions use deletion vectors (responseFormat=delta required). " +
                  s"Last confirmed version: $lastConfirmed. " +
                  s"To resolve this:\n" +
                  s"  1. ALTER TABLE <table> SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')\n" +
                  s"  2. REORG TABLE <table> APPLY (PURGE)\n" +
                  s"  3. Note the version: DESCRIBE HISTORY <table> LIMIT 5\n" +
                  s"  4. Set 'startingVersion' to that version and 'snapshotOnFirstRun' to true\n" +
                  s"Original error: ${e.getMessage}",
                ),
              )
          }
      }
    }
  }

  private def isDeletionVectorError(e: DeltaSharingException): Boolean = {
    val msg = e.getMessage.toLowerCase
    msg.contains("deletionvectors") || msg.contains("deletion_vectors") || msg.contains("deletion vectors")
  }

  private def validateMetadata(response: DeltaSharingMetadataResponse): Unit = {
    // No fatal validation — CDF being disabled is handled by the state machine:
    // snapshot if snapshotOnFirstRun=true, then complete (no polling).
  }

  private def isCdfEnabled(response: DeltaSharingMetadataResponse): Boolean = {
    val config = response.metadata.configuration
    val cdfSetting = config
      .get("delta.enableChangeDataFeed")
      .orElse(config.get("enableChangeDataFeed"))
    !cdfSetting.contains("false")
  }
}

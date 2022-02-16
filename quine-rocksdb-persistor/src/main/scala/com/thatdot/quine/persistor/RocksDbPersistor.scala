package com.thatdot.quine.persistor

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.StampedLock
import java.util.{Arrays, ConcurrentModificationException, UUID}

import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import org.rocksdb.{
  BuiltinComparator,
  ColumnFamilyDescriptor,
  ColumnFamilyHandle,
  ColumnFamilyOptions,
  DBOptions,
  Options,
  ReadOptions,
  RocksDB,
  RocksIterator,
  Slice,
  WriteOptions
}

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, StandingQuery, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.QuineId

/** Embedded persistence implementation based on RocksDB
  *
  * @param filePath path to the RocksDB folder
  * @param writeAheadLog whether to enable the WAL (enable if you want to avoid data loss on crash)
  * @param syncWrites whether to sync fully to the OS the write (much slower, but no data loss on power failure)
  * @param dbOptionProperties free-form properties for the DB (see `DBOptions.getDBOptionsFromProps`)
  * @param persistenceConfig configuration for persistence
  */
final class RocksDbPersistor(
  filePath: String,
  writeAheadLog: Boolean,
  syncWrites: Boolean,
  dbOptionProperties: java.util.Properties,
  val persistenceConfig: PersistenceConfig
)(implicit
  actorSystem: ActorSystem
) extends PersistenceAgent {

  /* TODO: which other `DBOptions` should we expose? Maybe `setIncreaseParallelism` (as per the
   * docs: "You almost definitely want to call this function if your system is bottlenecked by
   * RocksDB")?
   *
   * TODO: which other column family options should we set/expose? Some candidates:
   *   - `setNumLevels`
   *   - `setCompressionType`
   *   - `optimizeLevelStyleCompaction`
   *
   * TODO: should we use [prefix-seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek)? Does
   * that even work in the presence of variable length keys?
   */

  import RocksDbPersistor._

  implicit val ioDispatcher: ExecutionContext =
    actorSystem.dispatchers.lookup("akka.quine.persistor-blocking-dispatcher")

  /* All mutable fields below are mutated only when this lock is held exclusively
   *
   *   - "Regular" DB operations (`put`, `delete`, `seek`, etc.) which can occur concurrently
   *     acquire the lock non-exclusively
   *
   *   - "Global" DB operations (reset & shutdown) which involve mutating the fields below acquire
   *     the lock exclusively
   *
   * The purpose behind all of this is to make it impossible to have a regular DB operation occur
   * while something like a `reset` is underway. That sort of situation is undefined behaviour in
   * RocksDB and [may cause a segfault][0].
   *
   * As a reminder: the write lock enforces the memory synchronization we need to ensure that
   * subsequent read or write locks will see the up-to-date versions of the mutable fields below,
   * even though they are not volatile.
   *
   * [0]: https://github.com/facebook/rocksdb/issues/5234
   */
  private[this] val dbLock: StampedLock = new StampedLock()

  // RocksDB top-level
  private[this] var db: RocksDB = _
  private[this] var dbOpts: DBOptions = _
  private[this] var columnFamilyOpts: ColumnFamilyOptions = _
  private[this] var writeOpts: WriteOptions = _

  // How many times have we reset? This lets us detect when an iterator is invalidated by a reset.
  private[this] var dbResetCount: Int = 0

  // Column families
  private[this] var journalsCF: ColumnFamilyHandle = _
  private[this] var snapshotsCF: ColumnFamilyHandle = _
  private[this] var standingQueriesCF: ColumnFamilyHandle = _
  private[this] var standingQueryStatesCF: ColumnFamilyHandle = _
  private[this] var metaDataCF: ColumnFamilyHandle = _
  private[this] var defaultCF: ColumnFamilyHandle = _

  // Initialize the DB
  {
    RocksDB.loadLibrary()
    val stamp = dbLock.writeLock
    openRocksDB()
    dbLock.unlockWrite(stamp) // Intentionally don't unlock if there is an intervening crash!
  }

  /** Open (synchronously) a new Rocks DB instance, overwriting stored state
    *
    * @note this should only be called from a thread that holds [[dbLock]] (or the constructor)
    * @see [[https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families]]
    */
  private[this] def openRocksDB() = {
    // Technically, I don't think these ever need to change - they could be immutable
    dbOpts = (if (dbOptionProperties.isEmpty) new DBOptions() else DBOptions.getDBOptionsFromProps(dbOptionProperties))
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)
    columnFamilyOpts = new ColumnFamilyOptions()
      .optimizeUniversalStyleCompaction()
      .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR)
    writeOpts = new WriteOptions()
      .setDisableWAL(!writeAheadLog)
      .setSync(syncWrites)

    // Define column family options
    val journalsDesc = new ColumnFamilyDescriptor("journals".getBytes(UTF_8), columnFamilyOpts)
    val snapshotsDesc = new ColumnFamilyDescriptor("snapshots".getBytes(UTF_8), columnFamilyOpts)
    val standingQueriesDesc = new ColumnFamilyDescriptor("standing-queries".getBytes(UTF_8), columnFamilyOpts)
    val standingQueryStatesDesc = new ColumnFamilyDescriptor("standing-query-states".getBytes(UTF_8), columnFamilyOpts)
    val metaDataDesc = new ColumnFamilyDescriptor("meta-data".getBytes(UTF_8), columnFamilyOpts)
    val defaultDesc = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOpts)

    // Make the column families
    val columnFamilyDescs =
      java.util.Arrays.asList(
        journalsDesc,
        snapshotsDesc,
        standingQueriesDesc,
        standingQueryStatesDesc,
        metaDataDesc,
        defaultDesc
      )
    val columnFamilyHandles = new java.util.ArrayList[ColumnFamilyHandle]()
    db = RocksDB.open(dbOpts, filePath, columnFamilyDescs, columnFamilyHandles)

    journalsCF = columnFamilyHandles.get(0)
    snapshotsCF = columnFamilyHandles.get(1)
    standingQueriesCF = columnFamilyHandles.get(2)
    standingQueryStatesCF = columnFamilyHandles.get(3)
    metaDataCF = columnFamilyHandles.get(4)
    defaultCF = columnFamilyHandles.get(5)
  }

  /** Close (synchronously) the RocksDB
    *
    * @note this should only be called from a thread that holds [[dbLock]] (or the constructor)
    * @see [[https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families]]
    */
  private[this] def closeRocksDB(): Unit = {
    db.cancelAllBackgroundWork(true)

    // Order matters
    journalsCF.close()
    snapshotsCF.close()
    standingQueriesCF.close()
    standingQueryStatesCF.close()
    metaDataCF.close()
    defaultCF.close()
    db.close()
    dbOpts.close()
    columnFamilyOpts.close()

    // Just to be safe - using these objects after closing them means risking a segfault!
    journalsCF = null
    snapshotsCF = null
    standingQueriesCF = null
    standingQueryStatesCF = null
    metaDataCF = null
    defaultCF = null
    db = null
    dbOpts = null
    columnFamilyOpts = null
  }

  /** Write (synchronously) a key value pair into the column family
    *
    * @param columnFamily column family into which to write
    * @param key data key
    * @param value data to write
    */
  private[this] def putKeyValue(
    columnFamily: ColumnFamilyHandle,
    key: Array[Byte],
    value: Array[Byte]
  ): Unit = {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try db.put(columnFamily, writeOpts, key, value)
    finally dbLock.unlockRead(stamp)
  }

  /** Remove (synchronously) a key from the column family
    *
    * @param columnFamily column family from which to remove
    * @param key data key
    */
  private[this] def removeKey(
    columnFamily: ColumnFamilyHandle,
    key: Array[Byte]
  ): Unit = {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try db.delete(columnFamily, writeOpts, key)
    finally dbLock.unlockRead(stamp)
  }

  /** Get (synchronously) a key from the column family
    *
    * @param columnFamily column family from which to get
    * @param key data key
    */
  private[this] def getKey(
    columnFamily: ColumnFamilyHandle,
    key: Array[Byte]
  ): Option[Array[Byte]] = {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try Option(db.get(columnFamily, key))
    finally dbLock.unlockRead(stamp)
  }

  override def emptyOfQuineData()(implicit ec: ExecutionContext): Future[Boolean] = {
    def columnFamilyIsEmpty(cf: ColumnFamilyHandle): Boolean = {
      val it = db.newIterator(cf)
      try {
        it.seekToFirst()
        !it.isValid // the iterator is valid iff the column family is nonempty
      } finally it.close()
    }

    // on the io dispatcher: check that each column family is empty
    Future {
      val stamp = dbLock.tryReadLock()
      if (stamp == 0) throw new RocksDBUnavailableException()
      try columnFamilyIsEmpty(snapshotsCF) &&
      columnFamilyIsEmpty(journalsCF) &&
      columnFamilyIsEmpty(standingQueriesCF) &&
      columnFamilyIsEmpty(standingQueryStatesCF)
      finally dbLock.unlockRead(stamp)
    }(ioDispatcher)
  }

  def persistEvent(id: QuineId, atTime: EventTime, event: NodeChangeEvent): Future[Unit] = Future {
    val eventBytes = PersistenceCodecs.eventFormat.write(event)
    putKeyValue(journalsCF, qidAndTime2Key(id, atTime), eventBytes)
  }

  def persistSnapshot(id: QuineId, atTime: EventTime, snapshotBytes: Array[Byte]): Future[Unit] = Future {
    putKeyValue(snapshotsCF, qidAndTime2Key(id, atTime), snapshotBytes)
  }

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] = Future {
    val sqBytes = PersistenceCodecs.standingQueryFormat.write(standingQuery)
    putKeyValue(standingQueriesCF, standingQuery.name.getBytes(UTF_8), sqBytes)
  }

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = Future {
    val keyBytes = key.getBytes(UTF_8)
    newValue match {
      case None => removeKey(metaDataCF, keyBytes)
      case Some(valBytes) => putKeyValue(metaDataCF, keyBytes, valBytes)
    }
  }

  def setStandingQueryState(
    sqId: StandingQueryId,
    qid: QuineId,
    sqPartId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = Future {
    val keyBytes = sqIdQidAndSqPartId2Key(sqId, qid, sqPartId)
    state match {
      case None => removeKey(standingQueryStatesCF, keyBytes)
      case Some(stateBytes) => putKeyValue(standingQueryStatesCF, keyBytes, stateBytes)
    }
  }

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] = Future {
    val beginKey = sqIdPrefixKey(standingQuery.id)
    val endKeyOpt = incrementKey(beginKey)
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try {
      db.delete(standingQueriesCF, writeOpts, standingQuery.name.getBytes(UTF_8))
      val endKey = endKeyOpt match {
        case Some(endKey) => endKey

        // Very unlikely edge case - see "Use with `incrementKey`" scaladoc on `sqIdPrefixKey`
        case None =>
          val it = db.newIterator(standingQueryStatesCF)
          try {
            it.seekToLast()
            val lastKey = it.key()
            Arrays.copyOf(lastKey, lastKey.length + 1) // a key that is bigger than the last key
          } finally it.close()
      }
      db.deleteRange(standingQueryStatesCF, writeOpts, beginKey, endKey)
    } finally dbLock.unlockRead(stamp)
  }

  def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] = Future {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try {
      val mb = Map.newBuilder[(StandingQueryId, StandingQueryPartId), Array[Byte]]
      val it = db.newIterator(standingQueryStatesCF)
      try {
        it.seekToFirst()
        var noMoreSqs: Boolean = false
        while (it.isValid && !noMoreSqs) {

          // Advance the iterator to the right QuineId for this SQ
          val sqId = key2SqId(it.key())
          it.seek(sqIdAndQidPrefixKey(sqId, id))

          // Collect all the SQ parts for this SQ & QuineId
          var sqPartId: StandingQueryPartId = StandingQueryPartId(new UUID(0L, 0L))
          while (
            it.isValid && {
              val (sqId2, qid, sqPartId1) = key2SqIdQidAndSqPartId(it.key())
              sqPartId = sqPartId1

              // Check that standing query ID and QuineId are still what we want.
              // If they aren't, make sure the iterator is advanced to a new standing query ID
              sqId == sqId2 && {
                (qid == id) || {
                  incrementKey(sqIdPrefixKey(sqId)) match {
                    case Some(nextSqId) => it.seek(nextSqId)

                    // Very unlikely edge case - see "Use with `incrementKey`" scaladoc on `sqIdPrefixKey`
                    case None => noMoreSqs = true
                  }
                  false
                }
              }
            }
          )
            mb += (sqId -> sqPartId) -> it.value()
        }
      } finally it.close()
      mb.result()
    } finally dbLock.unlockRead(stamp)
  }

  def getMetaData(key: String): Future[Option[Array[Byte]]] = Future {
    getKey(metaDataCF, key.getBytes(UTF_8))
  }

  def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Vector[NodeChangeEvent]] = Future {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try {
      val vb = Vector.newBuilder[NodeChangeEvent]

      // Inclusive start key
      val startKey = qidAndTime2Key(id, startingAt)

      // Non-inclusive end key (see ReadOptions.setIterateUpperBound)
      val endKey = endingAt match {
        case EventTime.MaxValue => qidBytes2NextKey(id.array)
        case _ => qidAndTime2Key(id, endingAt.nextEventTime)
      }

      val readOptions = new ReadOptions().setIterateUpperBound(new Slice(endKey))
      val it = db.newIterator(journalsCF, readOptions)
      try {
        it.seek(startKey)
        while (it.isValid) {
          vb += PersistenceCodecs.eventFormat.read(it.value()).get
          it.next()
        }
      } finally {
        it.close()
        readOptions.close()
      }
      vb.result()
    } finally dbLock.unlockRead(stamp)
  }

  def getStandingQueries: Future[List[StandingQuery]] = Future {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try {
      val lb = List.newBuilder[StandingQuery]
      val it = db.newIterator(standingQueriesCF)
      try {
        it.seekToFirst()
        while (it.isValid) {
          lb += PersistenceCodecs.standingQueryFormat.read(it.value()).get
          it.next()
        }
      } finally it.close()
      lb.result()
    } finally dbLock.unlockRead(stamp)
  }

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = Future {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try {
      val mb = Map.newBuilder[String, Array[Byte]]
      val it = db.newIterator(metaDataCF)
      try {
        it.seekToFirst()
        while (it.isValid) {
          mb += new String(it.key(), UTF_8) -> it.value()
          it.next()
        }
      } finally it.close()
      mb.result()
    } finally dbLock.unlockRead(stamp)
  }

  def getLatestSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[(EventTime, Array[Byte])]] = Future {
    val stamp = dbLock.tryReadLock()
    if (stamp == 0) throw new RocksDBUnavailableException()
    try {
      val it = db.newIterator(snapshotsCF)
      try {
        val startKey = qidAndTime2Key(id, upToTime)
        it.seekForPrev(startKey)
        if (it.isValid && java.util.Arrays.equals(id.array, key2QidBytes(it.key()))) {
          val bb = ByteBuffer.wrap(it.key())
          val time = bb.getLong
          Some(EventTime(time) -> it.value())
        } else {
          None
        }
      } finally it.close()
    } finally dbLock.unlockRead(stamp)
  }

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] =
    enumerateIds(snapshotsCF)

  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] =
    enumerateIds(journalsCF)

  /** Iterate (asynchronously) through the ID part of keys in a column family
    *
    * @param columnFamily column family through which to iterate
    */
  private[this] def enumerateIds(columnFamily: ColumnFamilyHandle): Source[QuineId, NotUsed] = {
    val resetCnt = dbResetCount

    Source
      .unfoldResource[QuineId, RocksIterator](
        create = { () =>
          val stamp = dbLock.tryReadLock()
          if (stamp == 0) throw new RocksDBUnavailableException()
          if (resetCnt != dbResetCount) throw new ConcurrentModificationException("RocksDB has been reset")
          try {
            val it = db.newIterator(columnFamily)
            it.seekToFirst()
            it
          } finally dbLock.unlockRead(stamp)
        },
        read = { (it: RocksIterator) =>
          val stamp = dbLock.tryReadLock()
          if (stamp == 0) throw new RocksDBUnavailableException()
          if (resetCnt != dbResetCount) throw new ConcurrentModificationException("RocksDB has been reset")
          try if (!it.isValid) None
          else {
            val qidBytes = key2QidBytes(it.key())
            it.seek(qidBytes2NextKey(qidBytes))
            Some(QuineId(qidBytes))
          } finally dbLock.unlockRead(stamp)
        },
        close = _.close()
      )
  }

  private[this] def shutdownSync(): Unit = {
    val stamp = dbLock.tryWriteLock(1, TimeUnit.MINUTES)
    if (stamp == 0)
      throw new RocksDBUnavailableException(
        "RocksDB is not currently available (or is under too much load to be closed)"
      )
    closeRocksDB()
    // Intentionally leave the lock permanently exclusively acquired!
  }

  def shutdown(): Future[Unit] = Future(shutdownSync())

  def delete(): Unit = {
    shutdownSync()
    logger.info(s"Destroying RocksDB at $filePath...")
    RocksDB.destroyDB(filePath, new Options())
    logger.info(s"Destroyed RocksDB at $filePath.")
  }

}

object RocksDbPersistor {

  /* Note about keys encodings
   * =========================
   *
   * RocksDB keys are always just arrays of bytes (recommended not to exceed 8MB, the shorter the
   * better). This means that when we have maps with multiple keys, we need to encode those keys
   * into a single `Array[Byte]`. Although RocksDB supports custom comparators implemented in Java
   * (see `AbstractComparator`), these are much slower than the builtin comparators (since the
   * native code must be calling back into JVM code _for each comparison_). The only builtin
   * comparators are bytewise (equivalent to `java.util.Arrays.compareUnsigned([B,[B)`).
   *
   * All leads to the following conclusion: __our intuitive encoding of keys must be preserved as a
   * bytewise ordering after being encoded__. In pseudo-code:
   *
   * {{{
   * val key1: Key = ...
   * val key2: Key = ...
   * def encodeKey(k: Key): Array[Byte] = ...
   *
   * val directCompare: Int = key1 compare key2
   * val encodedCompare: Int = java.util.Arrays.compareUnsigned(encodeKey(key1), encodeKey(key2))
   * Integer.signum(directCompare) == Integer.signum(encodedCompare)
   * }}}
   *
   * If the key to be encoded is a tuple of fixed-width types (UUID's, Long's, Int's, etc.) then it
   * is enough to just concatenate their big-endian representations (modulo some small issues
   * around signedness). However, this doesn't work for variable length types like QuineId. For
   * those, we can use a different trick: encode first their length in a fixed-width, then the
   * actual array.
   *
   * We end up having three types of keys to encode:
   *
   *   - `(QuineId, EventTime)` for journals and snapshots
   *   - `(StandingQueryId, QuineId, StandingQueryPartId)` for standing query states
   *   - `String` for standing queries and meta data
   *
   * The various requirements laid out above are check in `RocksDbKeyEncodingTest`
   */

  /** Encode a [[QuineId]] and [[EventTime]] into a key
    *
    * @param qid node ID
    * @param eventTime unsigned timestamp
    * @return encoded key
    */
  final def qidAndTime2Key(qid: QuineId, eventTime: EventTime): Array[Byte] = {
    val qidBytes = qid.array
    val qidLen = qidBytes.length
    ByteBuffer
      .allocate(2 + qidLen + 8)
      .putShort((qidLen & 0xFFFF).asInstanceOf[Short])
      .put(qidBytes)
      .putLong(eventTime.eventTime)
      .array
  }

  /** Decode a key into a [[QuineId]] and [[EventTime]]
    *
    * Left inverse of [[qidAndTime2Key]]
    *
    * @param key encoded key
    * @return decoded ID and time
    */
  final def key2QidAndTime(key: Array[Byte]): (QuineId, EventTime) = {
    val keyBuf = ByteBuffer.wrap(key)
    val qidBytes = new Array[Byte](keyBuf.getShort & 0xFFFF)
    keyBuf.get(qidBytes)
    val eventTime = EventTime.fromRaw(keyBuf.getLong)
    (QuineId(qidBytes), eventTime)
  }

  /** Decode just the [[QuineId]] portion of a key (and just as bytes)
    *
    * This is equivalent to (but more efficient than) `key2QidAndTime(key)._1.array`.
    *
    * @param key encoded key
    * @return decoded ID
    */
  final def key2QidBytes(key: Array[Byte]): Array[Byte] = {
    val keyBuf = ByteBuffer.wrap(key)
    val qid = new Array[Byte](keyBuf.getShort & 0xFFFF)
    keyBuf.get(qid)
    qid
  }

  /** Given the bytes for a [[QuineId]], compute a key which can be [[seek]]-ed to skip straight
    * to the next [[QuineId]].
    *
    * @param qidBytes bytes for a [[QuineId]]
    * @return key to seek to the next ID
    */
  final def qidBytes2NextKey(qidBytes: Array[Byte]): Array[Byte] = {
    val len = qidBytes.length
    incrementKey(qidBytes) match {
      case None =>
        // `qidBytes` cannot be incremented - the next largest ID must be longer
        ByteBuffer
          .allocate(2)
          .putShort(((len + 1) & 0xFFFF).asInstanceOf[Short])
          .array
      case Some(incrementedBytes) =>
        // `qidBytes` can be incremented - just use the incremented value
        ByteBuffer
          .allocate(2 + len)
          .putShort((len & 0xFFFF).asInstanceOf[Short])
          .put(incrementedBytes)
          .array
    }
  }

  /** Encode a [[StandingQueryId]], [[QuineId]], and [[StandingQueryPartId]] into a key
    *
    * @param sqId standing query ID
    * @param qid node ID
    * @param sqPartId standing query part ID
    * @return encoded key
    */
  final def sqIdQidAndSqPartId2Key(
    sqId: StandingQueryId,
    qid: QuineId,
    sqPartId: StandingQueryPartId
  ): Array[Byte] = {
    val sqIdUuid = sqId.uuid
    val qidBytes = qid.array
    val qidLen = qidBytes.length
    val sqPartIdUuid = sqPartId.uuid
    ByteBuffer
      .allocate(16 + 2 + qidLen + 16)
      .putLong(sqIdUuid.getMostSignificantBits)
      .putLong(sqIdUuid.getLeastSignificantBits)
      .putShort((qidLen & 0xFFFF).asInstanceOf[Short])
      .put(qidBytes)
      .putLong(sqPartIdUuid.getMostSignificantBits)
      .putLong(sqPartIdUuid.getLeastSignificantBits)
      .array
  }

  /** Decode a key into a [[StandingQueryId]], [[QuineId]], and [[StandingQueryPartId]]
    *
    * Left inverse of [[sqIdQidAndSqPartId2Key]]
    *
    * @param key encoded key
    * @return decoded standing query ID, node ID, and standing query part ID
    */
  final def key2SqIdQidAndSqPartId(key: Array[Byte]): (StandingQueryId, QuineId, StandingQueryPartId) = {
    val keyBuf = ByteBuffer.wrap(key)
    val sqId = StandingQueryId(new UUID(keyBuf.getLong, keyBuf.getLong))
    val qidBytes = new Array[Byte](keyBuf.getShort & 0xFFFF)
    keyBuf.get(qidBytes)
    val sqPartId = StandingQueryPartId(new UUID(keyBuf.getLong, keyBuf.getLong))
    (sqId, QuineId(qidBytes), sqPartId)
  }

  /** Decode just the [[StandingQueryId]] portion of a key
    *
    * This is equivalent to (but more efficient than) `key2SqIdQidAndSqPartId(key)._1`.
    *
    * @param key encoded key
    * @return decoded standing query ID
    */
  final def key2SqId(key: Array[Byte]): StandingQueryId = {
    val keyBuf = ByteBuffer.wrap(key)
    StandingQueryId(new UUID(keyBuf.getLong, keyBuf.getLong))
  }

  /** Prefix key for [[sqIdQidAndSqPartId2Key]]
    *
    * [[seek]]-ing to this key will move straight to the start of the block of values associated
    * with the specified standing query ID
    *
    * == Use with `incrementKey` ==
    *
    * [[incrementKey]] is almost always going to work on the output [[sqIdPrefixKey]] except in the
    * extremely unlikely case that there is a `StandingQueryId(new UUID(-1L, -1L))`. As documented
    * in [[incrementKey]], this case corresponds to the key consisting entirey of 1 bits (so there
    * is no way to increment without overflowing). This is unlikely because standing query IDs are
    * chosen randomly.
    *
    * @param sqId standing query ID
    * @return prefix key
    */
  final def sqIdPrefixKey(sqId: StandingQueryId): Array[Byte] = ByteBuffer
    .allocate(16)
    .putLong(sqId.uuid.getMostSignificantBits)
    .putLong(sqId.uuid.getLeastSignificantBits)
    .array

  /** Prefix key for [[sqIdQidAndSqPartId2Key]]
    *
    * [[seek]]-ing to this key will move straight to the start of the block of values associated
    * with the specified standing query ID and node ID
    *
    * @param sqId standing query ID
    * @param qid node ID
    * @return prefix key
    */
  final def sqIdAndQidPrefixKey(sqId: StandingQueryId, qid: QuineId): Array[Byte] = {
    val sqIdUuid = sqId.uuid
    val qidBytes = qid.array
    val qidLen = qidBytes.length
    ByteBuffer
      .allocate(16 + 2 + qidLen)
      .putLong(sqIdUuid.getMostSignificantBits)
      .putLong(sqIdUuid.getLeastSignificantBits)
      .put(qidBytes)
      .array
  }

  /** Get the lexicographically (unsigned) "next" key of the same length
    *
    * @param key key to increment
    * @return the next key or [[None]] if there is no next key (eg. due to key having only 1 bits)
    */
  final def incrementKey(key: Array[Byte]): Option[Array[Byte]] = {
    val incrementedKey = key.clone()

    // `0xff` bytes go to zero and we carry the addition process to the next byte
    var i = incrementedKey.length - 1
    while (i >= 0 && incrementedKey(i) == -1) {
      incrementedKey(i) = 0
      i -= 1
    }

    // increment the next byte
    if (i >= 0) {
      incrementedKey(i) = (1 + incrementedKey(i)).toByte
      Some(incrementedKey)
    } else {
      None
    }
  }

  /** Like `RocksDB.loadLibrary`, but returns whether the operation succeeded
    *
    * @note the exception thrown the first time is a link, the second time it is a no class def
    * @return whether the library did get loaded
    */
  final def loadRocksDbLibrary(): Boolean =
    try {
      RocksDB.loadLibrary()
      true
    } catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError => false
    }

  class RocksDBUnavailableException(msg: String = "RocksDB is not currently available")
      extends IllegalStateException(msg)
}

package com.thatdot.quine.persistor

import java.io.File

import scala.concurrent.ExecutionContext

import org.apache.pekko.stream.Materializer

import com.thatdot.quine.graph.NamespaceId

class RocksDbPrimePersistor(
  createParentDir: Boolean = true,
  topLevelPath: File,
  writeAheadLog: Boolean = true,
  syncWrites: Boolean = false,
  dbOptionProperties: java.util.Properties = new java.util.Properties(),
  persistenceConfig: PersistenceConfig = PersistenceConfig(),
  bloomFilterSize: Option[Long] = None,
  ioDispatcher: ExecutionContext
)(implicit materializer: Materializer)
    extends UnifiedPrimePersistor(persistenceConfig, bloomFilterSize) {

  private val parentDir = topLevelPath.getAbsoluteFile.getParentFile
  if (createParentDir)
    if (parentDir.mkdirs())
      logger.warn(s"Configured persistence directory: $parentDir did not exist; created.")
    else if (!parentDir.isDirectory)
      sys.error(s"Error: $parentDir does not exist") // Replaces exception thrown by RocksDB

  private val namespacesDir = new File(topLevelPath, "namespaces")
  namespacesDir.mkdirs()

  private def makeRocksDb(persistenceConfig: PersistenceConfig, path: File): RocksDbPersistor =
    try new RocksDbPersistor(
      path.getAbsolutePath,
      null,
      writeAheadLog,
      syncWrites,
      dbOptionProperties,
      persistenceConfig,
      ioDispatcher
    )
    catch {
      case err: UnsatisfiedLinkError =>
        logger.error(
          "RocksDB native library could not be loaded. " +
          "Consider using MapDB instead by specifying `quine.store.type=map-db`",
          err
        )
        sys.exit(1)
    }
  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): PersistenceAgentType =
    namespace match {
      case Some(name) => makeRocksDb(persistenceConfig, new File(namespacesDir, name.name))
      case None => makeRocksDb(persistenceConfig, topLevelPath)
    }

}

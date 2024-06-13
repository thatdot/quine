package com.thatdot.quine.migrations

import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.quine.persistor.PrimePersistor

case class MigrationVersion(version: Int) extends AnyVal with Ordered[MigrationVersion] {
  def toBytes: Array[Byte] = ByteBuffer.allocate(4).putInt(version).array()

  def compare(that: MigrationVersion): Int = version - that.version
}
object MigrationVersion {
  def apply(bytes: Array[Byte]): MigrationVersion = MigrationVersion(ByteBuffer.wrap(bytes).getInt())

  private val MetadataKey = "migration_version"
  def getFrom(persistor: PrimePersistor): Future[Option[MigrationVersion]] =
    persistor.getMetaData(MetadataKey).map(_.map(apply))(ExecutionContext.parasitic)
  def set(persistor: PrimePersistor, version: MigrationVersion): Future[Unit] =
    persistor.setMetaData(MetadataKey, Some(version.toBytes))
}

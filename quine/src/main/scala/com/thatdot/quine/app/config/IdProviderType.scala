package com.thatdot.quine.app.config

import java.{util => ju}

import memeid.{UUID => UUID4s}
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.{
  IdentityIdProvider,
  QuineIdLongProvider,
  QuineIdRandomLongProvider,
  QuineUUIDProvider,
  Uuid3Provider,
  Uuid4Provider,
  Uuid5Provider,
  WithExplicitPositions,
}
import com.thatdot.quine.model.QuineIdProvider

/** Options for ID representations */
sealed abstract class IdProviderType {

  /** Does the ID provider have a partition prefix? */
  val partitioned: Boolean

  /** Construct the ID provider associated with this configuration */
  def idProvider(implicit logConfig: LogConfig): QuineIdProvider = {
    val baseProvider = createUnpartitioned
    if (partitioned) WithExplicitPositions(baseProvider) else baseProvider
  }

  /** Construct the unpartitioned ID provider associated with this configuration */
  protected def createUnpartitioned: QuineIdProvider
}
object IdProviderType extends PureconfigInstances {

  final case class Long(
    consecutiveStart: Option[scala.Long],
    partitioned: Boolean = false,
  ) extends IdProviderType {
    def createUnpartitioned: QuineIdProvider = consecutiveStart match {
      case None => QuineIdRandomLongProvider
      case Some(initial) => QuineIdLongProvider(initial)
    }
  }

  final case class UUID(partitioned: Boolean = false) extends IdProviderType {
    def createUnpartitioned = QuineUUIDProvider
  }

  final case class Uuid3(
    namespace: ju.UUID = UUID4s.NIL.asJava(),
    partitioned: Boolean = false,
  ) extends IdProviderType {
    def createUnpartitioned: Uuid3Provider = Uuid3Provider(namespace)
  }

  final case class Uuid4(partitioned: Boolean = false) extends IdProviderType {
    def createUnpartitioned = Uuid4Provider
  }

  final case class Uuid5(
    namespace: ju.UUID = UUID4s.NIL.asJava(),
    partitioned: Boolean = false,
  ) extends IdProviderType {
    def createUnpartitioned: Uuid5Provider = Uuid5Provider(namespace)
  }

  final case class ByteArray(partitioned: Boolean = false) extends IdProviderType {
    def createUnpartitioned = IdentityIdProvider
  }

  implicit val longConfigConvert: ConfigConvert[Long] = deriveConvert[Long]
  implicit val uuidConfigConvert: ConfigConvert[UUID] = deriveConvert[UUID]
  implicit val uuid3ConfigConvert: ConfigConvert[Uuid3] = deriveConvert[Uuid3]
  implicit val uuid4ConfigConvert: ConfigConvert[Uuid4] = deriveConvert[Uuid4]
  implicit val uuid5ConfigConvert: ConfigConvert[Uuid5] = deriveConvert[Uuid5]
  implicit val byteArrayConfigConvert: ConfigConvert[ByteArray] = deriveConvert[ByteArray]

  implicit val configConvert: ConfigConvert[IdProviderType] =
    deriveConvert[IdProviderType]
}

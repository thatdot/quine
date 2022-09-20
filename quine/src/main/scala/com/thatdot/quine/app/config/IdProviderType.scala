package com.thatdot.quine.app.config

import java.{util => ju}

import memeid.{UUID => UUID4s}

import com.thatdot.quine.graph.{
  IdentityIdProvider,
  QuineIdLongProvider,
  QuineIdRandomLongProvider,
  QuineUUIDProvider,
  Uuid3Provider,
  Uuid4Provider,
  Uuid5Provider,
  WithExplicitPositions
}
import com.thatdot.quine.model.QuineIdProvider

/** Options for ID representations */
sealed abstract class IdProviderType {

  /** Does the ID provider have a partition prefix? */
  val partitioned: Boolean

  /** Construct the ID provider associated with this configuration */
  def idProvider: QuineIdProvider = {
    val baseProvider = createUnpartitioned
    if (partitioned) WithExplicitPositions(baseProvider) else baseProvider
  }

  /** Construct the unpartitioned ID provider associated with this configuration */
  protected def createUnpartitioned: QuineIdProvider
}
object IdProviderType {

  final case class Long(
    consecutiveStart: Option[scala.Long],
    partitioned: Boolean = false
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
    partitioned: Boolean = false
  ) extends IdProviderType {
    def createUnpartitioned: Uuid3Provider = Uuid3Provider(namespace)
  }

  final case class Uuid4(partitioned: Boolean = false) extends IdProviderType {
    def createUnpartitioned = Uuid4Provider
  }

  final case class Uuid5(
    namespace: ju.UUID = UUID4s.NIL.asJava(),
    partitioned: Boolean = false
  ) extends IdProviderType {
    def createUnpartitioned: Uuid5Provider = Uuid5Provider(namespace)
  }

  final case class ByteArray(partitioned: Boolean = false) extends IdProviderType {
    def createUnpartitioned = IdentityIdProvider
  }
}

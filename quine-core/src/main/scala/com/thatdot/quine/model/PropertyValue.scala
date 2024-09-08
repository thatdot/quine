package com.thatdot.quine.model

import scala.util.{Success, Try}

import com.thatdot.quine.util.ByteConversions

/** Abstraction for values that are being read to, written to, and stored on
  * nodes.
  *
  * The purpose of this abstraction is to delay doing serialization work until
  * as late as possible, and then to have that work be cached. One important
  * path to optimize: when nodes are woken up, properties are left in their
  * serialized form, since they're probably never even going to be read.
  */
sealed abstract class PropertyValue extends Equals {

  /** @return serialized representation, possibly computing it */
  def serialized: Array[Byte]

  /** @return de-serialized representation, possibly computing it */
  def deserialized: Try[QuineValue]

  /** @return de-serialized type
    *
    * @note this should always be very cheap, and for bigger data (strings,
    *       lists, maps, etc.) cheaper than [[deserialized]].
    */
  def quineType: Try[QuineType]

  /** @return whether the serialized representation is cached */
  private[quine] def serializedReady: Boolean

  /** @return whether the de-serialized representation is cached */
  private[quine] def deserializedReady: Boolean

  override def canEqual(other: Any): Boolean = other.isInstanceOf[PropertyValue]

  override def equals(other: Any): Boolean = other match {
    case otherVal: PropertyValue =>
      (this eq otherVal) || (
        if (deserializedReady && otherVal.deserializedReady)
          deserialized == otherVal.deserialized
        else
          serialized.sameElements(otherVal.serialized)
      )
    case _ => false
  }

  // TODO: optimize this, or ensure it isn't used
  override def hashCode(): Int = deserialized.hashCode()
}

object PropertyValue {

  /** Construct a [[PropertyValue]] whose de-serialized representation will be
    * ready and whose serialized one will be lazily computed
    */
  def apply(deserialized: QuineValue): PropertyValue = Deserialized(Success(deserialized))

  def apply(v: QuineValue.Str#JvmType): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: QuineValue.Integer#JvmType): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: Int): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: QuineValue.Floating#JvmType): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: Float): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: QuineValue.True.JvmType): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: QuineValue.Null.JvmType): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: QuineValue.Bytes#JvmType): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: Vector[QuineValue]): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: scala.collection.immutable.List[QuineValue]): PropertyValue = PropertyValue(QuineValue(v))
  def apply(v: scala.collection.immutable.Map[String, QuineValue]): PropertyValue = PropertyValue(QuineValue(v))
  def apply[CustomIdType](v: CustomIdType)(implicit
    idProvider: QuineIdProvider.Aux[CustomIdType],
  ): PropertyValue = PropertyValue(QuineValue(v))

  def unapply(arg: PropertyValue): Option[QuineValue] = arg.deserialized.toOption

  /** Construct a [[PropertyValue]] whose serialized representation will be
    * ready and whose de0serialized one will be lazily computed
    */
  def fromBytes(serialized: Array[Byte]): PropertyValue = new Serialized(serialized)

  /* TODO: do we want to cache serialized/deserialized values with `lazy`?
   * TODO: consider using soft references for caching
   *
   * I think not: separate threads my race setting the value, but that won't
   * affect correctness - they'll all get the same value. It could be faster,
   * but I think the synchronized overhead is not worth. Profile!
   */

  /** Variant of [[PropertyValue]] obtained when we start with the de-serialized representation */
  final private case class Deserialized(deserialized: Success[QuineValue]) extends PropertyValue {
    private var cachedSerialized: Array[Byte] = null

    def serializedReady: Boolean = cachedSerialized ne null
    def deserializedReady = true

    def serialized: Array[Byte] = {
      if (cachedSerialized eq null) {
        cachedSerialized = QuineValue.writeMsgPack(deserialized.value)
      }
      cachedSerialized
    }

    def quineType: Success[QuineType] = Success(deserialized.value.quineType)
  }

  /** Variant of [[PropertyValue]] obtained when we start with the serialized representation */
  final private case class Serialized(serialized: Array[Byte]) extends PropertyValue {
    private var cachedDeserialized: Try[QuineValue] = null

    def serializedReady = true
    def deserializedReady: Boolean = cachedDeserialized ne null

    def deserialized: Try[QuineValue] = {
      if (cachedDeserialized eq null) {
        cachedDeserialized = Try(QuineValue.readMsgPack(serialized))
      }
      cachedDeserialized
    }

    override def toString: String = {
      val value = if (deserializedReady && deserialized.isSuccess) deserialized.get.toString else ""
      s"Serialized(${ByteConversions.formatHexBinary(serialized)}$value)"
    }

    def quineType: Try[QuineType] = Try(QuineValue.readMsgPackType(serialized))
  }
}

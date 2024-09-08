package com.thatdot.quine.util

import java.nio.ByteBuffer
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}
import java.util.UUID

import scala.collection.immutable.{ArraySeq, SortedMap}
import scala.reflect.ClassTag

import com.google.common.hash.{HashFunction, Hasher}
import shapeless._

/** Class of types that can be converted to a hash value */
trait Hashable[A] {

  /** Add a value to a hashed
    *
    * @param hasher hash builder
    * @param value what to hash
    */
  def addToHasher(hasher: Hasher, value: A): Hasher

  /** Hash a value and produce results in a read-only byte buffer
    *
    * @param function hash function to use (ex: `Hashing.murmur3_128`)
    * @param value what to hash
    * @return output hash
    */
  @inline
  final private def hashToBytes(function: HashFunction, value: A): ByteBuffer = {
    val hasher: Hasher = addToHasher(function.newHasher, value)
    ByteBuffer.wrap(hasher.hash.asBytes)
  }

  /** Hash a value into a UUID
    *
    * @note if the hash function does produce enough bits, this will throw
    * @param value what to hash
    * @param function hash function to use (ex: `Hashing.murmur3_128`)
    * @return UUID hash
    */
  final def hashToUuid(function: HashFunction, value: A): UUID = {
    val bb = hashToBytes(function, value)
    new UUID(bb.getLong(), bb.getLong())
  }

  final def contramap[B](f: B => A): Hashable[B] =
    (hasher: Hasher, value: B) => addToHasher(hasher, f(value))

}

trait BasicHashables {

  implicit val boolean: Hashable[Boolean] = _.putBoolean(_)

  implicit val byte: Hashable[Byte] = _.putByte(_)

  implicit val bytes: Hashable[Array[Byte]] = _.putBytes(_)

  implicit val char: Hashable[Char] = _.putChar(_)

  implicit val double: Hashable[Double] = _.putDouble(_)

  implicit val float: Hashable[Float] = _.putFloat(_)

  implicit val int: Hashable[Int] = _.putInt(_)

  implicit val long: Hashable[Long] = _.putLong(_)

  implicit val short: Hashable[Short] = _.putShort(_)

  implicit val string: Hashable[String] = _.putUnencodedChars(_)

  implicit val symbol: Hashable[Symbol] = string.contramap(_.name)

  implicit val localDate: Hashable[LocalDate] = long.contramap(_.toEpochDay)

  implicit val localTime: Hashable[LocalTime] = long.contramap(_.toNanoOfDay)

  implicit val offsetTime: Hashable[OffsetTime] = new Hashable[OffsetTime] {
    def addToHasher(hasher: Hasher, value: OffsetTime): Hasher = {
      localTime.addToHasher(hasher, value.toLocalTime)
      hasher.putInt(value.getOffset.getTotalSeconds)
    }
  }

  implicit val localDateTime: Hashable[LocalDateTime] = new Hashable[LocalDateTime] {
    def addToHasher(hasher: Hasher, value: LocalDateTime): Hasher = {
      hasher.putLong(value.toLocalDate.toEpochDay)
      hasher.putLong(value.toLocalTime.toNanoOfDay)
    }
  }

  implicit val instant: Hashable[Instant] = new Hashable[Instant] {
    def addToHasher(hasher: Hasher, value: Instant): Hasher = {
      hasher.putInt(value.getNano)
      hasher.putLong(value.getEpochSecond)
    }
  }

  implicit val zonedDateTime: Hashable[ZonedDateTime] = new Hashable[ZonedDateTime] {
    def addToHasher(hasher: Hasher, value: ZonedDateTime): Hasher = {
      instant.addToHasher(hasher, value.toInstant)
      hasher.putInt(value.getZone.hashCode)
    }
  }

  implicit val duration: Hashable[Duration] = new Hashable[Duration] {
    def addToHasher(hasher: Hasher, value: Duration): Hasher = {
      hasher.putInt(value.getNano)
      hasher.putLong(value.getSeconds)
    }
  }

  implicit val uuid: Hashable[UUID] = new Hashable[UUID] {
    def addToHasher(hasher: Hasher, value: UUID): Hasher = {
      hasher.putLong(value.getMostSignificantBits)
      hasher.putLong(value.getLeastSignificantBits)
    }
  }
}

trait CompositeHashables {

  implicit def pairHashable[A, B](implicit hashA: Hashable[A], hashB: Hashable[B]): Hashable[(A, B)] =
    (hasher: Hasher, pair: (A, B)) => {
      hasher.putUnencodedChars("Pair")
      hashA.addToHasher(hasher, pair._1)
      hashB.addToHasher(hasher, pair._2)
    }
  def iterable[A, I[X] <: Iterable[X]](implicit
    hashableA: Hashable[A],
    classTagIA: ClassTag[I[A]],
  ): Hashable[I[A]] = {
    val iaSeed = classTagIA.runtimeClass.getName.hashCode
    (hasher: Hasher, value: I[A]) => {
      hasher.putInt(iaSeed)
      val hashA = hashableA
      for (a <- value)
        hashA.addToHasher(hasher, a)
      hasher
    }
  }

  implicit def optionHashable[A: Hashable]: Hashable[Option[A]] = iterable[A, Iterable].contramap(_.toList)
  implicit def listHashable[A: Hashable]: Hashable[List[A]] = iterable
  implicit def vectorHashable[A: Hashable]: Hashable[Vector[A]] = iterable
  implicit def setHashable[A: Hashable]: Hashable[Set[A]] = iterable
  implicit def arraySeqHashable[A: Hashable]: Hashable[ArraySeq[A]] = iterable
  implicit def mapHashable[A: Hashable, B: Hashable]: Hashable[Map[A, B]] =
    iterable[(A, B), Iterable].contramap(identity[Iterable[(A, B)]])
  implicit def sortedMapHashable[A: Hashable, B: Hashable]: Hashable[SortedMap[A, B]] =
    iterable[(A, B), Iterable].contramap(identity[Iterable[(A, B)]])
  implicit def seqHashable[A: Hashable]: Hashable[Seq[A]] = iterable
}

object Hashable extends TypeClassCompanion[Hashable] with BasicHashables with CompositeHashables {

  object typeClass extends TypeClass[Hashable] {
    val emptyProduct: Hashable[HNil] = (hasher: Hasher, value: HNil) => hasher
    def product[H, T <: HList](hashHead: Hashable[H], hashTail: Hashable[T]): Hashable[H :: T] =
      (hasher: Hasher, headTail: H :: T) => {
        hashHead.addToHasher(hasher, headTail.head)
        hashTail.addToHasher(hasher, headTail.tail)
      }

    val emptyCoproduct: Hashable[CNil] = (hasher: Hasher, value: CNil) => hasher

    def coproduct[L, R <: Coproduct](hashLeft: => Hashable[L], hashRight: => Hashable[R]): Hashable[L :+: R] =
      (hasher: Hasher, leftRight: L :+: R) => {
        leftRight.eliminate(
          hashLeft.addToHasher(hasher, _),
          hashRight.addToHasher(hasher, _),
        )
      }

    def project[F, G](instance: => Hashable[G], to: F => G, from: G => F): Hashable[F] = (hasher: Hasher, rep: F) => {
      val value = to(rep)
      hasher.putInt(value.getClass.getName.hashCode)
      instance.addToHasher(hasher, value)
    }
  }

}

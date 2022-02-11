package com.thatdot.quine.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.UUID

import scala.language.higherKinds
import scala.reflect.ClassTag

import com.google.common.hash.{HashFunction, Hasher}
import shapeless._

/** Class of types that can be converted to a hash value */
trait Hashable[-A] {

  /** Add a value to a hashed
    *
    * @param hasher hash builder
    * @param value what to hash
    */
  def addToHasher(hasher: Hasher, value: A): Hasher

  /** Hash a value and produce results in a read-only byte buffer
    *
    * @param function hash function to use (ex: `Hashing.murmur3_128()`)
    * @param value what to hash
    * @return output hash
    */
  final def hashToBytes(function: HashFunction, value: A): ByteBuffer = {
    val hasher: Hasher = addToHasher(function.newHasher(), value)
    ByteBuffer.wrap(hasher.hash().asBytes()).asReadOnlyBuffer()
  }

  /** Hash a value into a UUID
    *
    * @note if the hash function does produce enough bits, this will throw
    * @param value what to hash
    * @param function hash function to use (ex: `Hashing.murmur3_128()`)
    * @return UUID hash
    */
  final def hashToUuid(function: HashFunction, value: A): UUID = {
    val bb = hashToBytes(function, value)
    new UUID(bb.getLong(), bb.getLong())
  }

  final def contramap[B](f: B => A): Hashable[B] =
    (hasher: Hasher, value: B) => addToHasher(hasher, f(value))
}

object Hashable extends BasicHashables with CompositeHashables1 {
  def apply[A](implicit aHashable: Lazy[Hashable[A]]): Hashable[A] = aHashable.value
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

  implicit val string: Hashable[CharSequence] = _.putString(_, StandardCharsets.UTF_8)

  implicit val symbol: Hashable[Symbol] = string.contramap(_.name)

  implicit val localDate: Hashable[LocalDate] = long.contramap(_.toEpochDay)

  implicit val localTime: Hashable[LocalTime] = long.contramap(_.toNanoOfDay)

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

trait CompositeHashables1 extends CompositeHashables2 {

  implicit def iterable1[A, F[X] <: Iterable[X]](implicit
    hashableA: Lazy[Hashable[A]],
    classTagFA: ClassTag[F[A]]
  ): Hashable[F[A]] = {
    val faSeed = classTagFA.runtimeClass.getName.hashCode
    (hasher: Hasher, value: F[A]) => {
      hasher.putInt(faSeed)
      val hashA = hashableA.value
      for (a <- value)
        hashA.addToHasher(hasher, a)
      hasher
    }
  }
}

/* The reason [[iterable2]] needs to exist in a different trait than [[iterable1]] is due to a
 * difference in 2.13 vs. 2.13:
 *
 *   - in 2.12, [[iterable1]] somehow works for type constructors of kinds other that just `F[_]`
 *     (it works for `Map[_, _]`, for example!)
 *
 *   - in 2.13, [[iterable1]] does not work for any kind other than `F[_]`
 *
 * We need [[iterable1]] and [[iterable2]] for 2.13 to compile, but we need them to not be the
 * same priority for 2.12 to compile
 */
trait CompositeHashables2 extends GenericHashables {
  implicit def iterable2[A, B, F[X, Y] <: Iterable[(X, Y)]](implicit
    hashableA: Lazy[Hashable[A]],
    hashableB: Lazy[Hashable[B]],
    classTagFAB: ClassTag[F[A, B]]
  ): Hashable[F[A, B]] = {
    val fabSeed = classTagFAB.runtimeClass.getName.hashCode
    (hasher: Hasher, value: F[A, B]) => {
      hasher.putInt(fabSeed)
      val hashA = hashableA.value
      val hashB = hashableB.value
      for ((a, b) <- value) {
        hashA.addToHasher(hasher, a)
        hashB.addToHasher(hasher, b)
      }
      hasher
    }
  }
}

trait GenericHashables {

  implicit def generic[A, G](implicit
    classTagA: ClassTag[A],
    generic: Generic.Aux[A, G],
    hashableG: Lazy[Hashable[G]]
  ): Hashable[A] = {
    val genericSeed = classTagA.runtimeClass.getName.hashCode
    (hasher: Hasher, value: A) => {
      hasher.putInt(genericSeed)
      hashableG.value.addToHasher(hasher, generic.to(value))
    }
  }

  implicit val hnil: Hashable[HNil] = (hasher: Hasher, value: HNil) => hasher

  implicit def hcons[A, B <: HList](implicit
    hashableA: Lazy[Hashable[A]],
    hashableB: Lazy[Hashable[B]]
  ): Hashable[A :: B] =
    (hasher: Hasher, value: A :: B) => {
      hashableA.value.addToHasher(hasher, value.head)
      hashableB.value.addToHasher(hasher, value.tail)
    }

  implicit val cnil: Hashable[CNil] = (hasher: Hasher, value: CNil) => hasher

  implicit def ccons[A, B <: Coproduct](implicit
    hashableA: Lazy[Hashable[A]],
    hashableB: Lazy[Hashable[B]]
  ): Hashable[A :+: B] =
    (hasher: Hasher, value: A :+: B) => {
      value.eliminate(
        hashableA.value.addToHasher(hasher, _),
        hashableB.value.addToHasher(hasher, _)
      )
    }
}

package com.thatdot.quine.persistor

import java.nio.ByteBuffer
import java.util.UUID

import org.rocksdb.ComparatorOptions
import org.rocksdb.util.BytewiseComparator
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.{EventTime, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.QuineId

class RocksDbKeyEncodingTest extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks {

  val nativeLibraryLoaded: Boolean = RocksDbPersistor.loadRocksDbLibrary()

  implicit val quineIdArb: Arbitrary[QuineId] = Arbitrary {
    Gen
      .frequency(
        2 -> Gen.const(128), // UUID sized
        1 -> Gen.const(64), // Long sized
        1 -> Gen.long.map(n => Math.abs(n % 256).toInt) // uniformly picked
      )
      .flatMap(n => Gen.containerOfN[Array, Byte](n, Arbitrary.arbByte.arbitrary))
      .map(QuineId(_))
  }

  implicit val eventTimeArb: Arbitrary[EventTime] = Arbitrary {
    Arbitrary.arbLong.arbitrary
      .map(EventTime.fromRaw)
  }

  implicit val standingQueryIdArb: Arbitrary[StandingQueryId] = Arbitrary {
    Arbitrary.arbUuid.arbitrary
      .map(StandingQueryId.apply)
  }

  implicit val standingQueryPartIdArb: Arbitrary[StandingQueryPartId] = Arbitrary {
    Arbitrary.arbUuid.arbitrary
      .map(StandingQueryPartId.apply)
  }

  // Lexicographic unsigned ordering (like `java.utils.Arrays.compareUnsigned` on JDK 9+)
  val unsignedByteArrayOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    def compare(arr1: Array[Byte], arr2: Array[Byte]): Int = {
      val iter1 = arr1.iterator
      val iter2 = arr2.iterator

      while (iter1.hasNext && iter2.hasNext) {
        val res = (iter1.next() & 0xFF) - (iter2.next() & 0xFF) // simulate `java.lang.Byte.compareUnsigned`
        if (res != 0) return res
      }

      return java.lang.Boolean.compare(iter1.hasNext, iter2.hasNext)
    }
  }

  // Rocks DB bytewise ordering
  lazy val rocksDbByteArrayOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    lazy val comparator = new BytewiseComparator(new ComparatorOptions())
    def compare(arr1: Array[Byte], arr2: Array[Byte]): Int =
      comparator.compare(ByteBuffer.wrap(arr1), ByteBuffer.wrap(arr2))
  }

  /* Ordering for `QuineId` which happens to match what we use in RocksDB
   *
   * The actual ordering we pick for `QuineId` has to match the ordering implicitly used in
   * the encoded mode, but otherwise doesn't really matter much - just insofar that it is a valid
   * total ordering.
   */
  val quineIdOrdering: Ordering[QuineId] = Ordering
    .by[Array[Byte], (Int, Array[Byte])](arr => (arr.length, arr))(
      Ordering.Tuple2(implicitly, unsignedByteArrayOrdering)
    )
    .on[QuineId](_.array)

  val unsignedUuidOrdering: Ordering[UUID] = new Ordering[UUID] {
    def compare(u1: UUID, u2: UUID): Int =
      java.lang.Long.compareUnsigned(u1.getMostSignificantBits, u2.getMostSignificantBits) match {
        case 0 => java.lang.Long.compareUnsigned(u1.getLeastSignificantBits, u2.getLeastSignificantBits)
        case other => other
      }
  }

  // The intuitive ordering we want for `(QuineId, EventTime)`
  val intuitiveOrdering1: Ordering[(QuineId, EventTime)] =
    Ordering.Tuple2[QuineId, EventTime](quineIdOrdering, implicitly)

  // The intuitive ordering we want for `(StandingQueryId, QuineId, StandingQueryPartId)`
  val intuitiveOrdering2: Ordering[(StandingQueryId, QuineId, StandingQueryPartId)] =
    Ordering.Tuple3[StandingQueryId, QuineId, StandingQueryPartId](
      unsignedUuidOrdering.on[StandingQueryId](_.uuid),
      quineIdOrdering,
      unsignedUuidOrdering.on[StandingQueryPartId](_.uuid)
    )

  "(QuineId, EventTime) key encoding" should "round-trip" in {
    forAll { (q1: QuineId, t1: EventTime) =>
      val k1Ser = RocksDbPersistor.qidAndTime2Key(q1, t1)
      val (q2, t2) = RocksDbPersistor.key2QidAndTime(k1Ser)
      q1 == q2 && t1 == t2
    }
  }

  it should "preserve the intuitive ordering" in {
    assume(nativeLibraryLoaded)

    // Small test cases - we'll compare every combination of these
    val smallKeys = List(
      QuineId(Array[Byte](0)) -> EventTime.fromRaw(0L),
      QuineId(Array[Byte](0)) -> EventTime.fromRaw(1L),
      QuineId(Array[Byte](0, 0)) -> EventTime.fromRaw(0L),
      QuineId(Array[Byte](0, 0)) -> EventTime.fromRaw(1L),
      QuineId(Array[Byte](1, 0)) -> EventTime.fromRaw(1L),
      QuineId(Array[Byte](0, 1)) -> EventTime.fromRaw(1L)
    )

    for {
      k1 @ (q1, t1) <- smallKeys
      k2 @ (q2, t2) <- smallKeys
    } {
      val cmp1 = Integer.signum(intuitiveOrdering1.compare(k1, k2))

      val k1Ser = RocksDbPersistor.qidAndTime2Key(q1, t1)
      val k2Ser = RocksDbPersistor.qidAndTime2Key(q2, t2)

      val cmp2 = Integer.signum(unsignedByteArrayOrdering.compare(k1Ser, k2Ser))
      val cmp3 = Integer.signum(rocksDbByteArrayOrdering.compare(k1Ser, k2Ser))

      assert(cmp1 == cmp2, "untuitive ordering is preserved through encoding")
      assert(cmp1 == cmp3, "unsigned byte array ordering matches RocksDB ordering")
    }

    forAll { (q1: QuineId, t1: EventTime, q2: QuineId, t2: EventTime) =>
      val k1 = q1 -> t1
      val k2 = q2 -> t2

      val cmp1 = Integer.signum(intuitiveOrdering1.compare(k1, k2))

      val k1Ser = RocksDbPersistor.qidAndTime2Key(q1, t1)
      val k2Ser = RocksDbPersistor.qidAndTime2Key(q2, t2)

      val cmp2 = Integer.signum(unsignedByteArrayOrdering.compare(k1Ser, k2Ser))
      val cmp3 = Integer.signum(rocksDbByteArrayOrdering.compare(k1Ser, k2Ser))

      cmp1 == cmp2 && cmp1 == cmp3
    }
  }

  "(StandingQueryId, QuineId, StandingQueryPartId) key encoding" should "round-trip" in {
    forAll { (sqId1: StandingQueryId, q1: QuineId, sqPartId1: StandingQueryPartId) =>
      val k1Ser = RocksDbPersistor.sqIdQidAndSqPartId2Key(sqId1, q1, sqPartId1)
      val (sqId2, q2, sqPartId2) = RocksDbPersistor.key2SqIdQidAndSqPartId(k1Ser)
      sqId1 == sqId2 && q1 == q2 && sqPartId1 == sqPartId2
    }
  }

  it should "preserve the intuitive ordering" in {
    assume(nativeLibraryLoaded)

    forAll {
      (
        sqId1: StandingQueryId,
        q1: QuineId,
        sqPartId1: StandingQueryPartId,
        sqId2: StandingQueryId,
        q2: QuineId,
        sqPartId2: StandingQueryPartId
      ) =>
        val k1 = (sqId1, q1, sqPartId1)
        val k2 = (sqId2, q2, sqPartId2)

        val cmp1 = Integer.signum(intuitiveOrdering2.compare(k1, k2))

        val k1Ser = RocksDbPersistor.sqIdQidAndSqPartId2Key(sqId1, q1, sqPartId1)
        val k2Ser = RocksDbPersistor.sqIdQidAndSqPartId2Key(sqId2, q2, sqPartId2)

        val cmp2 = Integer.signum(unsignedByteArrayOrdering.compare(k1Ser, k2Ser))
        val cmp3 = Integer.signum(rocksDbByteArrayOrdering.compare(k1Ser, k2Ser))

        cmp1 == cmp2 && cmp1 == cmp3
    }
  }
}

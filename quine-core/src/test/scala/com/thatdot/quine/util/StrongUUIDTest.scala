package com.thatdot.quine.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StrongUUIDTest extends AnyFlatSpec with Matchers {

  "StrongUUID.randomUUID" should "always return a valid UUID" in {
    // Generate multiple UUIDs and verify each one is valid
    val uuids = (1 to 1000).map(_ => StrongUUID.randomUUID())

    uuids.foreach { uuid =>
      uuid should not be null
      // Verify toString produces valid UUID string format
      uuid.toString should fullyMatch regex "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    }
  }

  it should "generate RFC 4122 version 4 UUIDs" in {
    val uuids = (1 to 1000).map(_ => StrongUUID.randomUUID())

    uuids.foreach { uuid =>
      // RFC 4122 section 4.1.3: version 4 UUID has version bits set to 0100 in the most
      // significant 4 bits of the time_hi_and_version field (7th byte)
      // Extract the version from the most significant bits
      val mostSigBits = uuid.getMostSignificantBits
      val version = ((mostSigBits >> 12) & 0x0F).toInt

      version shouldBe 4
    }
  }

  it should "set the RFC 4122 variant bits correctly" in {
    val uuids = (1 to 1000).map(_ => StrongUUID.randomUUID())

    uuids.foreach { uuid =>
      // RFC 4122 section 4.1.1: variant bits should be 10x (binary)
      // in the most significant bits of clock_seq_hi_and_reserved field
      val leastSigBits = uuid.getLeastSignificantBits
      val variantBits = ((leastSigBits >> 62) & 0x03).toInt

      // Variant should be 2 (binary 10), meaning the two most significant bits are "10"
      variantBits shouldBe 2
    }
  }

  it should "generate unique UUIDs with extremely low collision probability" in {
    // Generate a large sample and verify no duplicates
    val sampleSize = 100000
    val uuids = (1 to sampleSize).map(_ => StrongUUID.randomUUID()).toSet

    // All UUIDs should be unique
    uuids.size shouldBe sampleSize
  }

  it should "demonstrate cryptographic randomness properties" in {
    val sampleSize = 10000
    val uuids = (1 to sampleSize).map(_ => StrongUUID.randomUUID())

    // Test 1: Bit distribution - each bit position should be roughly 50% 0s and 50% 1s
    // We'll check the most significant Long values
    val mostSigBits = uuids.map(_.getMostSignificantBits)

    // Count 1-bits in various positions (avoiding version bits at 12-15 and variant bits in leastSigBits)
    // Testing bits: 0-7 (byte 7), 16-23 (byte 5), 24-31 (byte 4), 32-39 (byte 3), 56-63 (byte 0)
    val bitPositions = Seq(0, 1, 7, 16, 24, 32, 40, 56, 63)
    bitPositions.foreach { position =>
      val onesCount = mostSigBits.count(bits => ((bits >> position) & 1) == 1)
      val ratio = onesCount.toDouble / sampleSize

      // With 10000 samples, we expect roughly 50% ± 3% (allowing for statistical variance)
      ratio should be >= 0.47
      ratio should be <= 0.53
    }
  }

  it should "generate UUIDs with unpredictable byte values" in {
    val sampleSize = 1000
    val uuids = (1 to sampleSize).map(_ => StrongUUID.randomUUID())

    // Convert UUIDs to byte arrays and check for patterns
    val byteArrays = uuids.map { uuid =>
      val msb = uuid.getMostSignificantBits
      val lsb = uuid.getLeastSignificantBits

      (0 until 8).map(i => ((msb >> (56 - i * 8)) & 0xFF).toByte) ++
      (0 until 8).map(i => ((lsb >> (56 - i * 8)) & 0xFF).toByte)
    }

    // Check various byte positions (excluding version and variant bytes which are fixed)
    val randomBytePositions = Seq(0, 1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 15)

    randomBytePositions.foreach { position =>
      val byteValues = byteArrays.map(_(position)).toSet

      // We should see a good distribution of different byte values
      // With 1000 samples, we should see at least 100 different byte values at each position
      byteValues.size should be >= 100
    }
  }

  it should "use cryptographically strong randomness source" in {
    // Verify that the random source is properly initialized
    // by checking multiple UUIDs are truly different and random
    val uuid1 = StrongUUID.randomUUID()
    val uuid2 = StrongUUID.randomUUID()
    val uuid3 = StrongUUID.randomUUID()

    // Basic sanity: all different
    uuid1 should not equal uuid2
    uuid2 should not equal uuid3
    uuid1 should not equal uuid3

    // Check that the differences are not trivial (e.g., not just incrementing)
    val diff1 = uuid1.getLeastSignificantBits ^ uuid2.getLeastSignificantBits
    val diff2 = uuid2.getLeastSignificantBits ^ uuid3.getLeastSignificantBits

    // XOR should reveal many bit differences (at least 20 out of 64 bits)
    java.lang.Long.bitCount(diff1) should be >= 20
    java.lang.Long.bitCount(diff2) should be >= 20
  }

  it should "correctly encode version bits in byte position 6" in {
    val uuids = (1 to 100).map(_ => StrongUUID.randomUUID())

    uuids.foreach { uuid =>
      val mostSigBits = uuid.getMostSignificantBits
      // Extract byte 6 (counting from byte 0 at most significant)
      // In mostSigBits: byte 0 is at bits 56-63, byte 6 is at bits 8-15
      val byte6 = ((mostSigBits >> 8) & 0xFF).toInt

      // RFC 4122: version bits are the top 4 bits, should be 0100 (4)
      val versionNibble = (byte6 >> 4) & 0x0F
      versionNibble shouldBe 4

    // The lower 4 bits should be random
    // Not testing specific values, just that they vary across samples
    }

    // Verify the lower 4 bits of byte 6 show randomness
    val lowerNibbles = uuids.map { uuid =>
      val mostSigBits = uuid.getMostSignificantBits
      val byte6 = ((mostSigBits >> 8) & 0xFF).toInt
      byte6 & 0x0F
    }.toSet

    // Should see multiple different values in the random portion
    lowerNibbles.size should be >= 10
  }

  it should "correctly encode variant bits in byte position 8" in {
    val uuids = (1 to 100).map(_ => StrongUUID.randomUUID())

    uuids.foreach { uuid =>
      val leastSigBits = uuid.getLeastSignificantBits
      // Extract byte 8 (first byte of leastSigBits)
      val byte8 = ((leastSigBits >> 56) & 0xFF).toInt

      // RFC 4122: variant bits are the top 2 bits, should be 10 (binary)
      val variantBits = (byte8 >> 6) & 0x03
      variantBits shouldBe 2 // binary 10 = decimal 2

    // The lower 6 bits should be random
    }

    // Verify the lower 6 bits of byte 8 show randomness
    val lowerBits = uuids.map { uuid =>
      val leastSigBits = uuid.getLeastSignificantBits
      val byte8 = ((leastSigBits >> 56) & 0xFF).toInt
      byte8 & 0x3F
    }.toSet

    // Should see multiple different values in the random portion
    lowerBits.size should be >= 30
  }
}

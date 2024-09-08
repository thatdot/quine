package com.thatdot.quine.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class PackingTests extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks {

  "spec test cases" should "encode and decode to expected values" in {
    val decoded0 = Array[Byte]()
    val encoded0 = Array[Byte]()
    assert(Packing.pack(decoded0) sameElements encoded0)
    assert(Packing.unpack(encoded0) sameElements decoded0)

    val decoded1 = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)
    val encoded1 = Array[Byte](0, 0)
    assert(Packing.pack(decoded1) sameElements encoded1)
    assert(Packing.unpack(encoded1) sameElements decoded1)

    val decoded2 = Array[Byte](0, 0, 12, 0, 0, 34, 0, 0)
    val encoded2 = Array[Byte](0x24, 12, 34)
    assert(Packing.pack(decoded2) sameElements encoded2)
    assert(Packing.unpack(encoded2) sameElements decoded2)

    val decoded3 = Array[Byte](1, 3, 2, 4, 5, 7, 6, 8)
    val encoded3 = Array[Byte](0xFF.toByte, 1, 3, 2, 4, 5, 7, 6, 8, 0)
    assert(Packing.pack(decoded3) sameElements encoded3)
    assert(Packing.unpack(encoded3) sameElements decoded3)

    val decoded4 = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 2, 4, 5, 7, 6, 8)
    val encoded4 = Array[Byte](0, 0, 0xFF.toByte, 1, 3, 2, 4, 5, 7, 6, 8, 0)
    assert(Packing.pack(decoded4) sameElements encoded4)
    assert(Packing.unpack(encoded4) sameElements decoded4)

    val decoded5 = Array[Byte](0, 0, 12, 0, 0, 34, 0, 0, 1, 3, 2, 4, 5, 7, 6, 8)
    val encoded5 = Array[Byte](0x24, 12, 34, 0xFF.toByte, 1, 3, 2, 4, 5, 7, 6, 8, 0)
    assert(Packing.pack(decoded5) sameElements encoded5)
    assert(Packing.unpack(encoded5) sameElements decoded5)

    val decoded6 = Array[Byte](1, 3, 2, 4, 5, 7, 6, 8, 8, 6, 7, 4, 5, 2, 3, 1)
    val encoded6 = Array[Byte](0xFF.toByte, 1, 3, 2, 4, 5, 7, 6, 8, 1, 8, 6, 7, 4, 5, 2, 3, 1)
    assert(Packing.pack(decoded6) sameElements encoded6)
    assert(Packing.unpack(encoded6) sameElements decoded6)

    val decoded7 = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6,
      7, 8, 0, 2, 4, 0, 9, 0, 5, 1)
    val encoded7 = Array[Byte](
      -1, 1, 2, 3, 4, 5, 6, 7, 8, 3, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, -42, 2, 4,
      9, 5, 1,
    )
    assert(Packing.pack(decoded7) sameElements encoded7)
    assert(Packing.unpack(encoded7) sameElements decoded7)

    val decoded8 = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 6, 2, 4, 3, 9, 0, 5, 1, 1, 2, 3, 4, 5, 6,
      7, 8, 0, 2, 4, 0, 9, 0, 5, 1)
    val encoded8 = Array[Byte](
      -1, 1, 2, 3, 4, 5, 6, 7, 8, 3, 1, 2, 3, 4, 5, 6, 7, 8, 6, 2, 4, 3, 9, 0, 5, 1, 1, 2, 3, 4, 5, 6, 7, 8, -42, 2, 4,
      9, 5, 1,
    )
    assert(Packing.pack(decoded8) sameElements encoded8)
    assert(Packing.unpack(encoded8) sameElements decoded8)

    val decoded9 = Array[Byte](8, 0, 100, 6, 0, 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 1)
    val encoded9 = Array[Byte](0xED.toByte, 8, 100, 6, 1, 1, 2, 0, 2, 0xD4.toByte, 1, 2, 3, 1)
    assert(Packing.pack(decoded9) sameElements encoded9)
    assert(Packing.unpack(encoded9) sameElements decoded9)

    // Solid chunk of zero data
    val decoded10 = Array.fill[Byte](8 * 200)(0)
    val encoded10 = Array[Byte](0, 199.toByte)
    assert(Packing.pack(decoded10) sameElements encoded10)
    assert(Packing.unpack(encoded10) sameElements decoded10)

    // Very long solid chunk of zero data
    val decoded11 = Array.fill[Byte](8 * 400)(0)
    val encoded11 = Array[Byte](0, 255.toByte, 0, 143.toByte)
    assert(Packing.pack(decoded11) sameElements encoded11)
    assert(Packing.unpack(encoded11) sameElements decoded11)

    // Solid chunk of non-zero data
    val decoded12 = Array.tabulate[Byte](8 * 200)(i => (1 + i % 5).toByte)
    val encoded12 = Array.tabulate[Byte](10 + 8 * 199) {
      case 0 => 0xFF.toByte
      case i @ (1 | 2 | 3 | 4 | 5 | 6 | 7 | 8) => (1 + (i - 1) % 5).toByte
      case 9 => 199.toByte
      case i => (1 + (i - 2) % 5).toByte
    }
    assert(Packing.pack(decoded12) sameElements encoded12)
    assert(Packing.unpack(encoded12) sameElements decoded12)

    // Very long solid chunk of non-zero data
    val decoded13 = Array.fill[Byte](8 * 400)(42)
    val encoded13 = Array.tabulate[Byte](10 * 2 + 8 * 398) {
      case 0 | 9 | 2050 => 0xFF.toByte
      case 2059 => 143.toByte
      case _ => 42
    }
    assert(Packing.pack(decoded13) sameElements encoded13)
    assert(Packing.unpack(encoded13) sameElements decoded13)

  }

  "an array of bytes whose length is a multiple of 8" should "roundtrip when packed and then unpacked" in {
    forAll { (bytes: Array[Byte]) =>
      val paddedBytes = Packing.zeroPad(bytes)
      Packing.unpack(Packing.pack(paddedBytes)) sameElements paddedBytes
    }
  }
}

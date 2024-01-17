package com.thatdot.quine.bolt

import org.apache.pekko.util.ByteString

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.QuineIdLongProvider
import com.thatdot.quine.graph.cypher.{Expr, Value}
import com.thatdot.quine.util.ByteConversions

/* This tests the examples given in https://boltprotocol.org/v1/#serialization.
 * Note that the tests themselves use `HexConversions`.
 *
 * TODO: add quick-check roundtrip tests
 * TODO: add tests for relationships and nodes
 */
class BoltSerialization extends AnyFunSuite {

  def toHex(str: String): ByteString = ByteString(ByteConversions.parseHexBinary(str.filter(_ != ' ')))

  implicit val idProv: QuineIdLongProvider = QuineIdLongProvider()
  val bolt: Serialization = Serialization()

  def read(payload: String): Value = bolt.readFull(bolt.readValue)(toHex(payload))
  def write(value: Value): ByteString = bolt.writeFull(bolt.writeValue)(value)

  test("reading null") {
    assert(read("C0") == Expr.Null)
  }

  test("reading booleans") {
    assert(read("C3") == Expr.True)
    assert(read("C2") == Expr.False)
  }

  test("reading integers") {
    assert(read("01") == Expr.Integer(1)) // TINY_INT
    assert(read("05") == Expr.Integer(5)) // TINY_INT
    assert(read("2A") == Expr.Integer(42)) // TINY_INT
    assert(read("F5") == Expr.Integer(-11)) // TINY_INT

    assert(read("C9 04 D2") == Expr.Integer(1234))

    assert(
      read("CB 80 00 00  00 00 00 00  00") == Expr.Integer(-9223372036854775808L)
    ) // Min integer
    assert(
      read("CB 7F FF FF  FF FF FF FF  FF") == Expr.Integer(9223372036854775807L)
    ) // Max integer
  }

  test("reading floats") {
    assert(read("C1 40 19 21  FB 54 44 2D  18") == Expr.Floating(6.283185307179586))
    assert(read("C1 BF F1 99  99 99 99 99  9A") == Expr.Floating(-1.1))
    assert(read("C1 3F F1 99  99 99 99 99  9A") == Expr.Floating(1.1))
  }

  test("reading strings") {
    assert(read("80") == Expr.Str(""))
    assert(read("81 41") == Expr.Str("A"))
    assert(read("81 61") == Expr.Str("a"))
    assert(
      read(
        "D0 12 47 72  C3 B6 C3 9F  65 6E 6D 61  C3 9F 73 74" +
        "C3 A4 62 65"
      ) == Expr.Str("Größenmaßstäbe")
    )
    assert(
      read(
        "D0 1A 61 62  63 64 65 66  67 68 69 6A  6B 6C 6D 6E" +
        "6F 70 71 72  73 74 75 76  77 78 79 7A"
      ) == Expr.Str("abcdefghijklmnopqrstuvwxyz")
    )
    assert(
      read(
        "D0 18 45 6E  20 C3 A5 20  66 6C C3 B6  74 20 C3 B6" +
        "76 65 72 20  C3 A4 6E 67  65 6E"
      ) == Expr.Str("En å flöt över ängen")
    )
    assert(read("D0 81 " + ("61" * 129)) == Expr.Str("a" * 129))
  }

  test("reading lists") {
    assert(read("90") == Expr.List(Vector.empty))
    assert(
      read("93 01 02 03") ==
        Expr.List(Vector(1L, 2L, 3L).map(Expr.Integer(_)))
    )
    assert(
      read(
        "D4 14 01 02  03 04 05 06  07 08 09 00  01 02 03 04" +
        "05 06 07 08  09 00"
      ) == Expr.List(
        Vector(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 0L).map(
          Expr.Integer(_)
        )
      )
    )
    assert(
      read(
        "D4 28 01 02  03 04 05 06  07 08 09 0A  0B 0C 0D 0E" +
        "0F 10 11 12  13 14 15 16  17 18 19 1A  1B 1C 1D 1E" +
        "1F 20 21 22  23 24 25 26  27 28"
      ) == Expr.List((1L to 40L).map(Expr.Integer(_)).toVector)
    )
  }

  test("reading maps") {
    assert(read("A0") == Expr.Map(Map.empty))
    assert(
      read("A1 81 61 01") == Expr.Map(
        Map(
          "a" -> Expr.Integer(1)
        )
      )
    )
    assert(
      read("A1 83 6F 6E  65 84 65 69  6E 73") == Expr.Map(
        Map(
          "one" -> Expr.Str("eins")
        )
      )
    )
    assert(
      read(
        "D8 10 81 61  01 81 62 01  81 63 03 81  64 04 81 65" +
        "05 81 66 06  81 67 07 81  68 08 81 69  09 81 6A 00" +
        "81 6B 01 81  6C 02 81 6D  03 81 6E 04  81 6F 05 81" +
        "70 06"
      ) == Expr.Map(
        Map(
          "a" -> Expr.Integer(1L),
          "b" -> Expr.Integer(1L),
          "c" -> Expr.Integer(3L),
          "d" -> Expr.Integer(4L),
          "e" -> Expr.Integer(5L),
          "f" -> Expr.Integer(6L),
          "g" -> Expr.Integer(7L),
          "h" -> Expr.Integer(8L),
          "i" -> Expr.Integer(9L),
          "j" -> Expr.Integer(0L),
          "k" -> Expr.Integer(1L),
          "l" -> Expr.Integer(2L),
          "m" -> Expr.Integer(3L),
          "n" -> Expr.Integer(4L),
          "o" -> Expr.Integer(5L),
          "p" -> Expr.Integer(6L)
        )
      )
    )
  }

  test("writing null") {
    assert(toHex("C0") == write(Expr.Null))
  }

  test("writing booleans") {
    assert(toHex("C3") == write(Expr.True))
    assert(toHex("C2") == write(Expr.False))
  }

  test("writing integers") {
    assert(toHex("01") == write(Expr.Integer(1))) // TINY_INT
    assert(toHex("05") == write(Expr.Integer(5))) // TINY_INT
    assert(toHex("2A") == write(Expr.Integer(42))) // TINY_INT
    assert(toHex("F5") == write(Expr.Integer(-11))) // TINY_INT

    assert(toHex("C9 04 D2") == write(Expr.Integer(1234)))

    // Max & min integer
    assert(
      toHex("CB 80 00 00  00 00 00 00  00") ==
        write(Expr.Integer(-9223372036854775808L))
    )
    assert(
      toHex("CB 7F FF FF  FF FF FF FF  FF") ==
        write(Expr.Integer(9223372036854775807L))
    )
  }

  test("writing floats") {
    assert(toHex("C1 40 19 21  FB 54 44 2D  18") == write(Expr.Floating(6.283185307179586)))
    assert(toHex("C1 BF F1 99  99 99 99 99  9A") == write(Expr.Floating(-1.1)))
    assert(toHex("C1 3F F1 99  99 99 99 99  9A") == write(Expr.Floating(1.1)))
  }

  test("writing strings") {
    assert(toHex("80") == write(Expr.Str("")))
    assert(toHex("81 41") == write(Expr.Str("A")))
    assert(toHex("81 61") == write(Expr.Str("a")))
    assert(
      toHex(
        "D0 12 47 72  C3 B6 C3 9F  65 6E 6D 61  C3 9F 73 74" +
        "C3 A4 62 65"
      ) == write(Expr.Str("Größenmaßstäbe"))
    )
    assert(
      toHex(
        "D0 1A 61 62  63 64 65 66  67 68 69 6A  6B 6C 6D 6E" +
        "6F 70 71 72  73 74 75 76  77 78 79 7A"
      ) == write(Expr.Str("abcdefghijklmnopqrstuvwxyz"))
    )
    assert(
      toHex(
        "D0 18 45 6E  20 C3 A5 20  66 6C C3 B6  74 20 C3 B6" +
        "76 65 72 20  C3 A4 6E 67  65 6E"
      ) == write(Expr.Str("En å flöt över ängen"))
    )
    assert(toHex("D0 81 " + ("61" * 129)) == write(Expr.Str("a" * 129)))
  }

  test("writing lists") {
    assert(toHex("90") == write(Expr.List(Vector.empty)))
    assert(
      toHex("93 01 02 03") ==
        write(Expr.List(Vector(1L, 2L, 3L).map(Expr.Integer(_))))
    )
    assert(
      toHex(
        "D4 14 01 02  03 04 05 06  07 08 09 00  01 02 03 04" +
        "05 06 07 08  09 00"
      ) ==
        write(
          Expr.List(
            Vector(
              1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 0L
            ).map(Expr.Integer(_))
          )
        )
    )
    assert(
      toHex(
        "D4 28 01 02  03 04 05 06  07 08 09 0A  0B 0C 0D 0E" +
        "0F 10 11 12  13 14 15 16  17 18 19 1A  1B 1C 1D 1E" +
        "1F 20 21 22  23 24 25 26  27 28"
      ) ==
        write(Expr.List((1L to 40L).map(Expr.Integer(_)).toVector))
    )
  }

  test("writing maps") {
    assert(toHex("A0") == write(Expr.Map(Map.empty)))
    assert(
      toHex("A1 81 61 01") == write(
        Expr.Map(
          Map(
            "a" -> Expr.Integer(1)
          )
        )
      )
    )
    assert(
      toHex("A1 83 6F 6E  65 84 65 69  6E 73") ==
        write(Expr.Map(Map("one" -> Expr.Str("eins"))))
    )
    assert(
      toHex(
        "D8 10 81 61  01 81 62 01  81 63 03 81  64 04 81 65" +
        "05 81 66 06  81 67 07 81  68 08 81 69  09 81 6A 00" +
        "81 6B 01 81  6C 02 81 6D  03 81 6E 04  81 6F 05 81" +
        "70 06"
      ) == write(
        Expr.Map(
          collection.immutable.ListMap(
            "a" -> Expr.Integer(1L),
            "b" -> Expr.Integer(1L),
            "c" -> Expr.Integer(3L),
            "d" -> Expr.Integer(4L),
            "e" -> Expr.Integer(5L),
            "f" -> Expr.Integer(6L),
            "g" -> Expr.Integer(7L),
            "h" -> Expr.Integer(8L),
            "i" -> Expr.Integer(9L),
            "j" -> Expr.Integer(0L),
            "k" -> Expr.Integer(1L),
            "l" -> Expr.Integer(2L),
            "m" -> Expr.Integer(3L),
            "n" -> Expr.Integer(4L),
            "o" -> Expr.Integer(5L),
            "p" -> Expr.Integer(6L)
          )
        )
      )
    )
  }
}

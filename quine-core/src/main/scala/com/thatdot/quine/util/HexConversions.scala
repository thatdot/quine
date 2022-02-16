package com.thatdot.quine.util

/** Conversions back and forth between arrays of bytes and strings of hexadecimal characters */
object HexConversions {
  final private val HexArray = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  /** Convert a byte array into an even-length string of hexadecimal characters
    *
    * @param bytes byte array
    * @return even-length string of hexadecimal characters
    */
  def formatHexBinary(bytes: Array[Byte]): String = {
    val hexChars: Array[Char] = new Array(bytes.length * 2)
    var j = 0
    while (j < bytes.length) {
      val byteVal = bytes(j) & 0xFF
      hexChars(j * 2) = HexArray(byteVal >>> 4)
      hexChars(j * 2 + 1) = HexArray(byteVal & 0x0F)
      j += 1
    }
    new String(hexChars)
  }

  /** Convert an even-length string of hexadecimal characters into a byte array
    *
    * @param str even-length string of hexadecimal characters
    * @return byte array
    */
  @throws[IllegalArgumentException]("if the hex string has an odd length or a non-hex character")
  def parseHexBinary(str: String): Array[Byte] = {
    if (str.length % 2 != 0) {
      val msg = s"Hex input string must have even-length: $str"
      throw new IllegalArgumentException(msg)
    }

    val bytes: Array[Byte] = new Array(str.length / 2)
    var j = 0
    while (j < bytes.length) {
      val hiByte: Int = Character.digit(str.charAt(j * 2), 16)
      if (hiByte == -1) {
        val msg = s"Hex input string has a non-hex character at index ${j * 2}: $str"
        throw new IllegalArgumentException(msg)
      }

      val loByte: Int = Character.digit(str.charAt(j * 2 + 1), 16)
      if (loByte == -1) {
        val msg = s"Hex input string has a non-hex character at index ${j * 2 + 1}: $str"
        throw new IllegalArgumentException(msg)
      }

      bytes(j) = (hiByte * 16 + loByte).toByte
      j += 1
    }
    bytes
  }
}

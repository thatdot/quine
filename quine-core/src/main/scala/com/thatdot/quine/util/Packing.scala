package com.thatdot.quine.util

import java.nio.ByteBuffer

/** Implementation of the packing protocol described by Cap'n Proto
  *
  * For formats like FlatBuffers or Cap'n Proto, there tend to be lots of zeroes
  * in the serialized payloads. This packing protocol takes advantage of this
  * to try to reduce the size of payloads.
  *
  * @see [[https://capnproto.org/encoding.html#serialization-over-a-stream]]
  */
object Packing {

  /** Zero-pad data in preparation for packing
    * Pads on the right to the nearest multiple of 8 bytes
    *
    * @note whenever possible, try to do the packing _without_ this wasteful copy
    */
  def zeroPad(unpacked: Array[Byte]): Array[Byte] = {
    val rem = unpacked.length % 8
    if (rem != 0) unpacked.padTo(unpacked.length + 8 - rem, 0.toByte) else unpacked
  }

  /** Pack a data payload
    *
    * @param unpackedData unpacked data input
    * @return packed data output
    */
  @throws[IllegalArgumentException]("if unpacked data length is not a multiple of 8 bytes")
  def pack(unpackedData: Array[Byte]): Array[Byte] = {
    if (unpackedData.length % 8 != 0)
      throw new IllegalArgumentException(
        s"Data cannot be packed (length must be a multiple of 8, but is ${unpackedData.length}"
      )

    /* Reserve 10/8 times the output, so that we _know_ we always have enough space
     * This makes it possible to skip the bounds check entirely
     *
     * For every word, there will be a tag, up to 8 bytes, and possible a suffix byte.
     * In the absolute worst case, that means every 8 bytes turn into 1 + 8 + 1 = 10 bytes.
     */
    val output = ByteBuffer.allocate(unpackedData.length * 10 / 8)
    val input = ByteBuffer.wrap(unpackedData).asReadOnlyBuffer()
    val inputLimit: Int = unpackedData.length

    while (input.hasRemaining) {

      // Leave space for a tag byte (we'll come back for it)
      val tagPos: Int = output.position()
      output.put(0.toByte)

      // Morally a byte, but bit-operations in Scala typecheck on `Int`...
      var tag: Int = 0

      // TODO: consider unrolling this fixed-length loop
      var i: Int = 0
      while (i < 8) {
        input.get() match {
          case 0 => // skip
          case b =>
            tag |= 1 << i
            output.put(b)
        }
        i += 1
      }

      // Now go back to write the tag
      output.put(tagPos, tag.toByte)

      if (tag == 0) {
        // Try to look for more zeroed out 8-byte words
        var zeroedWordsFound: Int = 0

        var inputPosition: Int = input.position()
        while (inputPosition + 8 <= inputLimit && zeroedWordsFound < 0xFF && input.getLong(inputPosition) == 0) {
          zeroedWordsFound += 1
          inputPosition += 8
        }

        // Write out the number of zeroed words found
        output.put(zeroedWordsFound.toByte)
        input.position(inputPosition)
      } else if (tag == 0xFF) {
        // Try to look for more incompressible bytes
        var incompressibleWordsFound: Int = 0
        val initialInputPosition: Int = input.position()

        var inputPosition: Int = input.position()
        while (
          inputPosition + 8 <= inputLimit && incompressibleWordsFound < 0xFF && {
            var zeroBytesInWord: Int = 0
            var j: Int = 0
            while (j < 8) {
              input.get(inputPosition + j) match {
                case 0 => zeroBytesInWord += 1
                case _ => // skip
              }
              j += 1
            }

            // As soon as there is more than 1 zero, the word is no worse off being compressed
            // TODO: consider benchmarking if this heuristic is any good
            zeroBytesInWord <= 1
          }
        ) {
          incompressibleWordsFound += 1
          inputPosition += 8
        }

        // Write out the number of incompressible words found
        output.put(incompressibleWordsFound.toByte)

        // Write out the incompressible words themselves
        if (incompressibleWordsFound != 0) {
          val incompressibleByteLength: Int = incompressibleWordsFound * 8
          input.position(initialInputPosition)
          val incompressibleSlice: ByteBuffer = input.slice()
          incompressibleSlice.limit(incompressibleByteLength)
          output.put(incompressibleSlice)
        }
        input.position(inputPosition)
      }
    }

    val packedOutput = new Array[Byte](output.position())
    output.rewind()
    output.get(packedOutput)
    packedOutput
  }

  /** Unpack a data payload
    *
    * @param packedData packed data input
    * @return unpacked data output
    */
  def unpack(packedData: Array[Byte]): Array[Byte] = {
    var output = ByteBuffer.allocate(packedData.length * 2)
    val input = ByteBuffer.wrap(packedData).asReadOnlyBuffer()

    while (input.hasRemaining) {

      // Ensure there's space for at least 8 bytes
      if (output.remaining() < 8) {
        output = resize(output, 8)
      }

      input.get() match {
        case 0 =>
          output.putLong(0L)

          // Additional 0 words
          val zeroedBytesFound: Int = (input.get() & 0xFF) * 8
          if (zeroedBytesFound > 0) {
            if (output.remaining() < zeroedBytesFound) {
              output = resize(output, zeroedBytesFound)
            }

            // The buffer is zero-initialized, so just advance the position
            output.position(output.position() + zeroedBytesFound)
          }

        case -1 =>
          output.putLong(input.getLong())

          // Additional uncompressed words
          val incompressibleBytesFound: Int = (input.get() & 0xFF) * 8
          if (incompressibleBytesFound > 0) {
            if (output.remaining() < incompressibleBytesFound) {
              output = resize(output, incompressibleBytesFound)
            }
            val incompressibleSlice = input.slice()
            incompressibleSlice.limit(incompressibleBytesFound)
            output.put(incompressibleSlice)
            input.position(input.position() + incompressibleBytesFound)
          }

        case tag =>
          // Advance one bit at a time through the tag
          var mask: Int = 1
          while (mask < 0xFF) {
            output.put(if ((tag & mask) != 0) input.get() else 0.toByte)
            mask <<= 1
          }
      }
    }

    val unpackedOutput = new Array[Byte](output.position())
    output.rewind()
    output.get(unpackedOutput)
    unpackedOutput
  }

  /** Make a new larger buffer, preserving the position and the prefix of data
    *
    * @param buffer buffer to resize
    * @param minExtraLength minimum extra number of bytes we need
    * @return resized buffer
    */
  private[this] def resize(buffer: ByteBuffer, minExtraLength: Int): ByteBuffer = {
    val oldPos: Int = buffer.position()
    val newLength = Math.max(buffer.capacity * 2, oldPos + minExtraLength)
    val newBuffer = ByteBuffer.allocate(newLength)

    buffer.position(0)
    newBuffer.put(buffer)
    newBuffer.position(oldPos)
    newBuffer
  }
}

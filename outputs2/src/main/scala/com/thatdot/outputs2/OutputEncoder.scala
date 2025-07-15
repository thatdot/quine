package com.thatdot.outputs2

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import scala.reflect.ClassTag

import com.google.protobuf.Descriptors.Descriptor

import com.thatdot.data.DataFolderTo
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.serialization.QuineValueToProtobuf
import com.thatdot.quine.serialization.data.QuineSerializationFoldersTo

sealed trait OutputEncoder {
  type Repr
  val reprTag: ClassTag[Repr]
  def folderTo: DataFolderTo[Repr]
  def bytes(value: Repr): Array[Byte]
}

sealed trait BytesOutputEncoder[Repr] {
  def bytes(value: Repr): Array[Byte]
}

object BytesOutputEncoder {
  def apply[A](f: A => Array[Byte]): BytesOutputEncoder[A] = new BytesOutputEncoder[A] {
    override def bytes(value: A): Array[Byte] = f(value)
  }
}

object OutputEncoder {

  /** A JSON encoder for a [[charset]] that yields a byte array of a JSON value with a new line character appended.
    *
    * *NOTE* We do not currently allow the [[charset]] to be set via the API, but when we do, we will need
    * to adapt [[com.thatdot.model.v2.outputs.ResultDestination.Bytes.File]] to also accommodate the `charset`
    * (right now, it assumes UTF_8, since that's the default here)!
    *
    * @param charset the character set to use in encoding the [[io.circe.Json]] value to {{{Array[Byte]}}}
    */
  case class JSON(charset: Charset = StandardCharsets.UTF_8) extends OutputEncoder {

    import io.circe.{Json, Printer}

    type Repr = Json
    val reprTag: ClassTag[Repr] = implicitly[ClassTag[Repr]]

    override def folderTo: DataFolderTo[Repr] = DataFolderTo.jsonFolder

    private val printer = Printer.noSpaces

    private val newline: Array[Byte] = {
      val buf = charset.encode("\n")
      val arr = Array.ofDim[Byte](buf.limit() - buf.position())
      buf.get(arr)
      arr
    }

    override def bytes(value: Repr): Array[Byte] = {
      val buffer = printer.printToByteBuffer(value, charset)
      val bufSize = buffer.limit() - buffer.position()
      val arr = Array.ofDim[Byte](bufSize + newline.length)

      // Add the JSON bytes to the array
      buffer.get(arr, 0, bufSize)

      // Add the newline bytes after the JSON bytes
      ByteBuffer.wrap(newline).get(arr, bufSize, newline.length)
      arr
    }
  }

  final case class Protobuf(
    schemaUrl: String,
    typeName: String,
    descriptor: Descriptor,
  ) extends OutputEncoder {
    override type Repr = QuineValue
    val reprTag: ClassTag[Repr] = implicitly[ClassTag[Repr]]

    private val toPb: QuineValueToProtobuf = new QuineValueToProtobuf(descriptor)

    override def folderTo: DataFolderTo[Repr] = QuineSerializationFoldersTo.quineValueFolder

    override def bytes(value: Repr): Array[Byte] =
      value match {
        case QuineValue.Map(map) =>
          toPb
            .toProtobufBytes(map)
            .fold[Array[Byte]](
              failure => throw new Exception(failure.toString),
              identity,
            )
        case _ => throw new Exception("Unable to convert a non-map to Protobuf")
      }
  }
}

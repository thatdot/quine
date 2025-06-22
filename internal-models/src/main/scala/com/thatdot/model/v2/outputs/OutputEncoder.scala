package com.thatdot.model.v2.outputs

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
  case class JSON(charset: Charset = StandardCharsets.UTF_8) extends OutputEncoder {

    import io.circe.{Json, Printer}

    type Repr = Json
    val reprTag: ClassTag[Repr] = implicitly[ClassTag[Repr]]

    override def folderTo: DataFolderTo[Repr] = DataFolderTo.jsonFolder

    override def bytes(value: Repr): Array[Byte] = {
      val printer = Printer.noSpaces
      printer.printToByteBuffer(value, charset).array()
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

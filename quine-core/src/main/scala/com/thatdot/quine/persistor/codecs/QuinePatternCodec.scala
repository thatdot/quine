package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder

import com.thatdot.quine.graph.cypher.{BinOp, QuinePattern}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.Offset
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object QuinePatternCodec extends PersistenceCodec[QuinePattern] {
  private[this] def getTypeByte(qp: QuinePattern): Byte = qp match {
    case QuinePattern.QuineUnit => persistence.QuinePattern.QuineUnit
    case _: QuinePattern.Node => persistence.QuinePattern.Node
    case _: QuinePattern.Edge => persistence.QuinePattern.Edge
    case _: QuinePattern.Fold => persistence.QuinePattern.Fold
  }

  private[this] def getFoldTypeByte(op: BinOp): Byte = op match {
    case BinOp.Merge => persistence.BinOp.Merge
    case BinOp.Append => persistence.BinOp.Append
  }

  private[this] def writeQuinePattern(
    builder: FlatBufferBuilder,
    qp: QuinePattern,
  ): Offset =
    qp match {
      case QuinePattern.QuineUnit => persistence.QuineUnit.endQuineUnit(builder)
      case node: QuinePattern.Node =>
        val bindingOffset = builder.createString(node.binding.name.name)
        persistence.Node.createNode(builder, bindingOffset)
      case edge: QuinePattern.Edge =>
        val labelOffset = builder.createString(edge.edgeLabel.name)
        val remoteTy = getTypeByte(edge.remotePattern)
        persistence.Edge.createEdge(builder, labelOffset, remoteTy, writeQuinePattern(builder, edge.remotePattern))
      case fold: QuinePattern.Fold =>
        val initTy = getTypeByte(fold.init)
        val overTys = builder.createByteVector(fold.over.map(getTypeByte).toArray)
        val overOffset =
          builder.createVectorOfTables(fold.over.map(innerPattern => writeQuinePattern(builder, innerPattern)).toArray)
        persistence.Fold.createFold(
          builder,
          initTy,
          writeQuinePattern(builder, fold.init),
          overTys,
          overOffset,
          getFoldTypeByte(fold.f),
          fold.f match {
            case BinOp.Merge => persistence.Merge.endMerge(builder)
            case BinOp.Append => persistence.Append.endAppend(builder)
          },
          persistence.Output.endOutput(builder),
        )
    }

  val format: BinaryFormat[QuinePattern] = new PackedFlatBufferBinaryFormat[QuinePattern] {
    def writeToBuffer(builder: FlatBufferBuilder, qp: QuinePattern): Offset =
      writeQuinePattern(builder, qp)

    def readFromBuffer(buffer: ByteBuffer): QuinePattern = ???
///      readQuinePattern(persistence.QuinePattern.getRootAsQuinePattern(buffer))
  }
}

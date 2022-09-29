package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder

import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object CypherExprCodec extends PersistenceCodec[Expr] {
  private[this] def writeBoxedCypherExpr(builder: FlatBufferBuilder, expr: cypher.Expr): Offset = {
    val TypeAndOffset(exprTyp, exprOff) = writeCypherExpr(builder, expr)
    persistence.BoxedCypherExpr.createBoxedCypherExpr(builder, exprTyp, exprOff)
  }

  private[this] def readBoxedCypherExpr(expr: persistence.BoxedCypherExpr): cypher.Expr =
    readCypherExpr(expr.exprType, expr.expr(_))

  val format: BinaryFormat[Expr] = new PackedFlatBufferBinaryFormat[cypher.Expr] {
    def writeToBuffer(builder: FlatBufferBuilder, expr: cypher.Expr): Offset =
      writeBoxedCypherExpr(builder, expr)

    def readFromBuffer(buffer: ByteBuffer): cypher.Expr =
      readBoxedCypherExpr(persistence.BoxedCypherExpr.getRootAsBoxedCypherExpr(buffer))
  }

}

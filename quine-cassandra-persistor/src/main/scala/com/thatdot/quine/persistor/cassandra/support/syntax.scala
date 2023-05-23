package com.thatdot.quine.persistor.cassandra.support

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.{MappingCodec, PrimitiveLongCodec, TypeCodec}
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.cql.{BoundStatement, BoundStatementBuilder, PreparedStatement}

object syntax {
  private def genericType[A](implicit tag: ClassTag[A]): GenericType[A] =
    GenericType.of[A](tag.runtimeClass.asInstanceOf[Class[A]])

  implicit class PrimitiveLongCodecSyntax(longCodec: PrimitiveLongCodec) {
    def xmap[B: ClassTag](from: Long => B, to: B => Long): TypeCodec[B] = new TypeCodec[B] {
      override val getJavaType: GenericType[B] = genericType
      override val getCqlType: DataType = longCodec.getCqlType
      override def encode(value: B, protocolVersion: ProtocolVersion): ByteBuffer =
        longCodec.encodePrimitive(to(value), protocolVersion)
      override def decode(bytes: ByteBuffer, protocolVersion: ProtocolVersion): B = from(
        longCodec.decodePrimitive(bytes, protocolVersion)
      )
      override def format(value: B): String = longCodec.format(to(value))
      override def parse(value: String): B = from(longCodec.parse(value))
    }
  }
  implicit class TypeCodecSyntax[A](innerCodec: TypeCodec[A]) {
    def xmap[B: ClassTag](from: A => B, to: B => A): TypeCodec[B] =
      new MappingCodec[A, B](innerCodec, genericType) {
        override def innerToOuter(value: A): B = from(value)
        override def outerToInner(value: B): A = to(value)
      }
  }

  implicit class PreparedStatementBinding(statement: PreparedStatement) {
    def bindColumns(bindings: BoundStatementBuilder => BoundStatementBuilder*): BoundStatement =
      bindings.foldRight((statement.boundStatementBuilder()))(_ apply _).build()
  }
}

package com.thatdot.quine.persistor.cassandra.support

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.datastax.oss.driver.api.core.`type`.codec.ExtraTypeCodecs.BLOB_TO_ARRAY
import com.datastax.oss.driver.api.core.`type`.codec.{TypeCodec, TypeCodecs}

import com.thatdot.quine.graph.{
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  StandingQueryId,
}
import com.thatdot.quine.model.{DomainGraphNode, EdgeDirection, QuineId}
import com.thatdot.quine.persistor.BinaryFormat
import com.thatdot.quine.persistor.codecs.{DomainGraphNodeCodec, DomainIndexEventCodec, NodeChangeEventCodec}

object CassandraCodecs {
  import syntax._
  def fromBinaryFormat[A: ClassTag](format: BinaryFormat[A]): TypeCodec[A] =
    BLOB_TO_ARRAY.xmap(format.read(_).get, format.write)
  implicit val byteArrayCodec: TypeCodec[Array[Byte]] = BLOB_TO_ARRAY
  implicit val stringCodec: TypeCodec[String] = TypeCodecs.TEXT
  implicit val symbolCodec: TypeCodec[Symbol] = TypeCodecs.TEXT.xmap(Symbol(_), _.name)
  implicit val intCodec: TypeCodec[Int] = TypeCodecs.INT.asInstanceOf[TypeCodec[Int]]
  implicit val longCodec: TypeCodec[Long] = TypeCodecs.BIGINT.asInstanceOf[TypeCodec[Long]]
  implicit def listCodec[A](implicit elemCodec: TypeCodec[A]): TypeCodec[Seq[A]] =
    TypeCodecs.listOf(elemCodec).xmap(_.asScala.toSeq, _.asJava)
  implicit def setCodec[A](implicit elemCodec: TypeCodec[A]): TypeCodec[Set[A]] =
    TypeCodecs.setOf(elemCodec).xmap(_.asScala.toSet, _.asJava)
  implicit val quineIdCodec: TypeCodec[QuineId] = BLOB_TO_ARRAY.xmap(QuineId(_), _.array)
  implicit val edgeDirectionCodec: TypeCodec[EdgeDirection] =
    TypeCodecs.TINYINT.xmap(b => EdgeDirection.values(b.intValue), _.index)
  implicit val standingQueryIdCodec: TypeCodec[StandingQueryId] = TypeCodecs.UUID.xmap(StandingQueryId(_), _.uuid)
  implicit val MultipleValuesStandingQueryPartIdCodec: TypeCodec[MultipleValuesStandingQueryPartId] =
    TypeCodecs.UUID.xmap(MultipleValuesStandingQueryPartId(_), _.uuid)

  /** [[EventTime]] is represented using Cassandra's 64-bit `bigint`
    *
    * Since event time ordering is unsigned, we need to shift over the raw
    * underlying long by [[Long.MinValue]] in order to ensure that ordering
    * gets mapped over properly to the signed `bigint` ordering.
    *
    * {{{
    * EventTime.MinValue -> 0L + Long.MaxValue + 1L = Long.MinValue   // smallest event time
    * EventTime.MaxValue -> -1L + Long.MaxValue + 1L = Long.MaxValue  // largest event time
    * }}}
    */
  implicit val eventTimeCodec: TypeCodec[EventTime] =
    TypeCodecs.BIGINT.xmap(x => EventTime.fromRaw(x - Long.MaxValue - 1L), x => x.eventTime + Long.MaxValue + 1L)

  implicit val nodeChangeEventCodec: TypeCodec[NodeChangeEvent] = fromBinaryFormat(NodeChangeEventCodec.format)
  implicit val domainIndexEventCodec: TypeCodec[DomainIndexEvent] = fromBinaryFormat(DomainIndexEventCodec.format)
  implicit val domainGraphNodeCodec: TypeCodec[DomainGraphNode] = fromBinaryFormat(DomainGraphNodeCodec.format)

}

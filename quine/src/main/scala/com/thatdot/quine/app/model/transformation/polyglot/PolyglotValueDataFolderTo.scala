package com.thatdot.quine.app.model.transformation.polyglot

import java.time._

import scala.collection.immutable.SortedMap

import org.graalvm.polyglot
import org.graalvm.polyglot.proxy.{ProxyArray, ProxyObject}

import com.thatdot.data.DataFolderTo

object PolyglotValueDataFolderTo extends DataFolderTo[Polyglot.HostValue] {
  def nullValue: Polyglot.HostValue = null

  val trueValue: Polyglot.HostValue = Boolean.box(true)

  val falseValue: Polyglot.HostValue = Boolean.box(false)

  def integer(l: Long): Polyglot.HostValue = Long.box(l)

  def string(s: String): Polyglot.HostValue = s

  def bytes(b: Array[Byte]): Polyglot.HostValue = b

  def floating(d: Double): Polyglot.HostValue = Double.box(d)

  def date(d: LocalDate): Polyglot.HostValue = d

  override def time(t: OffsetTime): Polyglot.HostValue = t

  def localTime(t: LocalTime): Polyglot.HostValue = t

  def localDateTime(ldt: LocalDateTime): Polyglot.HostValue = ldt

  def zonedDateTime(zdt: ZonedDateTime): Polyglot.HostValue = zdt

  def duration(d: Duration): Polyglot.HostValue = d

  def vectorBuilder(): DataFolderTo.CollectionBuilder[Polyglot.HostValue] =
    new DataFolderTo.CollectionBuilder[Polyglot.HostValue] {
      private val elements = Vector.newBuilder[Polyglot.HostValue]
      def add(a: Polyglot.HostValue): Unit = elements += a

      def finish(): Polyglot.HostValue = VectorProxy(elements.result())
    }

  def mapBuilder(): DataFolderTo.MapBuilder[Polyglot.HostValue] = new DataFolderTo.MapBuilder[Polyglot.HostValue] {
    private val kvs = SortedMap.newBuilder[String, Polyglot.HostValue]

    def add(key: String, value: Polyglot.HostValue): Unit = kvs += (key -> value)

    def finish(): Polyglot.HostValue = MapProxy(kvs.result())
  }

  final case class VectorProxy(underlying: Vector[Polyglot.HostValue]) extends ProxyArray {
    def get(index: Long): Polyglot.HostValue = underlying(index.toInt)

    def set(index: Long, value: polyglot.Value): Unit = throw new UnsupportedOperationException

    def getSize: Long = underlying.size.toLong
  }

  final private case class MapProxy(underlying: Map[String, Polyglot.HostValue]) extends ProxyObject {
    def getMember(key: String): Polyglot.HostValue = underlying(key)

    def getMemberKeys: Polyglot.HostValue = VectorProxy(underlying.keys.toVector) // Could also just be List

    def hasMember(key: String): Boolean = underlying.contains(key)

    def putMember(key: String, value: polyglot.Value): Unit = throw new UnsupportedOperationException
  }

}

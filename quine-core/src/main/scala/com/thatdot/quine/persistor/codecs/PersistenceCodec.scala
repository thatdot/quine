package com.thatdot.quine.persistor.codecs

import java.util.UUID

import scala.collection.compat.immutable._

import com.google.flatbuffers.{FlatBufferBuilder, Table}
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.{cypher, _}
import com.thatdot.quine.model._
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset, TypeAndOffset, emptyTable}
import com.thatdot.quine.persistor.{BinaryFormat, PersistenceAgent}

/** The deserialization failed because a union (eg, a coproduct or enum) was tagged with an unknown type.
  *
  * Users hitting this error are probably trying to deserialize new union variants with old code, or removed union
  * variants with new code.
  *
  * @param typeTag union tag (which is invalid)
  * @param validTags array of valid tags (organized according to their indices)
  */
class InvalidUnionType(
  typeTag: Byte,
  validTags: Array[String]
) extends IllegalArgumentException(s"Invalid tag $typeTag (valid tags: ${validTags.mkString(", ")})")

class UnsupportedExtension(
  msg: String = s"Persisted data requires an extension to Quine that the current application does not support.",
  cause: Throwable = null
) extends IllegalArgumentException(msg, cause)

//TODO this is temporary, and only serves to mark places where the code is processing a NodeEvent type
// but expects a NodeChangeEvent or DomainIndexType. We can remove this when we remove the NodeEvent
// union type.
class InvalidEventType(
  event: NodeEvent,
  validTags: Array[String]
) extends IllegalArgumentException(
      s"The type ${event.getClass.getSimpleName} can not be processed as a NodeChangeEvent (valid types: ${validTags.mkString(", ")})"
    )

class InvalidPersistedQuineData(
  msg: String =
    s"Persisted data is invalid for the current version of Quine. Current Quine serialization version is ${PersistenceAgent.CurrentVersion}",
  cause: Throwable = null
) extends IllegalArgumentException(msg, cause)

/** FlatBuffer-based codecs
  *
  * == Choice of `FlatBuffer`
  *
  *   - support for backwards compatible schema evolution (and automated conformance checking)
  *   - minimal allocation overhead (avoids lots of allocations seen in eg. Protobuf)
  *   - incremental (lazy) deserialization (this is currently just a future plan)
  *   - allows the Scala code to "own" the class/trait definitions (unlike Protobuf)
  *   - fast serialization, somewhat compact output
  *
  * == Gotchas
  *
  *   - in the schema, unions must be defined before they can be used
  *   - unions can only be used as fields (since they actually desugar into a tag and value field)
  *   - fields are all optional by default, unless they are scalar types
  *   - serializing of objects (and vectors!) cannot be nested
  *
  * == Confusion
  *
  *   - is `null` supposed to work? The `has*` generated guards don't seem to work
  *   - sometimes the `create*` static methods are missing (eg. Instant)
  */
trait PersistenceCodec[T] extends LazyLogging {

  val format: BinaryFormat[T]

  private[this] def writeDuration(builder: FlatBufferBuilder, duration: java.time.Duration): Offset =
    persistence.Duration.createDuration(builder, duration.getSeconds, duration.getNano)

  private[this] def readDuration(duration: persistence.Duration): java.time.Duration =
    java.time.Duration.ofSeconds(duration.seconds, duration.nanos.toLong)

  private[this] def writeLocalDate(builder: FlatBufferBuilder, localDate: java.time.LocalDate): Offset =
    persistence.LocalDate.createLocalDate(
      builder,
      localDate.getYear,
      localDate.getMonthValue.toByte,
      localDate.getDayOfMonth.toByte
    )
  private[this] def readLocalDate(localDate: persistence.LocalDate): java.time.LocalDate =
    java.time.LocalDate.of(localDate.year, localDate.month.toInt, localDate.day.toInt)

  private[this] def writeLocalTime(builder: FlatBufferBuilder, localTime: java.time.LocalTime): Offset =
    persistence.LocalTime.createLocalTime(
      builder,
      localTime.getHour.toByte,
      localTime.getMinute.toByte,
      localTime.getSecond.toByte,
      localTime.getNano
    )

  private[this] def readLocalTime(localTime: persistence.LocalTime): java.time.LocalTime =
    java.time.LocalTime.of(localTime.hour.toInt, localTime.minute.toInt, localTime.second.toInt, localTime.nano)
  private[this] def writeOffsetTime(builder: FlatBufferBuilder, offsetTime: java.time.OffsetTime): Offset =
    persistence.OffsetTime.createOffsetTime(
      builder,
      offsetTime.getHour.toByte,
      offsetTime.getMinute.toByte,
      offsetTime.getSecond.toByte,
      offsetTime.getNano,
      (offsetTime.getOffset.getTotalSeconds / 60).toShort
    )
  private[this] def readOffsetTime(offsetTime: persistence.OffsetTime): java.time.OffsetTime =
    java.time.OffsetTime
      .of(readLocalTime(offsetTime.localTime), java.time.ZoneOffset.ofTotalSeconds(offsetTime.offset.toInt * 60))

  private[this] def writeInstant(builder: FlatBufferBuilder, instant: java.time.Instant): Offset =
    persistence.Instant.createInstant(builder, instant.getEpochSecond, instant.getNano)

  private[this] def readInstant(instant: persistence.Instant): java.time.Instant =
    java.time.Instant.ofEpochSecond(instant.seconds, instant.nanos.toLong)

  private[this] def writeLocalDateTime(builder: FlatBufferBuilder, localDateTime: java.time.LocalDateTime): Offset =
    persistence.LocalDateTime.createLocalDateTime(
      builder,
      localDateTime.getYear,
      localDateTime.getMonthValue.toByte,
      localDateTime.getDayOfMonth.toByte,
      localDateTime.getHour.toByte,
      localDateTime.getMinute.toByte,
      localDateTime.getSecond.toByte,
      localDateTime.getNano
    )

  private[this] def readLocalDateTime(localDateTime: persistence.LocalDateTime): java.time.LocalDateTime = {
    val localDate = readLocalDate(localDateTime.localDate)
    val localTime = readLocalTime(localDateTime.localTime)
    java.time.LocalDateTime.of(localDate, localTime)
  }

  private[this] def writeZonedDateTime(builder: FlatBufferBuilder, zonedDateTime: java.time.ZonedDateTime): Offset = {
    val zoneIdOff: Offset = builder.createString(zonedDateTime.getZone.getId)

    persistence.ZonedDateTime.startZonedDateTime(builder)
    val instantOff = writeInstant(builder, zonedDateTime.toInstant)
    persistence.ZonedDateTime.addInstant(builder, instantOff)
    persistence.ZonedDateTime.addZoneId(builder, zoneIdOff)
    persistence.ZonedDateTime.endZonedDateTime(builder)
  }

  private[this] def readZonedDateTime(zonedDateTime: persistence.ZonedDateTime): java.time.ZonedDateTime = {
    val instant = readInstant(zonedDateTime.instant)
    val zoneId = java.time.ZoneId.of(zonedDateTime.zoneId)
    java.time.ZonedDateTime.ofInstant(instant, zoneId)
  }

  protected[this] def edgeDirection2Byte(direction: EdgeDirection): Byte =
    direction match {
      case EdgeDirection.Outgoing => persistence.EdgeDirection.Outgoing
      case EdgeDirection.Incoming => persistence.EdgeDirection.Incoming
      case EdgeDirection.Undirected => persistence.EdgeDirection.Undirected
    }

  protected[this] def byte2EdgeDirection(direction: Byte): EdgeDirection =
    direction match {
      case persistence.EdgeDirection.Outgoing => EdgeDirection.Outgoing
      case persistence.EdgeDirection.Incoming => EdgeDirection.Incoming
      case persistence.EdgeDirection.Undirected => EdgeDirection.Undirected
      case other => throw new InvalidUnionType(other, persistence.EdgeDirection.names)
    }

  protected[this] def writeHalfEdge(builder: FlatBufferBuilder, edge: HalfEdge): Offset =
    persistence.HalfEdge.createHalfEdge(
      builder,
      builder.createSharedString(edge.edgeType.name),
      edgeDirection2Byte(edge.direction),
      writeQuineId(builder, edge.other)
    )

  protected[this] def readHalfEdge(edge: persistence.HalfEdge): HalfEdge =
    HalfEdge(
      Symbol(edge.edgeType),
      byte2EdgeDirection(edge.direction),
      readQuineId(edge.other)
    )
  protected[this] def writeQuineId(builder: FlatBufferBuilder, qid: QuineId): Offset =
    persistence.QuineId.createQuineId(
      builder,
      persistence.QuineId.createIdVector(builder, qid.array)
    )

  protected[this] def readQuineId(qid: persistence.QuineId): QuineId =
    QuineId(qid.idAsByteBuffer.remainingBytes)

  import org.msgpack.core.MessagePack

  protected[this] def writeQuineValue(builder: FlatBufferBuilder, quineValue: QuineValue): Offset =
    persistence.QuineValue.createQuineValue(
      builder,
      builder.createByteVector(QuineValue.writeMsgPack(quineValue))
    )

  protected[this] def readQuineValue(quineValue: persistence.QuineValue): QuineValue =
    QuineValue.readMsgPack(MessagePack.newDefaultUnpacker(quineValue.msgPackedAsByteBuffer))

  private[this] def writeCypherStr(builder: FlatBufferBuilder, str: cypher.Expr.Str): Offset = {
    val strOff: Offset = builder.createString(str.string)
    persistence.CypherStr.createCypherStr(builder, strOff)
  }

  private[this] def writeCypherInteger(builder: FlatBufferBuilder, integer: cypher.Expr.Integer): Offset =
    persistence.CypherInteger.createCypherInteger(builder, integer.long)

  private[this] def writeCypherFloating(builder: FlatBufferBuilder, floating: cypher.Expr.Floating): Offset =
    persistence.CypherFloating.createCypherFloating(builder, floating.double)

  private[this] def writeCypherBytes(builder: FlatBufferBuilder, bytes: cypher.Expr.Bytes): Offset = {
    val bytesOff: Offset = persistence.CypherBytes.createBytesVector(builder, bytes.b)
    persistence.CypherBytes.createCypherBytes(builder, bytesOff, bytes.representsId)
  }

  private[this] def writeCypherNode(builder: FlatBufferBuilder, node: cypher.Expr.Node): Offset = {
    val idOff: Offset = writeQuineId(builder, node.id)
    val labelsOff: Offset = {
      val labels: Set[Symbol] = node.labels
      val labelsOffs: Array[Offset] = new Array[Offset](node.labels.size)
      for ((label, i) <- labels.zipWithIndex)
        labelsOffs(i) = builder.createString(label.name)
      persistence.CypherNode.createLabelsVector(builder, labelsOffs)
    }
    val propertiesOff: Offset = {
      val properties: Map[Symbol, cypher.Value] = node.properties
      val propertiesOffs: Array[Offset] = new Array[Offset](properties.size)
      for (((key, value), i) <- properties.zipWithIndex) {
        val keyOff: Offset = builder.createString(key.name)
        val TypeAndOffset(valueType, valueOff) = writeCypherValue(builder, value)
        propertiesOffs(i) = persistence.CypherProperty.createCypherProperty(builder, keyOff, valueType, valueOff)
      }
      persistence.CypherNode.createPropertiesVector(builder, propertiesOffs)
    }
    persistence.CypherNode.createCypherNode(
      builder,
      idOff,
      labelsOff,
      propertiesOff
    )
  }

  private[this] def readCypherNode(node: persistence.CypherNode): cypher.Expr.Node = {
    val labels: Set[Symbol] = {
      val builder = Set.newBuilder[Symbol]
      var i = 0
      val labelsLength = node.labelsLength
      while (i < labelsLength) {
        builder += Symbol(node.labels(i))
        i += 1
      }
      builder.result()
    }
    val properties: Map[Symbol, cypher.Value] = {
      val builder = Map.newBuilder[Symbol, cypher.Value]
      var i = 0
      val propertiesLength = node.propertiesLength
      while (i < propertiesLength) {
        val property: persistence.CypherProperty = node.properties(i)
        val value: cypher.Value = readCypherValue(property.valueType, property.value)
        builder += Symbol(property.key) -> value
        i += 1
      }
      builder.result()
    }
    cypher.Expr.Node(readQuineId(node.id), labels, properties)
  }

  private[this] def readCypherPath(path: persistence.CypherPath): cypher.Expr.Path = {
    val head: cypher.Expr.Node = readCypherNode(path.head)
    val tails: Vector[(cypher.Expr.Relationship, cypher.Expr.Node)] = Vector.tabulate(path.tailsLength) { i =>
      val segment = path.tails(i)
      val rel = readCypherRelationship(segment.edge)
      val to = readCypherNode(segment.to)
      rel -> to
    }
    cypher.Expr.Path(head, tails)
  }

  private[this] def readCypherRelationship(relationship: persistence.CypherRelationship): cypher.Expr.Relationship = {
    val start: QuineId = readQuineId(relationship.start)
    val name: Symbol = Symbol(relationship.name)
    val properties: Map[Symbol, cypher.Value] = {
      val builder = Map.newBuilder[Symbol, cypher.Value]
      var i = 0
      val propertiesLength = relationship.propertiesLength
      while (i < propertiesLength) {
        val property: persistence.CypherProperty = relationship.properties(i)
        val value: cypher.Value = readCypherValue(property.valueType, property.value)
        builder += Symbol(property.key) -> value
        i += 1
      }
      builder.result()
    }
    val end: QuineId = readQuineId(relationship.end)
    cypher.Expr.Relationship(start, name, properties, end)
  }

  private[this] def readCypherList(list: persistence.CypherList): cypher.Expr.List = {
    val elements = Vector.tabulate(list.elementsLength) { i =>
      readCypherValue(list.elementsType(i), list.elements(_, i))
    }
    cypher.Expr.List(elements)
  }

  private[this] def readCypherMap(map: persistence.CypherMap): cypher.Expr.Map = {
    val entries = Map.newBuilder[String, cypher.Value]
    var i = 0
    val entriesLength = map.entriesLength
    while (i < entriesLength) {
      val entry: persistence.CypherProperty = map.entries(i)
      val value: cypher.Value = readCypherValue(entry.valueType, entry.value)
      entries += entry.key -> value
      i += 1
    }
    cypher.Expr.Map(entries.result())
  }

  private[this] def readCypherLocalDateTime(
    localDateTime: persistence.CypherLocalDateTime
  ): cypher.Expr.LocalDateTime = {
    val javaLocalDateTime = readLocalDateTime(localDateTime.localDateTime)
    cypher.Expr.LocalDateTime(javaLocalDateTime)
  }

  private[this] def readCypherDateTime(dateTime: persistence.CypherDateTime): cypher.Expr.DateTime = {
    val zonedDateTime = readZonedDateTime(dateTime.zonedDateTime)
    cypher.Expr.DateTime(zonedDateTime)
  }

  private[this] def readCypherDuration(duration: persistence.CypherDuration): cypher.Expr.Duration = {
    val javaDuration = readDuration(duration.duration)
    cypher.Expr.Duration(javaDuration)
  }

  private[this] def readCypherDate(date: persistence.CypherDate): cypher.Expr.Date = {
    val javaDate = readLocalDate(date.date)
    cypher.Expr.Date(javaDate)
  }

  private[this] def readCypherLocalTime(time: persistence.CypherLocalTime): cypher.Expr.LocalTime = {
    val javaTime = readLocalTime(time.time)
    cypher.Expr.LocalTime(javaTime)
  }

  private[this] def readCypherTime(time: persistence.CypherTime): cypher.Expr.Time = {
    val javaTime = readOffsetTime(time.time)
    cypher.Expr.Time(javaTime)
  }

  private[this] def writeCypherRelationship(
    builder: FlatBufferBuilder,
    relationship: cypher.Expr.Relationship
  ): Offset = {
    val startOff: Offset = writeQuineId(builder, relationship.start)
    val nameOff: Offset = builder.createString(relationship.name.name)
    val propertiesOff: Offset = {
      val properties: Map[Symbol, cypher.Value] = relationship.properties
      val propertiesOffs: Array[Offset] = new Array[Offset](properties.size)
      for (((key, value), i) <- properties.zipWithIndex) {
        val keyOff: Offset = builder.createString(key.name)
        val TypeAndOffset(valueType, valueOff) = writeCypherValue(builder, value)
        propertiesOffs(i) = persistence.CypherProperty.createCypherProperty(builder, keyOff, valueType, valueOff)
      }
      persistence.CypherRelationship.createPropertiesVector(builder, propertiesOffs)
    }
    val endOff: Offset = writeQuineId(builder, relationship.end)
    persistence.CypherRelationship.createCypherRelationship(
      builder,
      startOff,
      nameOff,
      propertiesOff,
      endOff
    )
  }

  private[this] def writeCypherList(builder: FlatBufferBuilder, list: cypher.Expr.List): Offset = {
    val elems: Vector[cypher.Value] = list.list
    val elemsTyps: Array[Byte] = new Array[Byte](elems.size)
    val elemsOffs: Array[Offset] = new Array[Offset](elems.size)
    for ((elem, i) <- elems.zipWithIndex) {
      val TypeAndOffset(typ, off) = writeCypherValue(builder, elem)
      elemsTyps(i) = typ
      elemsOffs(i) = off
    }
    val elemsTypsOff: Offset = persistence.CypherList.createElementsTypeVector(builder, elemsTyps)
    val elemsOffsOff: Offset = persistence.CypherList.createElementsVector(builder, elemsOffs)
    persistence.CypherList.createCypherList(builder, elemsTypsOff, elemsOffsOff)
  }

  private[this] def writeCypherMap(builder: FlatBufferBuilder, map: cypher.Expr.Map): Offset = {
    val elems = map.map
    val elemsOffs: Array[Offset] = new Array[Offset](elems.size)
    for (((key, value), i) <- elems.zipWithIndex) {
      val keyOff: Offset = builder.createString(key)
      val TypeAndOffset(valueTyp, valueOff) = writeCypherValue(builder, value)
      elemsOffs(i) = persistence.CypherProperty.createCypherProperty(
        builder,
        keyOff,
        valueTyp,
        valueOff
      )
    }
    val elemsOffsOff: Offset = persistence.CypherMap.createEntriesVector(builder, elemsOffs)
    persistence.CypherMap.createCypherMap(builder, elemsOffsOff)
  }

  private[this] def writeCypherPath(builder: FlatBufferBuilder, path: cypher.Expr.Path): Offset = {
    val headOff: Offset = writeCypherNode(builder, path.head)
    val tailsOff: Offset = {
      val tails = path.tails
      val tailsOffs: Array[Offset] = new Array[Offset](tails.length)
      for (((rel, node), i) <- tails.zipWithIndex) {
        val relOff: Offset = writeCypherRelationship(builder, rel)
        val nodeOff: Offset = writeCypherNode(builder, node)
        tailsOffs(i) = persistence.CypherPathSegment.createCypherPathSegment(
          builder,
          relOff,
          nodeOff
        )
      }
      persistence.CypherPath.createTailsVector(builder, tailsOffs)
    }
    persistence.CypherPath.createCypherPath(builder, headOff, tailsOff)
  }

  private[this] def writeCypherLocalDateTime(
    builder: FlatBufferBuilder,
    localDateTime: cypher.Expr.LocalDateTime
  ): Offset = {
    persistence.CypherLocalDateTime.startCypherLocalDateTime(builder)
    val localDateTimeOff: Offset = writeLocalDateTime(builder, localDateTime.localDateTime)
    persistence.CypherLocalDateTime.addLocalDateTime(builder, localDateTimeOff)
    persistence.CypherLocalDateTime.endCypherLocalDateTime(builder)
  }

  private[this] def writeCypherDateTime(builder: FlatBufferBuilder, dateTime: cypher.Expr.DateTime): Offset = {
    val zonedDateTimeOff: Offset = writeZonedDateTime(builder, dateTime.zonedDateTime)
    persistence.CypherDateTime.createCypherDateTime(builder, zonedDateTimeOff)
  }

  private[this] def writeCypherDate(builder: FlatBufferBuilder, date: cypher.Expr.Date): Offset = {
    persistence.CypherDate.startCypherDate(builder)
    val offset = writeLocalDate(builder, date.date)
    persistence.CypherDate.addDate(builder, offset)
    persistence.CypherDate.endCypherDate(builder)
  }

  private[this] def writeCypherTime(builder: FlatBufferBuilder, time: cypher.Expr.Time): Offset = {
    persistence.CypherTime.startCypherTime(builder)
    val offset = writeOffsetTime(builder, time.time)
    persistence.CypherTime.addTime(builder, offset)
    persistence.CypherTime.endCypherTime(builder)
  }

  private[this] def writeCypherLocalTime(builder: FlatBufferBuilder, time: cypher.Expr.LocalTime): Offset = {
    persistence.CypherTime.startCypherTime(builder)
    val offset = writeLocalTime(builder, time.localTime)
    persistence.CypherTime.addTime(builder, offset)
    persistence.CypherTime.endCypherTime(builder)
  }

  private[this] def writeCypherDuration(builder: FlatBufferBuilder, duration: cypher.Expr.Duration): Offset = {
    persistence.CypherDuration.startCypherDuration(builder)
    val durationOff: Offset = writeDuration(builder, duration.duration)
    persistence.CypherDuration.addDuration(builder, durationOff)
    persistence.CypherDuration.endCypherDuration(builder)
  }

  private[this] def writeCypherVariable(builder: FlatBufferBuilder, variable: cypher.Expr.Variable): Offset = {
    val variableOff: Offset = builder.createString(variable.id.name)
    persistence.CypherVariable.createCypherVariable(builder, variableOff)
  }

  private[this] def writeCypherProperty(
    builder: FlatBufferBuilder,
    property: cypher.Expr.Property
  ): Offset = {
    val TypeAndOffset(exprTyp, exprOff) = writeCypherExpr(builder, property.expr)
    val keyOff = builder.createString(property.key.name)
    persistence.CypherPropertyAccess.createCypherPropertyAccess(builder, exprTyp, exprOff, keyOff)
  }

  private[this] def readCypherProperty(property: persistence.CypherPropertyAccess): cypher.Expr.Property = {
    val expr: cypher.Expr = readCypherExpr(property.exprType, property.expr)
    val key: Symbol = Symbol(property.key)
    cypher.Expr.Property(expr, key)
  }

  private[this] def writeCypherDynamicProperty(
    builder: FlatBufferBuilder,
    property: cypher.Expr.DynamicProperty
  ): Offset = {
    val TypeAndOffset(exprTyp, exprOff) = writeCypherExpr(builder, property.expr)
    val TypeAndOffset(keyExprTyp, keyExprOff) = writeCypherExpr(builder, property.keyExpr)
    persistence.CypherDynamicPropertyAccess.createCypherDynamicPropertyAccess(
      builder,
      exprTyp,
      exprOff,
      keyExprTyp,
      keyExprOff
    )
  }

  private[this] def readCypherDynamicProperty(
    property: persistence.CypherDynamicPropertyAccess
  ): cypher.Expr.DynamicProperty = {
    val expr: cypher.Expr = readCypherExpr(property.exprType, property.expr(_))
    val keyExpr: cypher.Expr = readCypherExpr(property.keyExprType, property.keyExpr(_))
    cypher.Expr.DynamicProperty(expr, keyExpr)
  }

  private[this] def writeCypherListSlice(
    builder: FlatBufferBuilder,
    listSlice: cypher.Expr.ListSlice
  ): Offset = {
    val TypeAndOffset(listTyp, listOff) = writeCypherExpr(builder, listSlice.list)
    val TypeAndOffset(fromTyp, fromOff) = listSlice.from match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(fromExpr) => writeCypherExpr(builder, fromExpr)
    }
    val TypeAndOffset(toTyp, toOff) = listSlice.to match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(toExpr) => writeCypherExpr(builder, toExpr)
    }
    persistence.CypherListSlice.createCypherListSlice(builder, listTyp, listOff, fromTyp, fromOff, toTyp, toOff)
  }

  private[this] def readCypherListSlice(listSlice: persistence.CypherListSlice): cypher.Expr.ListSlice = {
    val list: cypher.Expr = readCypherExpr(listSlice.listType, listSlice.list)
    val from: Option[cypher.Expr] =
      if (listSlice.fromType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(listSlice.fromType, listSlice.from))
    val to: Option[cypher.Expr] =
      if (listSlice.toType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(listSlice.toType, listSlice.to))
    cypher.Expr.ListSlice(list, from, to)
  }

  private[this] def writeCypherParameter(builder: FlatBufferBuilder, parameter: cypher.Expr.Parameter): Offset =
    persistence.CypherParameter.createCypherParameter(builder, parameter.name)

  private[this] def writeCypherMapLiteral(builder: FlatBufferBuilder, mapLiteral: cypher.Expr.MapLiteral): Offset = {
    val map: Map[String, cypher.Expr] = mapLiteral.entries
    val entriesOffs: Array[Offset] = new Array[Offset](map.size)
    for (((key, valueExpr), i) <- map.zipWithIndex) {
      val keyOff = builder.createString(key)
      val TypeAndOffset(valueTyp, valueOff) = writeCypherExpr(builder, valueExpr)
      entriesOffs(i) = persistence.CypherMapExprEntry.createCypherMapExprEntry(builder, keyOff, valueTyp, valueOff)
    }
    val entriesOff: Offset = persistence.CypherMapLiteral.createArgumentsVector(builder, entriesOffs)
    persistence.CypherMapLiteral.createCypherMapLiteral(builder, entriesOff)
  }

  private[this] def readCypherMapLiteral(mapLiteral: persistence.CypherMapLiteral): cypher.Expr.MapLiteral = {
    val entries = Map.newBuilder[String, cypher.Expr]
    var i = 0
    val entriesLength = mapLiteral.argumentsLength
    while (i < entriesLength) {
      val mapEntry: persistence.CypherMapExprEntry = mapLiteral.arguments(i)
      val value = readCypherExpr(mapEntry.valueType, mapEntry.value)
      entries += mapEntry.key -> value
      i += 1
    }
    cypher.Expr.MapLiteral(entries.result())
  }

  private[this] def writeCypherMapProjection(builder: FlatBufferBuilder, mapProj: cypher.Expr.MapProjection): Offset = {
    val TypeAndOffset(originalTyp, originalOff) = writeCypherExpr(builder, mapProj.original)
    val items: Seq[(String, cypher.Expr)] = mapProj.items
    val entriesOffs: Array[Offset] = new Array[Offset](items.size)
    for (((key, valueExpr), i) <- items.zipWithIndex) {
      val keyOff = builder.createString(key)
      val TypeAndOffset(valueTyp, valueOff) = writeCypherExpr(builder, valueExpr)
      entriesOffs(i) = persistence.CypherMapExprEntry.createCypherMapExprEntry(builder, keyOff, valueTyp, valueOff)
    }
    val itemsOff: Offset = persistence.CypherMapProjection.createItemsVector(builder, entriesOffs)
    persistence.CypherMapProjection.createCypherMapProjection(
      builder,
      originalTyp,
      originalOff,
      itemsOff,
      mapProj.includeAllProps
    )
  }

  private[this] def readCypherMapProjection(
    mapProjection: persistence.CypherMapProjection
  ): cypher.Expr.MapProjection = {
    val original: cypher.Expr = readCypherExpr(mapProjection.originalType, mapProjection.original)
    val items: Seq[(String, cypher.Expr)] = Seq.tabulate(mapProjection.itemsLength) { i =>
      val mapEntry: persistence.CypherMapExprEntry = mapProjection.items(i)
      val value = readCypherExpr(mapEntry.valueType, mapEntry.value)
      mapEntry.key -> value
    }
    cypher.Expr.MapProjection(original, items, mapProjection.includeAllProps)
  }

  private[this] def writeCypherUnaryOp(
    builder: FlatBufferBuilder,
    unaryOperator: Byte,
    rhs: cypher.Expr
  ): Offset = {
    val TypeAndOffset(rhsTyp, rhsOff) = writeCypherExpr(builder, rhs)
    persistence.CypherUnaryOp.createCypherUnaryOp(
      builder,
      unaryOperator,
      rhsTyp,
      rhsOff
    )
  }

  private[this] def readCypherUnaryOp(unaryOp: persistence.CypherUnaryOp): cypher.Expr = {
    val rhs: cypher.Expr = readCypherExpr(unaryOp.rhsType, unaryOp.rhs)
    unaryOp.operation match {
      case persistence.CypherUnaryOperator.Add => cypher.Expr.UnaryAdd(rhs)
      case persistence.CypherUnaryOperator.Negate => cypher.Expr.UnarySubtract(rhs)
      case persistence.CypherUnaryOperator.Not => cypher.Expr.Not(rhs)
      case persistence.CypherUnaryOperator.IsNull => cypher.Expr.IsNull(rhs)
      case persistence.CypherUnaryOperator.IsNotNull => cypher.Expr.IsNotNull(rhs)
      case persistence.CypherUnaryOperator.RelationshipStart => cypher.Expr.RelationshipStart(rhs)
      case persistence.CypherUnaryOperator.RelationshipEnd => cypher.Expr.RelationshipEnd(rhs)
      case other => throw new InvalidUnionType(other, persistence.CypherUnaryOperator.names)
    }
  }

  private[this] def writeCypherBinaryOp(
    builder: FlatBufferBuilder,
    binaryOperator: Byte,
    lhs: cypher.Expr,
    rhs: cypher.Expr
  ): Offset = {
    val TypeAndOffset(lhsTyp, lhsOff) = writeCypherExpr(builder, lhs)
    val TypeAndOffset(rhsTyp, rhsOff) = writeCypherExpr(builder, rhs)
    persistence.CypherBinaryOp.createCypherBinaryOp(
      builder,
      binaryOperator,
      lhsTyp,
      lhsOff,
      rhsTyp,
      rhsOff
    )
  }

  private[this] def readCypherBinaryOp(binaryOp: persistence.CypherBinaryOp): cypher.Expr = {
    val lhs: cypher.Expr = readCypherExpr(binaryOp.lhsType, binaryOp.lhs)
    val rhs: cypher.Expr = readCypherExpr(binaryOp.rhsType, binaryOp.rhs)
    binaryOp.operation match {
      case persistence.CypherBinaryOperator.Add => cypher.Expr.Add(lhs, rhs)
      case persistence.CypherBinaryOperator.Subtract => cypher.Expr.Subtract(lhs, rhs)
      case persistence.CypherBinaryOperator.Multiply => cypher.Expr.Multiply(lhs, rhs)
      case persistence.CypherBinaryOperator.Divide => cypher.Expr.Divide(lhs, rhs)
      case persistence.CypherBinaryOperator.Modulo => cypher.Expr.Modulo(lhs, rhs)
      case persistence.CypherBinaryOperator.Exponentiate => cypher.Expr.Exponentiate(lhs, rhs)
      case persistence.CypherBinaryOperator.Equal => cypher.Expr.Equal(lhs, rhs)
      case persistence.CypherBinaryOperator.GreaterEqual => cypher.Expr.GreaterEqual(lhs, rhs)
      case persistence.CypherBinaryOperator.LessEqual => cypher.Expr.LessEqual(lhs, rhs)
      case persistence.CypherBinaryOperator.Greater => cypher.Expr.Greater(lhs, rhs)
      case persistence.CypherBinaryOperator.Less => cypher.Expr.Less(lhs, rhs)
      case persistence.CypherBinaryOperator.InList => cypher.Expr.InList(lhs, rhs)
      case persistence.CypherBinaryOperator.StartsWith => cypher.Expr.StartsWith(lhs, rhs)
      case persistence.CypherBinaryOperator.EndsWith => cypher.Expr.EndsWith(lhs, rhs)
      case persistence.CypherBinaryOperator.Contains => cypher.Expr.Contains(lhs, rhs)
      case persistence.CypherBinaryOperator.Regex => cypher.Expr.Regex(lhs, rhs)
      case other => throw new InvalidUnionType(other, persistence.CypherBinaryOperator.names)
    }
  }

  private[this] def writeCypherNaryOp(
    builder: FlatBufferBuilder,
    naryOperator: Byte,
    args: Vector[cypher.Expr]
  ): Offset = {
    val argTypesOffs: Array[Byte] = new Array[Byte](args.length)
    val argOffs: Array[Offset] = new Array[Offset](args.length)
    for ((expr, i) <- args.zipWithIndex) {
      val TypeAndOffset(exprTyp, exprOff) = writeCypherExpr(builder, expr)
      argTypesOffs(i) = exprTyp
      argOffs(i) = exprOff
    }
    val argTypesOff = persistence.CypherNaryOp.createArgumentsTypeVector(builder, argTypesOffs)
    val argsOff = persistence.CypherNaryOp.createArgumentsVector(builder, argOffs)
    persistence.CypherNaryOp.createCypherNaryOp(
      builder,
      naryOperator,
      argTypesOff,
      argsOff
    )
  }

  private[this] def readCypherNaryOp(naryOp: persistence.CypherNaryOp): cypher.Expr = {
    val arguments: Vector[cypher.Expr] = Vector.tabulate(naryOp.argumentsLength) { i =>
      readCypherExpr(naryOp.argumentsType(i), naryOp.arguments(_, i))
    }
    naryOp.operation match {
      case persistence.CypherNaryOperator.And => cypher.Expr.And(arguments)
      case persistence.CypherNaryOperator.Or => cypher.Expr.Or(arguments)
      case persistence.CypherNaryOperator.ListLiteral => cypher.Expr.ListLiteral(arguments)
      case persistence.CypherNaryOperator.PathExpression => cypher.Expr.PathExpression(arguments)
      case other => throw new InvalidUnionType(other, persistence.CypherNaryOperator.names)
    }
  }

  private[this] def writeCypherCase(builder: FlatBufferBuilder, caseExp: cypher.Expr.Case): Offset = {
    val TypeAndOffset(scrutineeTyp, scrutineeOff) = caseExp.scrutinee match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(scrut) => writeCypherExpr(builder, scrut)
    }
    val branchesOff: Offset = {
      val branches: Vector[(cypher.Expr, cypher.Expr)] = caseExp.branches
      val branchesOffs: Array[Offset] = new Array[Offset](branches.size)
      for (((cond, outcome), i) <- branches.zipWithIndex) {
        val TypeAndOffset(condTyp, condOff) = writeCypherExpr(builder, cond)
        val TypeAndOffset(outcomeTyp, outcomeOff) = writeCypherExpr(builder, outcome)
        branchesOffs(i) = persistence.CypherCaseBranch.createCypherCaseBranch(
          builder,
          condTyp,
          condOff,
          outcomeTyp,
          outcomeOff
        )
      }
      persistence.CypherCase.createBranchesVector(builder, branchesOffs)
    }
    val TypeAndOffset(fallThroughTyp, fallThroughOff) = caseExp.default match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(ft) => writeCypherExpr(builder, ft)
    }
    persistence.CypherCase.createCypherCase(
      builder,
      scrutineeTyp,
      scrutineeOff,
      branchesOff,
      fallThroughTyp,
      fallThroughOff
    )
  }

  private[this] def readCypherCase(caseExp: persistence.CypherCase): cypher.Expr.Case = {
    val scrutinee: Option[cypher.Expr] =
      if (caseExp.scrutineeType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(caseExp.scrutineeType, caseExp.scrutinee))
    val branches: Vector[(cypher.Expr, cypher.Expr)] = Vector.tabulate(caseExp.branchesLength) { i =>
      val branch: persistence.CypherCaseBranch = caseExp.branches(i)
      val cond: cypher.Expr = readCypherExpr(branch.conditionType, branch.condition)
      val outcome: cypher.Expr = readCypherExpr(branch.outcomeType, branch.outcome)
      cond -> outcome
    }
    val default: Option[cypher.Expr] =
      if (caseExp.fallThroughType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(caseExp.fallThroughType, caseExp.fallThrough))
    cypher.Expr.Case(scrutinee, branches, default)
  }

  private[this] def writeCypherFunction(builder: FlatBufferBuilder, func: cypher.Expr.Function): Offset = {
    val nameOff: Offset = builder.createString(func.function.name)
    val argumentTyps: Array[Byte] = new Array(func.arguments.length)
    val argumentOffs: Array[Offset] = new Array(func.arguments.length)
    for ((argument, i) <- func.arguments.zipWithIndex) {
      val TypeAndOffset(argumentTyp, argumentOff) = writeCypherExpr(builder, argument)
      argumentTyps(i) = argumentTyp
      argumentOffs(i) = argumentOff
    }
    val argumentTypsOff: Offset = persistence.CypherFunction.createArgumentsTypeVector(builder, argumentTyps)
    val argumentsOff: Offset = persistence.CypherFunction.createArgumentsVector(builder, argumentOffs)
    persistence.CypherFunction.createCypherFunction(
      builder,
      nameOff,
      argumentTypsOff,
      argumentsOff
    )
  }

  private[this] val builtinFuncs: Map[String, cypher.BuiltinFunc] =
    cypher.Func.builtinFunctions.map(f => f.name -> f).toMap

  private[this] def readCypherFunction(func: persistence.CypherFunction): cypher.Expr.Function = {
    val name: String = func.function
    val arguments: Vector[cypher.Expr] = Vector.tabulate(func.argumentsLength) { i =>
      readCypherExpr(func.argumentsType(i), func.arguments(_, i))
    }
    cypher.Expr.Function(builtinFuncs.getOrElse(name, cypher.Func.UserDefined(name)), arguments)
  }

  private[this] def writeCypherListComprehension(
    builder: FlatBufferBuilder,
    comp: cypher.Expr.ListComprehension
  ): Offset = {
    val variableOff: Offset = builder.createString(comp.variable.name)
    val TypeAndOffset(listTyp, listOff) = writeCypherExpr(builder, comp.list)
    val TypeAndOffset(predTyp, predOff) = writeCypherExpr(builder, comp.filterPredicate)
    val TypeAndOffset(extractTyp, extractOff) = writeCypherExpr(builder, comp.extract)
    persistence.CypherListComprehension.createCypherListComprehension(
      builder,
      variableOff,
      listTyp,
      listOff,
      predTyp,
      predOff,
      extractTyp,
      extractOff
    )
  }

  private[this] def readCypherListComprehension(
    comp: persistence.CypherListComprehension
  ): cypher.Expr.ListComprehension = {
    val variable: Symbol = Symbol(comp.variable)
    val list: cypher.Expr = readCypherExpr(comp.listType, comp.list(_))
    val predicate: cypher.Expr = readCypherExpr(comp.filterPredicateType, comp.filterPredicate(_))
    val extract: cypher.Expr = readCypherExpr(comp.extractType, comp.extract(_))
    cypher.Expr.ListComprehension(variable, list, predicate, extract)
  }

  private[this] def writeCypherListFold(
    builder: FlatBufferBuilder,
    listFoldOperator: Byte,
    variable: Symbol,
    list: cypher.Expr,
    pred: cypher.Expr
  ): Offset = {
    val variableOff: Offset = builder.createString(variable.name)
    val TypeAndOffset(listTyp, listOff) = writeCypherExpr(builder, list)
    val TypeAndOffset(predTyp, predOff) = writeCypherExpr(builder, pred)
    persistence.CypherListFold.createCypherListFold(
      builder,
      listFoldOperator,
      variableOff,
      listTyp,
      listOff,
      predTyp,
      predOff
    )
  }

  private[this] def readCypherListFold(
    comp: persistence.CypherListFold
  ): cypher.Expr = {
    val variable: Symbol = Symbol(comp.variable)
    val list: cypher.Expr = readCypherExpr(comp.listType, comp.list(_))
    val predicate: cypher.Expr = readCypherExpr(comp.filterPredicateType, comp.filterPredicate(_))
    comp.operator match {
      case persistence.CypherListFoldOperator.All =>
        cypher.Expr.AllInList(variable, list, predicate)

      case persistence.CypherListFoldOperator.Any =>
        cypher.Expr.AnyInList(variable, list, predicate)

      case persistence.CypherListFoldOperator.Single =>
        cypher.Expr.SingleInList(variable, list, predicate)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherListFoldOperator.names)
    }
  }

  private[this] def writeCypherReduceList(builder: FlatBufferBuilder, reduce: cypher.Expr.ReduceList): Offset = {
    val accumulatorOff: Offset = builder.createString(reduce.accumulator.name)
    val TypeAndOffset(initialTyp, initialOff) = writeCypherExpr(builder, reduce.initial)
    val variableOff: Offset = builder.createString(reduce.variable.name)
    val TypeAndOffset(listTyp, listOff) = writeCypherExpr(builder, reduce.list)
    val TypeAndOffset(reducerTyp, reducerOff) = writeCypherExpr(builder, reduce.reducer)
    persistence.CypherReduceList.createCypherReduceList(
      builder,
      accumulatorOff,
      initialTyp,
      initialOff,
      variableOff,
      listTyp,
      listOff,
      reducerTyp,
      reducerOff
    )
  }

  private[this] def readCypherReduceList(
    reduce: persistence.CypherReduceList
  ): cypher.Expr.ReduceList = {
    val accumulator: Symbol = Symbol(reduce.accumulator)
    val initial: cypher.Expr = readCypherExpr(reduce.initialType, reduce.initial(_))
    val variable: Symbol = Symbol(reduce.variable)
    val list: cypher.Expr = readCypherExpr(reduce.listType, reduce.list(_))
    val reducer: cypher.Expr = readCypherExpr(reduce.reducerType, reduce.reducer(_))
    cypher.Expr.ReduceList(accumulator, initial, variable, list, reducer)
  }

  protected[this] def writeCypherValue(builder: FlatBufferBuilder, expr: cypher.Value): TypeAndOffset =
    expr match {
      case str: cypher.Expr.Str =>
        TypeAndOffset(persistence.CypherValue.CypherStr, writeCypherStr(builder, str))

      case integer: cypher.Expr.Integer =>
        TypeAndOffset(persistence.CypherValue.CypherInteger, writeCypherInteger(builder, integer))

      case floating: cypher.Expr.Floating =>
        TypeAndOffset(persistence.CypherValue.CypherFloating, writeCypherFloating(builder, floating))

      case cypher.Expr.True =>
        TypeAndOffset(persistence.CypherValue.CypherTrue, emptyTable(builder))

      case cypher.Expr.False =>
        TypeAndOffset(persistence.CypherValue.CypherFalse, emptyTable(builder))

      case cypher.Expr.Null =>
        TypeAndOffset(persistence.CypherValue.CypherNull, emptyTable(builder))

      case bytes: cypher.Expr.Bytes =>
        TypeAndOffset(persistence.CypherValue.CypherBytes, writeCypherBytes(builder, bytes))

      case node: cypher.Expr.Node =>
        TypeAndOffset(persistence.CypherValue.CypherNode, writeCypherNode(builder, node))

      case path: cypher.Expr.Path =>
        TypeAndOffset(persistence.CypherValue.CypherPath, writeCypherPath(builder, path))

      case relationship: cypher.Expr.Relationship =>
        TypeAndOffset(persistence.CypherValue.CypherRelationship, writeCypherRelationship(builder, relationship))

      case list: cypher.Expr.List =>
        TypeAndOffset(persistence.CypherValue.CypherList, writeCypherList(builder, list))

      case map: cypher.Expr.Map =>
        TypeAndOffset(persistence.CypherValue.CypherMap, writeCypherMap(builder, map))

      case localDateTime: cypher.Expr.LocalDateTime =>
        TypeAndOffset(persistence.CypherValue.CypherLocalDateTime, writeCypherLocalDateTime(builder, localDateTime))

      case dateTime: cypher.Expr.DateTime =>
        TypeAndOffset(persistence.CypherValue.CypherDateTime, writeCypherDateTime(builder, dateTime))

      case duration: cypher.Expr.Duration =>
        TypeAndOffset(persistence.CypherValue.CypherDuration, writeCypherDuration(builder, duration))

      case date: cypher.Expr.Date =>
        TypeAndOffset(persistence.CypherValue.CypherDate, writeCypherDate(builder, date))

      case time: cypher.Expr.Time =>
        TypeAndOffset(persistence.CypherValue.CypherTime, writeCypherTime(builder, time))

      case localTime: cypher.Expr.LocalTime =>
        TypeAndOffset(persistence.CypherValue.CypherLocalTime, writeCypherLocalTime(builder, localTime))
    }

  protected[this] def readCypherValue(typ: Byte, makeExpr: Table => Table): cypher.Value =
    typ match {
      case persistence.CypherValue.CypherStr =>
        val str = makeExpr(new persistence.CypherStr()).asInstanceOf[persistence.CypherStr]
        cypher.Expr.Str(str.text)

      case persistence.CypherValue.CypherInteger =>
        val integer = makeExpr(new persistence.CypherInteger()).asInstanceOf[persistence.CypherInteger]
        cypher.Expr.Integer(integer.integer)

      case persistence.CypherValue.CypherFloating =>
        val floating = makeExpr(new persistence.CypherFloating()).asInstanceOf[persistence.CypherFloating]
        cypher.Expr.Floating(floating.floating)

      case persistence.CypherValue.CypherTrue =>
        cypher.Expr.True

      case persistence.CypherValue.CypherFalse =>
        cypher.Expr.False

      case persistence.CypherValue.CypherNull =>
        cypher.Expr.Null

      case persistence.CypherValue.CypherBytes =>
        val bytes = makeExpr(new persistence.CypherBytes()).asInstanceOf[persistence.CypherBytes]
        cypher.Expr.Bytes(bytes.bytesAsByteBuffer.remainingBytes, bytes.representsId)

      case persistence.CypherValue.CypherNode =>
        val node = makeExpr(new persistence.CypherNode()).asInstanceOf[persistence.CypherNode]
        readCypherNode(node)

      case persistence.CypherValue.CypherPath =>
        val path = makeExpr(new persistence.CypherPath()).asInstanceOf[persistence.CypherPath]
        readCypherPath(path)

      case persistence.CypherValue.CypherRelationship =>
        val relationship = makeExpr(new persistence.CypherRelationship()).asInstanceOf[persistence.CypherRelationship]
        readCypherRelationship(relationship)

      case persistence.CypherValue.CypherList =>
        val list = makeExpr(new persistence.CypherList()).asInstanceOf[persistence.CypherList]
        readCypherList(list)

      case persistence.CypherValue.CypherMap =>
        val map = makeExpr(new persistence.CypherMap()).asInstanceOf[persistence.CypherMap]
        readCypherMap(map)

      case persistence.CypherValue.CypherLocalDateTime =>
        val localDateTime =
          makeExpr(new persistence.CypherLocalDateTime()).asInstanceOf[persistence.CypherLocalDateTime]
        readCypherLocalDateTime(localDateTime)

      case persistence.CypherValue.CypherDateTime =>
        val dateTime = makeExpr(new persistence.CypherDateTime()).asInstanceOf[persistence.CypherDateTime]
        readCypherDateTime(dateTime)

      case persistence.CypherValue.CypherLocalTime =>
        val time = makeExpr(new persistence.CypherLocalTime()).asInstanceOf[persistence.CypherLocalTime]
        readCypherLocalTime(time)

      case persistence.CypherValue.CypherDuration =>
        val duration = makeExpr(new persistence.CypherDuration()).asInstanceOf[persistence.CypherDuration]
        readCypherDuration(duration)

      case persistence.CypherValue.CypherDate =>
        val date = makeExpr(new persistence.CypherDate()).asInstanceOf[persistence.CypherDate]
        readCypherDate(date)

      case persistence.CypherValue.CypherTime =>
        val time = makeExpr(new persistence.CypherTime()).asInstanceOf[persistence.CypherTime]
        readCypherTime(time)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherValue.names)
    }

  protected[this] def writeCypherExpr(builder: FlatBufferBuilder, expr: cypher.Expr): TypeAndOffset =
    expr match {
      case str: cypher.Expr.Str =>
        TypeAndOffset(persistence.CypherExpr.CypherStr, writeCypherStr(builder, str))

      case integer: cypher.Expr.Integer =>
        TypeAndOffset(persistence.CypherExpr.CypherInteger, writeCypherInteger(builder, integer))

      case floating: cypher.Expr.Floating =>
        TypeAndOffset(persistence.CypherExpr.CypherFloating, writeCypherFloating(builder, floating))

      case cypher.Expr.True =>
        TypeAndOffset(persistence.CypherExpr.CypherTrue, emptyTable(builder))

      case cypher.Expr.False =>
        TypeAndOffset(persistence.CypherExpr.CypherFalse, emptyTable(builder))

      case cypher.Expr.Null =>
        TypeAndOffset(persistence.CypherExpr.CypherNull, emptyTable(builder))

      case bytes: cypher.Expr.Bytes =>
        TypeAndOffset(persistence.CypherExpr.CypherBytes, writeCypherBytes(builder, bytes))

      case node: cypher.Expr.Node =>
        TypeAndOffset(persistence.CypherExpr.CypherNode, writeCypherNode(builder, node))

      case path: cypher.Expr.Path =>
        TypeAndOffset(persistence.CypherExpr.CypherPath, writeCypherPath(builder, path))

      case relationship: cypher.Expr.Relationship =>
        TypeAndOffset(persistence.CypherExpr.CypherRelationship, writeCypherRelationship(builder, relationship))

      case list: cypher.Expr.List =>
        TypeAndOffset(persistence.CypherExpr.CypherList, writeCypherList(builder, list))

      case map: cypher.Expr.Map =>
        TypeAndOffset(persistence.CypherExpr.CypherMap, writeCypherMap(builder, map))

      case localDateTime: cypher.Expr.LocalDateTime =>
        TypeAndOffset(persistence.CypherExpr.CypherLocalDateTime, writeCypherLocalDateTime(builder, localDateTime))

      case dateTime: cypher.Expr.DateTime =>
        TypeAndOffset(persistence.CypherExpr.CypherDateTime, writeCypherDateTime(builder, dateTime))

      case duration: cypher.Expr.Duration =>
        TypeAndOffset(persistence.CypherExpr.CypherDuration, writeCypherDuration(builder, duration))

      case date: cypher.Expr.Date =>
        TypeAndOffset(persistence.CypherExpr.CypherDate, writeCypherDate(builder, date))

      case time: cypher.Expr.Time =>
        TypeAndOffset(persistence.CypherExpr.CypherTime, writeCypherTime(builder, time))

      case time: cypher.Expr.LocalTime =>
        TypeAndOffset(persistence.CypherExpr.CypherLocalTime, writeCypherLocalTime(builder, time))

      case variable: cypher.Expr.Variable =>
        TypeAndOffset(persistence.CypherExpr.CypherVariable, writeCypherVariable(builder, variable))

      case property: cypher.Expr.Property =>
        TypeAndOffset(persistence.CypherExpr.CypherPropertyAccess, writeCypherProperty(builder, property))

      case property: cypher.Expr.DynamicProperty =>
        TypeAndOffset(persistence.CypherExpr.CypherDynamicPropertyAccess, writeCypherDynamicProperty(builder, property))

      case slice: cypher.Expr.ListSlice =>
        TypeAndOffset(persistence.CypherExpr.CypherListSlice, writeCypherListSlice(builder, slice))

      case param: cypher.Expr.Parameter =>
        TypeAndOffset(persistence.CypherExpr.CypherParameter, writeCypherParameter(builder, param))

      case map: cypher.Expr.MapLiteral =>
        TypeAndOffset(persistence.CypherExpr.CypherMapLiteral, writeCypherMapLiteral(builder, map))

      case projection: cypher.Expr.MapProjection =>
        TypeAndOffset(persistence.CypherExpr.CypherMapProjection, writeCypherMapProjection(builder, projection))

      case cypher.Expr.UnaryAdd(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.Add, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.UnarySubtract(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.Negate, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.Not(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.Not, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.IsNull(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.IsNull, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.IsNotNull(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.IsNotNull, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.RelationshipStart(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.RelationshipStart, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.RelationshipEnd(arg) =>
        val off = writeCypherUnaryOp(builder, persistence.CypherUnaryOperator.RelationshipEnd, arg)
        TypeAndOffset(persistence.CypherExpr.CypherUnaryOp, off)

      case cypher.Expr.Add(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Add, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Subtract(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Subtract, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Multiply(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Multiply, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Divide(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Divide, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Modulo(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Modulo, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Exponentiate(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Exponentiate, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Equal(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Equal, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.GreaterEqual(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.GreaterEqual, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.LessEqual(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.LessEqual, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Greater(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Greater, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Less(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Less, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.InList(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.InList, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.StartsWith(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.StartsWith, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.EndsWith(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.EndsWith, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Contains(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Contains, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.Regex(lhs, rhs) =>
        val off = writeCypherBinaryOp(builder, persistence.CypherBinaryOperator.Regex, lhs, rhs)
        TypeAndOffset(persistence.CypherExpr.CypherBinaryOp, off)

      case cypher.Expr.ListLiteral(args) =>
        val off = writeCypherNaryOp(builder, persistence.CypherNaryOperator.ListLiteral, args)
        TypeAndOffset(persistence.CypherExpr.CypherNaryOp, off)

      case cypher.Expr.PathExpression(args) =>
        val off = writeCypherNaryOp(builder, persistence.CypherNaryOperator.PathExpression, args)
        TypeAndOffset(persistence.CypherExpr.CypherNaryOp, off)

      case cypher.Expr.And(args) =>
        val off = writeCypherNaryOp(builder, persistence.CypherNaryOperator.And, args)
        TypeAndOffset(persistence.CypherExpr.CypherNaryOp, off)

      case cypher.Expr.Or(args) =>
        val off = writeCypherNaryOp(builder, persistence.CypherNaryOperator.Or, args)
        TypeAndOffset(persistence.CypherExpr.CypherNaryOp, off)

      case caseExp: cypher.Expr.Case =>
        TypeAndOffset(persistence.CypherExpr.CypherCase, writeCypherCase(builder, caseExp))

      case func: cypher.Expr.Function =>
        TypeAndOffset(persistence.CypherExpr.CypherFunction, writeCypherFunction(builder, func))

      case comp: cypher.Expr.ListComprehension =>
        TypeAndOffset(persistence.CypherExpr.CypherListComprehension, writeCypherListComprehension(builder, comp))

      case cypher.Expr.AllInList(variable, list, pred) =>
        val off = writeCypherListFold(builder, persistence.CypherListFoldOperator.All, variable, list, pred)
        TypeAndOffset(persistence.CypherExpr.CypherListFold, off)

      case cypher.Expr.AnyInList(variable, list, pred) =>
        val off = writeCypherListFold(builder, persistence.CypherListFoldOperator.Any, variable, list, pred)
        TypeAndOffset(persistence.CypherExpr.CypherListFold, off)

      case cypher.Expr.SingleInList(variable, list, pred) =>
        val off = writeCypherListFold(builder, persistence.CypherListFoldOperator.Single, variable, list, pred)
        TypeAndOffset(persistence.CypherExpr.CypherListFold, off)

      case reduce: cypher.Expr.ReduceList =>
        TypeAndOffset(persistence.CypherExpr.CypherReduceList, writeCypherReduceList(builder, reduce))

      case cypher.Expr.FreshNodeId =>
        TypeAndOffset(persistence.CypherExpr.CypherFreshNodeId, emptyTable(builder))
    }

  protected[this] def readCypherExpr(typ: Byte, makeExpr: Table => Table): cypher.Expr = {
    // rawMakeExpr
    // In Scala 3 we could type `makeExpr` as [A <: Table] => A => A
    // to avoid the cast
    typ match {
      case persistence.CypherExpr.CypherStr =>
        val str = makeExpr(new persistence.CypherStr()).asInstanceOf[persistence.CypherStr]
        cypher.Expr.Str(str.text)

      case persistence.CypherExpr.CypherInteger =>
        val integer = makeExpr(new persistence.CypherInteger()).asInstanceOf[persistence.CypherInteger]
        cypher.Expr.Integer(integer.integer)

      case persistence.CypherExpr.CypherFloating =>
        val floating = makeExpr(new persistence.CypherFloating()).asInstanceOf[persistence.CypherFloating]
        cypher.Expr.Floating(floating.floating)

      case persistence.CypherExpr.CypherTrue =>
        cypher.Expr.True

      case persistence.CypherExpr.CypherFalse =>
        cypher.Expr.False

      case persistence.CypherExpr.CypherNull =>
        cypher.Expr.Null

      case persistence.CypherExpr.CypherBytes =>
        val bytes = makeExpr(new persistence.CypherBytes()).asInstanceOf[persistence.CypherBytes]
        cypher.Expr.Bytes(bytes.bytesAsByteBuffer.remainingBytes, bytes.representsId)

      case persistence.CypherExpr.CypherNode =>
        val node = makeExpr(new persistence.CypherNode()).asInstanceOf[persistence.CypherNode]
        readCypherNode(node)

      case persistence.CypherExpr.CypherPath =>
        val path = makeExpr(new persistence.CypherPath()).asInstanceOf[persistence.CypherPath]
        readCypherPath(path)

      case persistence.CypherExpr.CypherRelationship =>
        val relationship = makeExpr(new persistence.CypherRelationship()).asInstanceOf[persistence.CypherRelationship]
        readCypherRelationship(relationship)

      case persistence.CypherExpr.CypherList =>
        val list = makeExpr(new persistence.CypherList()).asInstanceOf[persistence.CypherList]
        readCypherList(list)

      case persistence.CypherExpr.CypherMap =>
        val map = makeExpr(new persistence.CypherMap()).asInstanceOf[persistence.CypherMap]
        readCypherMap(map)

      case persistence.CypherExpr.CypherLocalDateTime =>
        val localDateTime =
          makeExpr(new persistence.CypherLocalDateTime()).asInstanceOf[persistence.CypherLocalDateTime]
        readCypherLocalDateTime(localDateTime)

      case persistence.CypherExpr.CypherDateTime =>
        val dateTime = makeExpr(new persistence.CypherDateTime()).asInstanceOf[persistence.CypherDateTime]
        readCypherDateTime(dateTime)

      case persistence.CypherExpr.CypherDuration =>
        val duration = makeExpr(new persistence.CypherDuration()).asInstanceOf[persistence.CypherDuration]
        readCypherDuration(duration)

      case persistence.CypherExpr.CypherDate =>
        val date = makeExpr(new persistence.CypherDate).asInstanceOf[persistence.CypherDate]
        readCypherDate(date)
      case persistence.CypherExpr.CypherLocalTime =>
        val time = makeExpr(new persistence.CypherLocalTime).asInstanceOf[persistence.CypherLocalTime]
        readCypherLocalTime(time)

      case persistence.CypherExpr.CypherVariable =>
        val variable = makeExpr(new persistence.CypherVariable()).asInstanceOf[persistence.CypherVariable]
        cypher.Expr.Variable(Symbol(variable.id))

      case persistence.CypherExpr.CypherPropertyAccess =>
        val propertyAccess =
          makeExpr(new persistence.CypherPropertyAccess()).asInstanceOf[persistence.CypherPropertyAccess]
        readCypherProperty(propertyAccess)

      case persistence.CypherExpr.CypherDynamicPropertyAccess =>
        val propertyAccess =
          makeExpr(new persistence.CypherDynamicPropertyAccess()).asInstanceOf[persistence.CypherDynamicPropertyAccess]
        readCypherDynamicProperty(propertyAccess)

      case persistence.CypherExpr.CypherListSlice =>
        val slice = makeExpr(new persistence.CypherListSlice()).asInstanceOf[persistence.CypherListSlice]
        readCypherListSlice(slice)

      case persistence.CypherExpr.CypherParameter =>
        val parameter = makeExpr(new persistence.CypherParameter()).asInstanceOf[persistence.CypherParameter]
        cypher.Expr.Parameter(parameter.index)

      case persistence.CypherExpr.CypherMapLiteral =>
        val mapLiteral = makeExpr(new persistence.CypherMapLiteral()).asInstanceOf[persistence.CypherMapLiteral]
        readCypherMapLiteral(mapLiteral)

      case persistence.CypherExpr.CypherMapProjection =>
        val mapProjection =
          makeExpr(new persistence.CypherMapProjection()).asInstanceOf[persistence.CypherMapProjection]
        readCypherMapProjection(mapProjection)

      case persistence.CypherExpr.CypherUnaryOp =>
        val unaryOp = makeExpr(new persistence.CypherUnaryOp()).asInstanceOf[persistence.CypherUnaryOp]
        readCypherUnaryOp(unaryOp)

      case persistence.CypherExpr.CypherBinaryOp =>
        val binaryOp = makeExpr(new persistence.CypherBinaryOp()).asInstanceOf[persistence.CypherBinaryOp]
        readCypherBinaryOp(binaryOp)

      case persistence.CypherExpr.CypherNaryOp =>
        val naryOp = makeExpr(new persistence.CypherNaryOp()).asInstanceOf[persistence.CypherNaryOp]
        readCypherNaryOp(naryOp)

      case persistence.CypherExpr.CypherCase =>
        val caseExp = makeExpr(new persistence.CypherCase()).asInstanceOf[persistence.CypherCase]
        readCypherCase(caseExp)

      case persistence.CypherExpr.CypherFunction =>
        val func = makeExpr(new persistence.CypherFunction()).asInstanceOf[persistence.CypherFunction]
        readCypherFunction(func)

      case persistence.CypherExpr.CypherListComprehension =>
        val comp = makeExpr(new persistence.CypherListComprehension()).asInstanceOf[persistence.CypherListComprehension]
        readCypherListComprehension(comp)

      case persistence.CypherExpr.CypherListFold =>
        val fold = makeExpr(new persistence.CypherListFold()).asInstanceOf[persistence.CypherListFold]
        readCypherListFold(fold)

      case persistence.CypherExpr.CypherReduceList =>
        val reduce = makeExpr(new persistence.CypherReduceList()).asInstanceOf[persistence.CypherReduceList]
        readCypherReduceList(reduce)

      case persistence.CypherExpr.CypherFreshNodeId =>
        cypher.Expr.FreshNodeId

      case persistence.CypherExpr.CypherTime =>
        val time = makeExpr(new persistence.CypherTime).asInstanceOf[persistence.CypherTime]
        readCypherTime(time)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherExpr.names)
    }
  }

  protected[this] def writeMultipleValuesStandingQueryPartId(
    builder: FlatBufferBuilder,
    sqId: MultipleValuesStandingQueryPartId
  ): Offset =
    persistence.MultipleValuesStandingQueryPartId.createMultipleValuesStandingQueryPartId(
      builder,
      sqId.uuid.getLeastSignificantBits,
      sqId.uuid.getMostSignificantBits
    )

  protected[this] def readMultipleValuesStandingQueryPartId(
    sqId: persistence.MultipleValuesStandingQueryPartId
  ): MultipleValuesStandingQueryPartId =
    MultipleValuesStandingQueryPartId(new UUID(sqId.highBytes, sqId.lowBytes))

  protected[this] def writeStandingQueryId(builder: FlatBufferBuilder, sqId: StandingQueryId): Offset =
    persistence.StandingQueryId.createStandingQueryId(
      builder,
      sqId.uuid.getLeastSignificantBits,
      sqId.uuid.getMostSignificantBits
    )

  protected[this] def readStandingQueryId(sqId: persistence.StandingQueryId): StandingQueryId =
    StandingQueryId(new UUID(sqId.highBytes, sqId.lowBytes))

  private[this] def writeMultipleValuesCrossStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery.Cross
  ): Offset = {
    val queriesTyps: Array[Byte] = new Array[Byte](query.queries.length)
    val queriesOffs: Array[Offset] = new Array[Offset](query.queries.length)
    for ((subQuery, i) <- query.queries.zipWithIndex) {
      val TypeAndOffset(subQueryTyp, subQueryOff) = writeMultipleValuesStandingQuery(builder, subQuery)
      queriesTyps(i) = subQueryTyp
      queriesOffs(i) = subQueryOff
    }
    persistence.MultipleValuesCrossStandingQuery.createMultipleValuesCrossStandingQuery(
      builder,
      persistence.MultipleValuesCrossStandingQuery.createQueriesTypeVector(builder, queriesTyps),
      persistence.MultipleValuesCrossStandingQuery.createQueriesVector(builder, queriesOffs),
      query.emitSubscriptionsLazily
    )
  }

  private[this] def readMultipleValuesCrossStandingQuery(
    query: persistence.MultipleValuesCrossStandingQuery
  ): MultipleValuesStandingQuery.Cross = {
    var i = 0
    val queriesLength = query.queriesLength
    val queries: Array[MultipleValuesStandingQuery] = new Array[MultipleValuesStandingQuery](queriesLength)
    while (i < queriesLength) {
      queries(i) = readMultipleValuesStandingQuery(query.queriesType(i), query.queries(_, i))
      i += 1
    }
    MultipleValuesStandingQuery.Cross(
      ArraySeq.unsafeWrapArray(queries),
      query.emitSubscriptionsLazily
    )
  }

  private[this] def writeCypherValueConstraint(
    builder: FlatBufferBuilder,
    constraint: MultipleValuesStandingQuery.LocalProperty.ValueConstraint
  ): TypeAndOffset =
    constraint match {
      case MultipleValuesStandingQuery.LocalProperty.Equal(value) =>
        val TypeAndOffset(compareToTyp, compareToOff) = writeCypherValue(builder, value)
        val off = persistence.CypherValueConstraintEqual.createCypherValueConstraintEqual(
          builder,
          compareToTyp,
          compareToOff
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintEqual, off)

      case MultipleValuesStandingQuery.LocalProperty.NotEqual(value) =>
        val TypeAndOffset(compareToTyp, compareToOff) = writeCypherValue(builder, value)
        val off = persistence.CypherValueConstraintNotEqual.createCypherValueConstraintNotEqual(
          builder,
          compareToTyp,
          compareToOff
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintNotEqual, off)

      case MultipleValuesStandingQuery.LocalProperty.Any =>
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintAny, emptyTable(builder))

      case MultipleValuesStandingQuery.LocalProperty.None =>
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintNone, emptyTable(builder))

      case MultipleValuesStandingQuery.LocalProperty.Regex(pattern) =>
        val patternOff = builder.createString(pattern)
        val off = persistence.CypherValueConstraintRegex.createCypherValueConstraintRegex(
          builder,
          patternOff
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintRegex, off)

      case MultipleValuesStandingQuery.LocalProperty.ListContains(values) =>
        val valuesTyps: Array[Byte] = new Array[Byte](values.size)
        val valuesOffs: Array[Offset] = new Array[Offset](values.size)
        for ((value, i) <- values.zipWithIndex) {
          val TypeAndOffset(valueTyp, valueOff) = writeCypherValue(builder, value)
          valuesTyps(i) = valueTyp
          valuesOffs(i) = valueOff
        }
        val off = persistence.CypherValueConstraintListContains.createCypherValueConstraintListContains(
          builder,
          persistence.CypherValueConstraintListContains.createValuesTypeVector(builder, valuesTyps),
          persistence.CypherValueConstraintListContains.createValuesVector(builder, valuesOffs)
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintListContains, off)
    }

  private[this] def readCypherValueConstraint(
    typ: Byte,
    makeValueConstraint: Table => Table
  ): MultipleValuesStandingQuery.LocalProperty.ValueConstraint =
    typ match {
      case persistence.CypherValueConstraint.CypherValueConstraintEqual =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintEqual())
          .asInstanceOf[persistence.CypherValueConstraintEqual]
        val value = readCypherValue(cons.compareToType, cons.compareTo(_))
        MultipleValuesStandingQuery.LocalProperty.Equal(value)

      case persistence.CypherValueConstraint.CypherValueConstraintNotEqual =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintNotEqual())
          .asInstanceOf[persistence.CypherValueConstraintNotEqual]
        val value = readCypherValue(cons.compareToType, cons.compareTo)
        MultipleValuesStandingQuery.LocalProperty.NotEqual(value)

      case persistence.CypherValueConstraint.CypherValueConstraintAny =>
        MultipleValuesStandingQuery.LocalProperty.Any

      case persistence.CypherValueConstraint.CypherValueConstraintNone =>
        MultipleValuesStandingQuery.LocalProperty.None

      case persistence.CypherValueConstraint.CypherValueConstraintRegex =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintRegex())
          .asInstanceOf[persistence.CypherValueConstraintRegex]
        val pattern = cons.pattern
        MultipleValuesStandingQuery.LocalProperty.Regex(pattern)

      case persistence.CypherValueConstraint.CypherValueConstraintListContains =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintListContains())
          .asInstanceOf[persistence.CypherValueConstraintListContains]
        val values: Set[cypher.Value] = {
          val builder = Set.newBuilder[cypher.Value]
          var i = 0
          val valuesLength = cons.valuesLength
          while (i < valuesLength) {
            builder += readCypherValue(cons.valuesType(i), cons.values(_, i))
            i += 1
          }
          builder.result()
        }
        MultipleValuesStandingQuery.LocalProperty.ListContains(values)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherValueConstraint.names)
    }

  private[this] def writeMultipleValuesLocalPropertyStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery.LocalProperty
  ): Offset = {
    val propertyKeyOff: Offset = builder.createString(query.propKey.name)
    val TypeAndOffset(consTyp, consOff) = writeCypherValueConstraint(builder, query.propConstraint)
    val aliasedAsOff: Offset = query.aliasedAs match {
      case None => NoOffset
      case Some(an) => builder.createString(an.name)
    }
    persistence.MultipleValuesLocalPropertyStandingQuery.createMultipleValuesLocalPropertyStandingQuery(
      builder,
      propertyKeyOff,
      consTyp,
      consOff,
      aliasedAsOff
    )
  }

  private[this] def readMultipleValuesLocalPropertyStandingQuery(
    query: persistence.MultipleValuesLocalPropertyStandingQuery
  ): MultipleValuesStandingQuery.LocalProperty = {
    val propertyKey = Symbol(query.propertyKey)
    val propertyConstraint = readCypherValueConstraint(query.propertyConstraintType, query.propertyConstraint(_))
    val aliasedAs = Option(query.aliasedAs).map(Symbol.apply)
    MultipleValuesStandingQuery.LocalProperty(propertyKey, propertyConstraint, aliasedAs)
  }

  private[this] def readMultipleValuesLocalIdStandingQuery(
    query: persistence.MultipleValuesLocalIdStandingQuery
  ): MultipleValuesStandingQuery.LocalId =
    MultipleValuesStandingQuery.LocalId(Symbol(query.aliasedAs), query.formatAsString)

  private[this] def writeMultipleValuesLocalIdStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery.LocalId
  ): Offset = {
    val aliasedAsOff: Offset = builder.createString(query.aliasedAs.name)
    persistence.MultipleValuesLocalIdStandingQuery.createMultipleValuesLocalIdStandingQuery(
      builder,
      aliasedAsOff,
      query.formatAsString
    )
  }

  private[this] def writeMultipleValuesSubscribeAcrossEdgeStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery.SubscribeAcrossEdge
  ): Offset = {
    val edgeNameOff: Offset = query.edgeName match {
      case None => NoOffset
      case Some(n) => builder.createString(n.name)
    }
    val edgeDirOff: Offset = query.edgeDirection match {
      case None => NoOffset
      case Some(edgeDir) =>
        persistence.BoxedEdgeDirection.createBoxedEdgeDirection(
          builder,
          edgeDir match {
            case EdgeDirection.Outgoing => persistence.EdgeDirection.Outgoing
            case EdgeDirection.Incoming => persistence.EdgeDirection.Incoming
            case EdgeDirection.Undirected => persistence.EdgeDirection.Undirected
          }
        )
    }
    val TypeAndOffset(andThenTyp, andThenOff) = writeMultipleValuesStandingQuery(builder, query.andThen)
    persistence.MultipleValuesSubscribeAcrossEdgeStandingQuery.createMultipleValuesSubscribeAcrossEdgeStandingQuery(
      builder,
      edgeNameOff,
      edgeDirOff,
      andThenTyp,
      andThenOff
    )
  }

  private[this] def readMultipleValuesSubscribeAcrossEdgeStandingQuery(
    query: persistence.MultipleValuesSubscribeAcrossEdgeStandingQuery
  ): MultipleValuesStandingQuery.SubscribeAcrossEdge = {
    val edgeName: Option[Symbol] = Option(query.edgeName).map(Symbol.apply)
    val edgeDirection: Option[EdgeDirection] = Option(query.edgeDirection).map { dir =>
      dir.edgeDirection match {
        case persistence.EdgeDirection.Outgoing => EdgeDirection.Outgoing
        case persistence.EdgeDirection.Incoming => EdgeDirection.Incoming
        case persistence.EdgeDirection.Undirected => EdgeDirection.Undirected
        case other => throw new InvalidUnionType(other, persistence.EdgeDirection.names)
      }
    }
    val andThen: MultipleValuesStandingQuery = readMultipleValuesStandingQuery(query.andThenType, query.andThen(_))
    MultipleValuesStandingQuery.SubscribeAcrossEdge(edgeName, edgeDirection, andThen)
  }

  private[this] def writeMultipleValuesEdgeSubscriptionReciprocalStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery.EdgeSubscriptionReciprocal
  ): Offset = {
    val halfEdgeOff: Offset = writeHalfEdge(builder, query.halfEdge)
    val andThenIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, query.andThenId)
    persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQuery
      .createMultipleValuesEdgeSubscriptionReciprocalStandingQuery(
        builder,
        halfEdgeOff,
        andThenIdOff
      )
  }

  private[this] def readMultipleValuesEdgeSubscriptionReciprocalStandingQuery(
    query: persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQuery
  ): MultipleValuesStandingQuery.EdgeSubscriptionReciprocal = {
    val halfEdge: HalfEdge = readHalfEdge(query.halfEdge)
    val andThenId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(query.andThenId)
    MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(halfEdge, andThenId)
  }

  private[this] def writeMultipleValuesFilterMapStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery.FilterMap
  ): Offset = {
    val TypeAndOffset(condTyp, condOff) = query.condition match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(cond) => writeCypherExpr(builder, cond)
    }
    val TypeAndOffset(toFilterTyp, toFilterOff) = writeMultipleValuesStandingQuery(builder, query.toFilter)
    val toAddOff: Offset = {
      val toAddOffs: Array[Offset] = new Array[Offset](query.toAdd.size)
      for (((key, valueExpr), i) <- query.toAdd.zipWithIndex) {
        val keyOff = builder.createString(key.name)
        val TypeAndOffset(valueTyp, valueOff) = writeCypherExpr(builder, valueExpr)
        toAddOffs(i) = persistence.CypherMapExprEntry.createCypherMapExprEntry(builder, keyOff, valueTyp, valueOff)
      }
      persistence.MultipleValuesFilterMapStandingQuery.createToAddVector(builder, toAddOffs)
    }
    persistence.MultipleValuesFilterMapStandingQuery.createMultipleValuesFilterMapStandingQuery(
      builder,
      condTyp,
      condOff,
      toFilterTyp,
      toFilterOff,
      query.dropExisting,
      toAddOff
    )
  }

  private[this] def readMultipleValuesFilterMapStandingQuery(
    query: persistence.MultipleValuesFilterMapStandingQuery
  ): MultipleValuesStandingQuery.FilterMap = {
    val condition: Option[cypher.Expr] =
      if (query.conditionType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(query.conditionType, query.condition))
    val toFilter: MultipleValuesStandingQuery = readMultipleValuesStandingQuery(query.toFilterType, query.toFilter)
    val toAdd: List[(Symbol, cypher.Expr)] = List.tabulate(query.toAddLength) { i =>
      val entry: persistence.CypherMapExprEntry = query.toAdd(i)
      val key: Symbol = Symbol(entry.key)
      val value: cypher.Expr = readCypherExpr(entry.valueType, entry.value)
      key -> value
    }
    MultipleValuesStandingQuery.FilterMap(condition, toFilter, query.dropExisting, toAdd)
  }

  protected[this] def writeMultipleValuesStandingQuery(
    builder: FlatBufferBuilder,
    query: MultipleValuesStandingQuery
  ): TypeAndOffset =
    query match {
      case _: MultipleValuesStandingQuery.UnitSq =>
        TypeAndOffset(persistence.MultipleValuesStandingQuery.MultipleValuesUnitStandingQuery, emptyTable(builder))

      case cross: MultipleValuesStandingQuery.Cross =>
        val offset: Offset = writeMultipleValuesCrossStandingQuery(builder, cross)
        TypeAndOffset(persistence.MultipleValuesStandingQuery.MultipleValuesCrossStandingQuery, offset)

      case localProp: MultipleValuesStandingQuery.LocalProperty =>
        val offset: Offset = writeMultipleValuesLocalPropertyStandingQuery(builder, localProp)
        TypeAndOffset(persistence.MultipleValuesStandingQuery.MultipleValuesLocalPropertyStandingQuery, offset)

      case localId: MultipleValuesStandingQuery.LocalId =>
        val offset: Offset = writeMultipleValuesLocalIdStandingQuery(builder, localId)
        TypeAndOffset(persistence.MultipleValuesStandingQuery.MultipleValuesLocalIdStandingQuery, offset)

      case subscriber: MultipleValuesStandingQuery.SubscribeAcrossEdge =>
        val offset: Offset = writeMultipleValuesSubscribeAcrossEdgeStandingQuery(builder, subscriber)
        TypeAndOffset(persistence.MultipleValuesStandingQuery.MultipleValuesSubscribeAcrossEdgeStandingQuery, offset)

      case reciprocal: MultipleValuesStandingQuery.EdgeSubscriptionReciprocal =>
        val offset: Offset = writeMultipleValuesEdgeSubscriptionReciprocalStandingQuery(builder, reciprocal)
        TypeAndOffset(
          persistence.MultipleValuesStandingQuery.MultipleValuesEdgeSubscriptionReciprocalStandingQuery,
          offset
        )

      case filterMap: MultipleValuesStandingQuery.FilterMap =>
        val offset: Offset = writeMultipleValuesFilterMapStandingQuery(builder, filterMap)
        TypeAndOffset(persistence.MultipleValuesStandingQuery.MultipleValuesFilterMapStandingQuery, offset)

    }

  protected[this] def readMultipleValuesStandingQuery(
    typ: Byte,
    makeSq: Table => Table
  ): MultipleValuesStandingQuery =
    typ match {
      case persistence.MultipleValuesStandingQuery.MultipleValuesUnitStandingQuery =>
        MultipleValuesStandingQuery.UnitSq()

      case persistence.MultipleValuesStandingQuery.MultipleValuesCrossStandingQuery =>
        val cross =
          makeSq(new persistence.MultipleValuesCrossStandingQuery())
            .asInstanceOf[persistence.MultipleValuesCrossStandingQuery]
        readMultipleValuesCrossStandingQuery(cross)

      case persistence.MultipleValuesStandingQuery.MultipleValuesLocalPropertyStandingQuery =>
        val localProp = makeSq(new persistence.MultipleValuesLocalPropertyStandingQuery())
          .asInstanceOf[persistence.MultipleValuesLocalPropertyStandingQuery]
        readMultipleValuesLocalPropertyStandingQuery(localProp)

      case persistence.MultipleValuesStandingQuery.MultipleValuesLocalIdStandingQuery =>
        val localId =
          makeSq(new persistence.MultipleValuesLocalIdStandingQuery())
            .asInstanceOf[persistence.MultipleValuesLocalIdStandingQuery]
        readMultipleValuesLocalIdStandingQuery(localId)

      case persistence.MultipleValuesStandingQuery.MultipleValuesSubscribeAcrossEdgeStandingQuery =>
        val subscribeAcrossEdge = makeSq(new persistence.MultipleValuesSubscribeAcrossEdgeStandingQuery())
          .asInstanceOf[persistence.MultipleValuesSubscribeAcrossEdgeStandingQuery]
        readMultipleValuesSubscribeAcrossEdgeStandingQuery(subscribeAcrossEdge)

      case persistence.MultipleValuesStandingQuery.MultipleValuesEdgeSubscriptionReciprocalStandingQuery =>
        val reciprocal = makeSq(new persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQuery())
          .asInstanceOf[persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQuery]
        readMultipleValuesEdgeSubscriptionReciprocalStandingQuery(reciprocal)

      case persistence.MultipleValuesStandingQuery.MultipleValuesFilterMapStandingQuery =>
        val filterMap =
          makeSq(new persistence.MultipleValuesFilterMapStandingQuery())
            .asInstanceOf[persistence.MultipleValuesFilterMapStandingQuery]
        readMultipleValuesFilterMapStandingQuery(filterMap)

      case other =>
        throw new InvalidUnionType(other, persistence.MultipleValuesStandingQuery.names)
    }

}

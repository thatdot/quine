package com.thatdot.quine.persistor

import java.nio.ByteBuffer
import java.util.UUID
import java.util.regex.Pattern

import scala.collection.compat.immutable._
import scala.collection.{AbstractIterable, mutable}

import com.google.flatbuffers.{FlatBufferBuilder, Table}
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph._
import com.thatdot.quine.graph.behavior.{DomainNodeIndexBehavior, StandingQuerySubscribers}
import com.thatdot.quine.graph.cypher.{Expr, StandingQueryState}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{CypherSubscriber, ResultId}
import com.thatdot.quine.model.DomainGraphNode.{DomainGraphNodeEdge, DomainGraphNodeId}
import com.thatdot.quine.model._
import com.thatdot.quine.persistence

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
object PersistenceCodecs extends LazyLogging {

  import PackedFlatBufferBinaryFormat._

  private[this] def writeDuration(builder: FlatBufferBuilder, duration: java.time.Duration): Offset =
    persistence.Duration.createDuration(builder, duration.getSeconds, duration.getNano)

  private[this] def readDuration(duration: persistence.Duration): java.time.Duration =
    java.time.Duration.ofSeconds(duration.seconds, duration.nanos.toLong)

  private[this] def readLocalDate(localDate: persistence.LocalDate): java.time.LocalDate =
    java.time.LocalDate.of(localDate.year, localDate.month.toInt, localDate.day.toInt)

  private[this] def readLocalTime(localTime: persistence.LocalTime): java.time.LocalTime =
    java.time.LocalTime.of(localTime.hour.toInt, localTime.minute.toInt, localTime.second.toInt, localTime.nano)

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

  private[this] def edgeDirection2Byte(direction: EdgeDirection): Byte =
    direction match {
      case EdgeDirection.Outgoing => persistence.EdgeDirection.Outgoing
      case EdgeDirection.Incoming => persistence.EdgeDirection.Incoming
      case EdgeDirection.Undirected => persistence.EdgeDirection.Undirected
    }

  private[this] def byte2EdgeDirection(direction: Byte): EdgeDirection =
    direction match {
      case persistence.EdgeDirection.Outgoing => EdgeDirection.Outgoing
      case persistence.EdgeDirection.Incoming => EdgeDirection.Incoming
      case persistence.EdgeDirection.Undirected => EdgeDirection.Undirected
      case other => throw new InvalidUnionType(other, persistence.EdgeDirection.names)
    }

  private[this] def writeGenericEdge(builder: FlatBufferBuilder, edge: GenericEdge): Offset =
    persistence.GenericEdge.createGenericEdge(
      builder,
      builder.createString(edge.edgeType.name),
      edgeDirection2Byte(edge.direction)
    )

  private[this] def readGenericEdge(edge: persistence.GenericEdge): GenericEdge =
    GenericEdge(
      Symbol(edge.edgeType),
      byte2EdgeDirection(edge.direction)
    )

  private[this] def writeQuineId(builder: FlatBufferBuilder, qid: QuineId): Offset =
    persistence.QuineId.createQuineId(
      builder,
      persistence.QuineId.createIdVector(builder, qid.array)
    )

  private[this] def readQuineId(qid: persistence.QuineId): QuineId =
    new QuineId(qid.idAsByteBuffer.remainingBytes)

  import org.msgpack.core.MessagePack

  private[this] def writeQuineValue(builder: FlatBufferBuilder, quineValue: QuineValue): Offset =
    persistence.QuineValue.createQuineValue(
      builder,
      builder.createByteVector(QuineValue.writeMsgPack(quineValue))
    )

  private[this] def readQuineValue(quineValue: persistence.QuineValue): QuineValue =
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
        val value: cypher.Value = readCypherValue(property.valueType, property.value(_))
        builder += Symbol(property.key) -> value
        i += 1
      }
      builder.result()
    }
    cypher.Expr.Node(readQuineId(node.id), labels, properties)
  }

  private[this] def readCypherPath(path: persistence.CypherPath): cypher.Expr.Path = {
    val head: cypher.Expr.Node = readCypherNode(path.head)
    val tails: Vector[(cypher.Expr.Relationship, cypher.Expr.Node)] = {
      val builder = Vector.newBuilder[(cypher.Expr.Relationship, cypher.Expr.Node)]
      var i = 0
      val tailsLength = path.tailsLength
      while (i < tailsLength) {
        val segment = path.tails(i)
        val rel = readCypherRelationship(segment.edge)
        val to = readCypherNode(segment.to)
        builder += rel -> to
        i += 1
      }
      builder.result()
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
        val value: cypher.Value = readCypherValue(property.valueType, property.value(_))
        builder += Symbol(property.key) -> value
        i += 1
      }
      builder.result()
    }
    val end: QuineId = readQuineId(relationship.end)
    cypher.Expr.Relationship(start, name, properties, end)
  }

  private[this] def readCypherList(list: persistence.CypherList): cypher.Expr.List = {
    val elements = Vector.newBuilder[cypher.Value]
    var i = 0
    val elementsLength = list.elementsLength
    while (i < elementsLength) {
      elements += readCypherValue(list.elementsType(i), list.elements(_, i))
      i += 1
    }
    cypher.Expr.List(elements.result())
  }

  private[this] def readCypherMap(map: persistence.CypherMap): cypher.Expr.Map = {
    val entries = Map.newBuilder[String, cypher.Value]
    var i = 0
    val entriesLength = map.entriesLength
    while (i < entriesLength) {
      val entry: persistence.CypherProperty = map.entries(i)
      val value: cypher.Value = readCypherValue(entry.valueType, entry.value(_))
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
    val expr: cypher.Expr = readCypherExpr(property.exprType, property.expr(_))
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
    val list: cypher.Expr = readCypherExpr(listSlice.listType, listSlice.list(_))
    val from: Option[cypher.Expr] =
      if (listSlice.fromType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(listSlice.fromType, listSlice.from(_)))
    val to: Option[cypher.Expr] =
      if (listSlice.toType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(listSlice.toType, listSlice.to(_)))
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
      val value = readCypherExpr(mapEntry.valueType, mapEntry.value(_))
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
    val original: cypher.Expr = readCypherExpr(mapProjection.originalType, mapProjection.original(_))
    val items: Seq[(String, cypher.Expr)] = {
      val builder = Seq.newBuilder[(String, cypher.Expr)]
      var i = 0
      val itemsLength = mapProjection.itemsLength
      while (i < itemsLength) {
        val mapEntry: persistence.CypherMapExprEntry = mapProjection.items(i)
        val value = readCypherExpr(mapEntry.valueType, mapEntry.value(_))
        builder += mapEntry.key -> value
        i += 1
      }
      builder.result()
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
    val rhs: cypher.Expr = readCypherExpr(unaryOp.rhsType, unaryOp.rhs(_))
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
    val lhs: cypher.Expr = readCypherExpr(binaryOp.lhsType, binaryOp.lhs(_))
    val rhs: cypher.Expr = readCypherExpr(binaryOp.rhsType, binaryOp.rhs(_))
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
    val arguments: Vector[cypher.Expr] = {
      val builder = Vector.newBuilder[cypher.Expr]
      var i = 0
      val argumentsLength = naryOp.argumentsLength
      while (i < argumentsLength) {
        builder += readCypherExpr(naryOp.argumentsType(i), naryOp.arguments(_, i))
        i += 1
      }
      builder.result()
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
      else Some(readCypherExpr(caseExp.scrutineeType, caseExp.scrutinee(_)))
    val branches: Vector[(cypher.Expr, cypher.Expr)] = {
      val builder = Vector.newBuilder[(cypher.Expr, cypher.Expr)]
      var i = 0
      val branchesLength = caseExp.branchesLength
      while (i < branchesLength) {
        val branch: persistence.CypherCaseBranch = caseExp.branches(i)
        val cond: cypher.Expr = readCypherExpr(branch.conditionType, branch.condition(_))
        val outcome: cypher.Expr = readCypherExpr(branch.outcomeType, branch.outcome(_))
        builder += cond -> outcome
        i += 1
      }
      builder.result()
    }
    val default: Option[cypher.Expr] =
      if (caseExp.fallThroughType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(caseExp.fallThroughType, caseExp.fallThrough(_)))
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
    val arguments: Vector[cypher.Expr] = {
      val builder = Vector.newBuilder[cypher.Expr]
      var i: Int = 0
      val argumentsLength = func.argumentsLength
      while (i < argumentsLength) {
        builder += readCypherExpr(func.argumentsType(i), func.arguments(_, i))
        i += 1
      }
      builder.result()
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

  private[this] def writeCypherValue(builder: FlatBufferBuilder, expr: cypher.Value): TypeAndOffset =
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
    }

  private[this] def readCypherValue(typ: Byte, makeExpr: Table => Table): cypher.Value =
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

      case persistence.CypherValue.CypherDuration =>
        val duration = makeExpr(new persistence.CypherDuration()).asInstanceOf[persistence.CypherDuration]
        readCypherDuration(duration)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherValue.names)
    }

  private[this] def writeCypherExpr(builder: FlatBufferBuilder, expr: cypher.Expr): TypeAndOffset =
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

  private[this] def readCypherExpr(typ: Byte, makeExpr: Table => Table): cypher.Expr =
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

      case other =>
        throw new InvalidUnionType(other, persistence.CypherValue.names)
    }

  private[this] def writeBoxedCypherExpr(builder: FlatBufferBuilder, expr: cypher.Expr): Offset = {
    val TypeAndOffset(exprTyp, exprOff) = writeCypherExpr(builder, expr)
    persistence.BoxedCypherExpr.createBoxedCypherExpr(builder, exprTyp, exprOff)
  }

  private[this] def readBoxedCypherExpr(expr: persistence.BoxedCypherExpr): cypher.Expr =
    readCypherExpr(expr.exprType, expr.expr(_))

  private[this] def writeStandingQueryId(builder: FlatBufferBuilder, sqId: StandingQueryId): Offset =
    persistence.StandingQueryId.createStandingQueryId(
      builder,
      sqId.uuid.getLeastSignificantBits,
      sqId.uuid.getMostSignificantBits
    )

  private[this] def readStandingQueryId(sqId: persistence.StandingQueryId): StandingQueryId =
    StandingQueryId(new UUID(sqId.highBytes, sqId.lowBytes))

  private[this] def writeStandingQueryPartId(builder: FlatBufferBuilder, sqId: StandingQueryPartId): Offset =
    persistence.StandingQueryPartId.createStandingQueryPartId(
      builder,
      sqId.uuid.getLeastSignificantBits,
      sqId.uuid.getMostSignificantBits
    )

  private[this] def readStandingQueryResultId(resId: persistence.StandingQueryResultId): ResultId =
    ResultId(new UUID(resId.highBytes, resId.lowBytes))

  private[this] def writeStandingQueryResultId(builder: FlatBufferBuilder, resId: ResultId): Offset =
    persistence.StandingQueryResultId.createStandingQueryResultId(
      builder,
      resId.uuid.getLeastSignificantBits,
      resId.uuid.getMostSignificantBits
    )

  private[this] def readStandingQueryPartId(sqId: persistence.StandingQueryPartId): StandingQueryPartId =
    StandingQueryPartId(new UUID(sqId.highBytes, sqId.lowBytes))

  private[this] def writeHalfEdge(builder: FlatBufferBuilder, edge: HalfEdge): Offset =
    persistence.HalfEdge.createHalfEdge(
      builder,
      builder.createSharedString(edge.edgeType.name),
      edgeDirection2Byte(edge.direction),
      writeQuineId(builder, edge.other)
    )

  private[this] def readHalfEdge(edge: persistence.HalfEdge): HalfEdge =
    HalfEdge(
      Symbol(edge.edgeType),
      byte2EdgeDirection(edge.direction),
      readQuineId(edge.other)
    )

  private[this] def writePropertyComparisonFunction(
    builder: FlatBufferBuilder,
    func: PropertyComparisonFunc
  ): TypeAndOffset =
    func match {
      case PropertyComparisonFunctions.Identicality =>
        TypeAndOffset(
          persistence.PropertyComparisonFunction.PropertyComparisonFunctionIdenticality,
          emptyTable(builder)
        )
      case PropertyComparisonFunctions.Wildcard =>
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionWildcard, emptyTable(builder))
      case PropertyComparisonFunctions.NoValue =>
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionNone, emptyTable(builder))
      case PropertyComparisonFunctions.NonIdenticality =>
        TypeAndOffset(
          persistence.PropertyComparisonFunction.PropertyComparisonFunctionNonIdenticality,
          emptyTable(builder)
        )
      case PropertyComparisonFunctions.RegexMatch(pattern) =>
        val patternOff: Offset = builder.createString(pattern)
        val offset: Offset =
          persistence.PropertyComparisonFunctionRegexMatch.createPropertyComparisonFunctionRegexMatch(
            builder,
            patternOff
          )
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionRegexMatch, offset)
      case PropertyComparisonFunctions.ListContains(values) =>
        val valuesOffs: Array[Offset] = new Array[Offset](values.size)
        for ((value, i) <- values.zipWithIndex)
          valuesOffs(i) = writeQuineValue(builder, value)
        val off = persistence.PropertyComparisonFunctionListContains.createPropertyComparisonFunctionListContains(
          builder,
          persistence.PropertyComparisonFunctionListContains.createValuesVector(builder, valuesOffs)
        )
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionListContains, off)
    }

  private[this] def readPropertyComparisonFunction(
    typ: Byte,
    makeFunc: Table => Table
  ): PropertyComparisonFunc =
    typ match {
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionIdenticality =>
        PropertyComparisonFunctions.Identicality
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionWildcard =>
        PropertyComparisonFunctions.Wildcard
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionNone =>
        PropertyComparisonFunctions.NoValue
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionNonIdenticality =>
        PropertyComparisonFunctions.NonIdenticality
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionRegexMatch =>
        val regexMatch = makeFunc(new persistence.PropertyComparisonFunctionRegexMatch())
          .asInstanceOf[persistence.PropertyComparisonFunctionRegexMatch]
        PropertyComparisonFunctions.RegexMatch(regexMatch.pattern)
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionListContains =>
        val cons = makeFunc(new persistence.PropertyComparisonFunctionListContains())
          .asInstanceOf[persistence.PropertyComparisonFunctionListContains]
        val values: Set[QuineValue] = {
          val builder = Set.newBuilder[QuineValue]
          var i = 0
          val valuesLength = cons.valuesLength
          while (i < valuesLength) {
            builder += readQuineValue(cons.values(i))
            i += 1
          }
          builder.result()
        }
        PropertyComparisonFunctions.ListContains(values)

      case other =>
        throw new InvalidUnionType(other, persistence.PropertyComparisonFunction.names)
    }

  private[this] def writeDomainNodeEquiv(builder: FlatBufferBuilder, dne: DomainNodeEquiv): Offset = {
    val classNameOff: Offset = dne.className match {
      case None => NoOffset
      case Some(name) => builder.createString(name)
    }

    val localPropsOff: Offset = {
      val localPropertiesOffs: Array[Offset] = new Array[Offset](dne.localProps.size)
      for (((propKey, (compFunction, propValueOpt)), i) <- dne.localProps.zipWithIndex) {
        val propKeyOff: Offset = builder.createString(propKey.name)
        val TypeAndOffset(compFuncTyp, compFuncOff) = writePropertyComparisonFunction(builder, compFunction)
        val propValueOff: Offset = persistence.LocalProperty.createValueVector(
          builder,
          propValueOpt match {
            case None => Array.emptyByteArray
            case Some(propVal) => propVal.serialized
          }
        )
        val localProp =
          persistence.LocalProperty.createLocalProperty(builder, propKeyOff, compFuncTyp, compFuncOff, propValueOff)
        localPropertiesOffs(i) = localProp
      }
      persistence.DomainNodeEquiv.createLocalPropertiesVector(builder, localPropertiesOffs)
    }

    val circularEdgesOff: Offset = {
      val circularEdgesOffs: Array[Offset] = new Array[Offset](dne.circularEdges.size)
      for (((edgeType, isDirected), i) <- dne.circularEdges.zipWithIndex)
        circularEdgesOffs(i) = persistence.CircularEdge.createCircularEdge(
          builder,
          builder.createString(edgeType.name),
          isDirected
        )
      persistence.DomainNodeEquiv.createCircularEdgesVector(builder, circularEdgesOffs)
    }

    persistence.DomainNodeEquiv.createDomainNodeEquiv(
      builder,
      classNameOff,
      localPropsOff,
      circularEdgesOff
    )
  }

  private[this] def readDomainNodeEquiv(dne: persistence.DomainNodeEquiv): DomainNodeEquiv = {
    val className: Option[String] = Option(dne.className)

    val localProps: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])] = {
      val builder = Map.newBuilder[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]
      var i: Int = 0
      val localPropertiesLength: Int = dne.localPropertiesLength
      while (i < localPropertiesLength) {
        val localProperty: persistence.LocalProperty = dne.localProperties(i)
        val propertyKey: Symbol = Symbol(localProperty.propertyKey)
        val compFunc: PropertyComparisonFunc = readPropertyComparisonFunction(
          localProperty.comparisonFunctionType,
          localProperty.comparisonFunction(_)
        )
        val propertyValueBytes: Option[PropertyValue] = {
          val bytes = localProperty.valueAsByteBuffer.remainingBytes
          if (bytes.length == 0) None
          else Some(PropertyValue.fromBytes(bytes))
        }
        builder += propertyKey -> (compFunc -> propertyValueBytes)
        i += 1
      }
      builder.result()
    }

    val circularEdges: Set[CircularEdge] = {
      val builder = Set.newBuilder[CircularEdge]
      var i: Int = 0
      val circularEdgesLength = dne.circularEdgesLength
      while (i < circularEdgesLength) {
        val circularEdge: persistence.CircularEdge = dne.circularEdges(i)
        builder += Symbol(circularEdge.edgeType) -> circularEdge.isDirected
        i += 1
      }
      builder.result()
    }

    DomainNodeEquiv(className, localProps, circularEdges)
  }

  private[this] def writeDomainEdge(
    builder: FlatBufferBuilder,
    de: DomainGraphNodeEdge
  ): Offset = {

    val depDirection: Byte = de.depDirection match {
      case DependsUpon => persistence.DependencyDirection.DependsUpon
      case IsDependedUpon => persistence.DependencyDirection.IsDependedUpon
      case Incidental => persistence.DependencyDirection.Incidental
    }

    val constraints: TypeAndOffset = de.constraints match {
      case MandatoryConstraint =>
        TypeAndOffset(persistence.EdgeMatchConstraints.MandatoryConstraint, emptyTable(builder))

      case FetchConstraint(min, max) =>
        TypeAndOffset(
          persistence.EdgeMatchConstraints.FetchConstraint,
          persistence.FetchConstraint.createFetchConstraint(builder, min, max.isDefined, max.getOrElse(0))
        )
    }

    persistence.DomainEdge.createDomainEdge(
      builder,
      writeGenericEdge(builder, de.edge),
      depDirection,
      de.dgnId,
      de.circularMatchAllowed,
      constraints.typ,
      constraints.offset
    )
  }

  private[this] def readDomainEdge(de: persistence.DomainEdge): DomainGraphNodeEdge = {

    val depDirection: DependencyDirection = de.dependency match {
      case persistence.DependencyDirection.DependsUpon => DependsUpon
      case persistence.DependencyDirection.IsDependedUpon => IsDependedUpon
      case persistence.DependencyDirection.Incidental => Incidental
      case other => throw new InvalidUnionType(other, persistence.DependencyDirection.names)
    }

    val constraints: EdgeMatchConstraints = de.constraintsType match {
      case persistence.EdgeMatchConstraints.MandatoryConstraint =>
        MandatoryConstraint
      case persistence.EdgeMatchConstraints.FetchConstraint =>
        val fetch = de.constraints(new persistence.FetchConstraint()).asInstanceOf[persistence.FetchConstraint]
        FetchConstraint(fetch.min, if (fetch.hasMax) Some(fetch.max) else None)
      case other =>
        throw new InvalidUnionType(other, persistence.EdgeMatchConstraints.names)
    }

    DomainGraphNodeEdge(
      readGenericEdge(de.edge),
      depDirection,
      de.dgnId,
      de.circularMatchAllowed,
      constraints
    )
  }

  private[this] def writeDomainGraphNode(
    builder: FlatBufferBuilder,
    dgn: DomainGraphNode
  ): TypeAndOffset =
    dgn match {
      case DomainGraphNode.Single(dne, identification, nextNodes, compFunc) =>
        val identificationOff: Offset = identification match {
          case None => NoOffset
          case Some(id) =>
            persistence.Identification.createIdentification(
              builder,
              writeQuineId(builder, id)
            )
        }

        val nextNodesOff: Offset = {
          val nextNodesOffs: Array[Offset] = new Array[Offset](nextNodes.size)
          var i = 0
          for (nextNode <- nextNodes) {
            nextNodesOffs(i) = writeDomainEdge(builder, nextNode)
            i += 1
          }
          persistence.SingleNode.createNextNodesVector(builder, nextNodesOffs)
        }

        val comparisonFunction: Byte = compFunc match {
          case NodeLocalComparisonFunctions.Identicality =>
            persistence.NodeLocalComparisonFunction.Identicality
          case NodeLocalComparisonFunctions.EqualSubset =>
            persistence.NodeLocalComparisonFunction.EqualSubset
          case NodeLocalComparisonFunctions.Wildcard =>
            persistence.NodeLocalComparisonFunction.Wildcard
        }

        val offset: Offset = persistence.SingleNode.createSingleNode(
          builder,
          writeDomainNodeEquiv(builder, dne),
          identificationOff,
          nextNodesOff,
          comparisonFunction
        )
        TypeAndOffset(persistence.DomainGraphNode.SingleNode, offset)

      case DomainGraphNode.Or(disjuncts) =>
        val offset: Offset = persistence.OrNode.createOrNode(
          builder,
          persistence.OrNode.createDisjunctsDgnIdsVector(builder, disjuncts.toArray)
        )
        TypeAndOffset(persistence.DomainGraphNode.OrNode, offset)

      case DomainGraphNode.And(conjuncts) =>
        val offset = persistence.AndNode.createAndNode(
          builder,
          persistence.AndNode.createConjunctsDgnIdsVector(builder, conjuncts.toArray)
        )
        TypeAndOffset(persistence.DomainGraphNode.AndNode, offset)

      case DomainGraphNode.Not(negated) =>
        TypeAndOffset(
          persistence.DomainGraphNode.NotNode,
          persistence.NotNode.createNotNode(builder, negated)
        )

      case DomainGraphNode.Mu(muVar, dgnId) =>
        val offset: Offset = persistence.MuNode.createMuNode(
          builder,
          builder.createString(muVar.str),
          dgnId
        )
        TypeAndOffset(persistence.DomainGraphNode.MuNode, offset)

      case DomainGraphNode.MuVar(muVar) =>
        TypeAndOffset(
          persistence.DomainGraphNode.MuVarNode,
          persistence.MuVarNode.createMuVarNode(builder, builder.createString(muVar.str))
        )
    }

  private[this] def readDomainGraphNode(
    typ: Byte,
    makeNode: Table => Table
  ): DomainGraphNode =
    typ match {
      case persistence.DomainGraphNode.SingleNode =>
        val single = makeNode(new persistence.SingleNode()).asInstanceOf[persistence.SingleNode]
        val domainNodeEquiv: DomainNodeEquiv = readDomainNodeEquiv(single.domainNodeEquiv)
        val identification = Option(single.identification).map(ident => readQuineId(ident.id))
        val nextNodes = {
          val builder = Seq.newBuilder[DomainGraphNodeEdge]
          var i: Int = 0
          val nextNodesLength = single.nextNodesLength
          while (i < nextNodesLength) {
            builder += readDomainEdge(single.nextNodes(i))
            i += 1
          }
          builder.result()
        }
        val comparisonFunc: NodeLocalComparisonFunc = single.comparisonFunction match {
          case persistence.NodeLocalComparisonFunction.Identicality =>
            NodeLocalComparisonFunctions.Identicality
          case persistence.NodeLocalComparisonFunction.EqualSubset =>
            NodeLocalComparisonFunctions.EqualSubset
          case persistence.NodeLocalComparisonFunction.Wildcard =>
            NodeLocalComparisonFunctions.Wildcard
          case other =>
            throw new InvalidUnionType(other, persistence.NodeLocalComparisonFunction.names)
        }

        DomainGraphNode.Single(domainNodeEquiv, identification, nextNodes, comparisonFunc)

      case persistence.DomainGraphNode.OrNode =>
        val or = makeNode(new persistence.OrNode()).asInstanceOf[persistence.OrNode]
        val disjuncts = Seq.newBuilder[DomainGraphNodeId]
        var i: Int = 0
        while (i < or.disjunctsDgnIdsLength()) {
          disjuncts += or.disjunctsDgnIds(i)
          i += 1
        }
        DomainGraphNode.Or(disjuncts.result())

      case persistence.DomainGraphNode.AndNode =>
        val and = makeNode(new persistence.AndNode()).asInstanceOf[persistence.AndNode]
        val conjuncts = Seq.newBuilder[DomainGraphNodeId]
        var i: Int = 0
        while (i < and.conjunctsDgnIdsLength()) {
          conjuncts += and.conjunctsDgnIds(i)
          i += 1
        }
        DomainGraphNode.And(conjuncts.result())

      case persistence.DomainGraphNode.NotNode =>
        val not = makeNode(new persistence.NotNode()).asInstanceOf[persistence.NotNode]
        DomainGraphNode.Not(not.negatedDgnId)

      case persistence.DomainGraphNode.MuNode =>
        val mu = makeNode(new persistence.MuNode()).asInstanceOf[persistence.MuNode]
        DomainGraphNode.Mu(MuVariableName(mu.variable), mu.dgnId)

      case persistence.DomainGraphNode.MuVarNode =>
        val muVar = makeNode(new persistence.MuVarNode()).asInstanceOf[persistence.MuVarNode]
        DomainGraphNode.MuVar(MuVariableName(muVar.variable))

      case other =>
        throw new InvalidUnionType(other, persistence.DomainGraphNode.names)
    }

  private[this] def writeBoxedDomainGraphNode(builder: FlatBufferBuilder, dgn: DomainGraphNode): Offset = {
    val TypeAndOffset(nodeTyp, nodeOff) = writeDomainGraphNode(builder, dgn)
    persistence.BoxedDomainGraphNode.createBoxedDomainGraphNode(builder, nodeTyp, nodeOff)
  }

  private[this] def readBoxedDomainGraphNode(branch: persistence.BoxedDomainGraphNode): DomainGraphNode =
    readDomainGraphNode(branch.nodeType, branch.node(_))

  private[this] def writeNodeEventUnion(
    builder: FlatBufferBuilder,
    event: NodeEvent
  ): TypeAndOffset =
    event match {
      case NodeChangeEvent.EdgeAdded(HalfEdge(edgeType, dir, other)) =>
        val event = persistence.AddEdge.createAddEdge(
          builder,
          builder.createString(edgeType.name),
          edgeDirection2Byte(dir),
          builder.createByteVector(other.array)
        )
        TypeAndOffset(persistence.NodeEventUnion.AddEdge, event)

      case NodeChangeEvent.EdgeRemoved(HalfEdge(edgeType, dir, other)) =>
        val event = persistence.RemoveEdge.createRemoveEdge(
          builder,
          builder.createString(edgeType.name),
          edgeDirection2Byte(dir),
          builder.createByteVector(other.array)
        )
        TypeAndOffset(persistence.NodeEventUnion.RemoveEdge, event)

      case NodeChangeEvent.PropertySet(propKey, value) =>
        val event = persistence.AddProperty.createAddProperty(
          builder,
          builder.createString(propKey.name),
          builder.createByteVector(value.serialized)
        )
        TypeAndOffset(persistence.NodeEventUnion.AddProperty, event)

      case NodeChangeEvent.PropertyRemoved(propKey, value) =>
        val event = persistence.RemoveProperty.createRemoveProperty(
          builder,
          builder.createString(propKey.name),
          builder.createByteVector(value.serialized)
        )
        TypeAndOffset(persistence.NodeEventUnion.RemoveProperty, event)

      case DomainIndexEvent.CreateDomainNodeSubscription(dgnId, replyToNode, relatedQueries) =>
        val rltd: Offset = {
          val relatedQueriesOffsets = new Array[Offset](relatedQueries.size)
          for ((relatedQuery, i) <- relatedQueries.zipWithIndex)
            relatedQueriesOffsets(i) = writeStandingQueryId(builder, relatedQuery)
          persistence.CreateDomainNodeSubscription.createRelatedQueriesVector(builder, relatedQueriesOffsets)
        }
        val event = persistence.CreateDomainNodeSubscription.createCreateDomainNodeSubscription(
          builder,
          dgnId,
          builder.createByteVector(replyToNode.array), //,writeQuineId(builder, replyToNode),
          rltd
        )
        TypeAndOffset(persistence.NodeEventUnion.CreateDomainNodeSubscription, event)

      case DomainIndexEvent.CreateDomainStandingQuerySubscription(testBranch, sqId, relatedQueries) =>
        val rltd = {
          val relatedQueriesOffsets = new Array[Offset](relatedQueries.size)
          for ((relatedQuery, i) <- relatedQueries.zipWithIndex)
            relatedQueriesOffsets(i) = writeStandingQueryId(builder, relatedQuery)
          persistence.CreateDomainStandingQuerySubscription.createRelatedQueriesVector(builder, relatedQueriesOffsets)

        }

        val event = persistence.CreateDomainStandingQuerySubscription.createCreateDomainStandingQuerySubscription(
          builder,
          testBranch,
          writeStandingQueryId(builder, sqId),
          rltd
        )
        TypeAndOffset(persistence.NodeEventUnion.CreateDomainStandingQuerySubscription, event)

      case DomainIndexEvent.DomainNodeSubscriptionResult(from, testBranch, result) =>
        val event: Offset = persistence.DomainNodeSubscriptionResult.createDomainNodeSubscriptionResult(
          builder,
          builder.createByteVector(from.array),
          testBranch,
          result
        )
        TypeAndOffset(persistence.NodeEventUnion.DomainNodeSubscriptionResult, event)

      case DomainIndexEvent.CancelDomainNodeSubscription(testBranch, alreadyCancelledSubscriber) =>
        val event = persistence.CancelDomainNodeSubscription.createCancelDomainNodeSubscription(
          builder,
          testBranch,
          builder.createByteVector(alreadyCancelledSubscriber.array)
        )
        TypeAndOffset(persistence.NodeEventUnion.CancelDomainNodeSubscription, event)
    }

  private[this] def readNodeEventUnion(
    typ: Byte,
    makeEvent: Table => Table
  ): NodeEvent =
    typ match {
      case persistence.NodeEventUnion.AddEdge =>
        val event = makeEvent(new persistence.AddEdge()).asInstanceOf[persistence.AddEdge]
        val halfEdge = HalfEdge(
          Symbol(event.edgeType),
          byte2EdgeDirection(event.direction),
          new QuineId(event.otherIdAsByteBuffer.remainingBytes)
        )
        NodeChangeEvent.EdgeAdded(halfEdge)

      case persistence.NodeEventUnion.RemoveEdge =>
        val event = makeEvent(new persistence.RemoveEdge()).asInstanceOf[persistence.RemoveEdge]
        val halfEdge = HalfEdge(
          Symbol(event.edgeType),
          byte2EdgeDirection(event.direction),
          new QuineId(event.otherIdAsByteBuffer.remainingBytes)
        )
        NodeChangeEvent.EdgeRemoved(halfEdge)

      case persistence.NodeEventUnion.AddProperty =>
        val event = makeEvent(new persistence.AddProperty()).asInstanceOf[persistence.AddProperty]
        val propertyKey = Symbol(event.key)
        val propertyValue = PropertyValue.fromBytes(event.valueAsByteBuffer.remainingBytes)
        NodeChangeEvent.PropertySet(propertyKey, propertyValue)

      case persistence.NodeEventUnion.RemoveProperty =>
        val event = makeEvent(new persistence.RemoveProperty()).asInstanceOf[persistence.RemoveProperty]
        val propertyKey = Symbol(event.key)
        val propertyValue = PropertyValue.fromBytes(event.valueAsByteBuffer.remainingBytes)
        NodeChangeEvent.PropertyRemoved(propertyKey, propertyValue)

      case persistence.NodeEventUnion.CreateDomainNodeSubscription =>
        val event = makeEvent(new persistence.CreateDomainNodeSubscription())
          .asInstanceOf[persistence.CreateDomainNodeSubscription]
        val dgnId = event.testDgnId()
        val replyTo = new QuineId(event.replyToAsByteBuffer.remainingBytes)
        val relatedQueries = Set.newBuilder[StandingQueryId]
        var i = 0
        val length = event.relatedQueriesLength
        while (i < length) {
          relatedQueries += readStandingQueryId(event.relatedQueries(i))
          i += 1
        }
        DomainIndexEvent.CreateDomainNodeSubscription(dgnId, replyTo, relatedQueries.result())

      case persistence.NodeEventUnion.CreateDomainStandingQuerySubscription =>
        val event = makeEvent(new persistence.CreateDomainStandingQuerySubscription())
          .asInstanceOf[persistence.CreateDomainStandingQuerySubscription]
        val dgnId = event.testDgnId()
        val replyTo = readStandingQueryId(event.replyTo)
        val relatedQueries = Set.newBuilder[StandingQueryId]
        var i = 0
        val length = event.relatedQueriesLength
        while (i < length) {
          relatedQueries += readStandingQueryId(event.relatedQueries(i))
          i += 1
        }
        DomainIndexEvent.CreateDomainStandingQuerySubscription(dgnId, replyTo, relatedQueries.result())

      case persistence.NodeEventUnion.DomainNodeSubscriptionResult =>
        val event = makeEvent(new persistence.DomainNodeSubscriptionResult())
          .asInstanceOf[persistence.DomainNodeSubscriptionResult]
        val from = new QuineId(event.fromIdAsByteBuffer.remainingBytes)
        val dgnId = event.testDgnId()
        val result = event.result()
        DomainIndexEvent.DomainNodeSubscriptionResult(from, dgnId, result)

      case persistence.NodeEventUnion.CancelDomainNodeSubscription =>
        val event = makeEvent(new persistence.CancelDomainNodeSubscription())
          .asInstanceOf[persistence.CancelDomainNodeSubscription]
        val dgnId = event.testDgnId()
        val subscriber = new QuineId(event.alreadyCancelledSubscriberAsByteBuffer.remainingBytes)
        DomainIndexEvent.CancelDomainNodeSubscription(dgnId, subscriber)

      case other =>
        throw new InvalidUnionType(other, persistence.NodeEventUnion.names)
    }

  private[this] def writeNodeEventWithTime(
    builder: FlatBufferBuilder,
    eventWithTime: NodeEvent.WithTime
  ): Offset = {
    val TypeAndOffset(eventTyp, eventOff) = writeNodeEventUnion(builder, eventWithTime.event)
    persistence.NodeEventWithTime.createNodeEventWithTime(
      builder,
      eventWithTime.atTime.eventTime,
      eventTyp,
      eventOff
    )
  }

  private[this] def readNodeEventWithTime(
    eventWithTime: persistence.NodeEventWithTime
  ): NodeEvent.WithTime = {
    val event = readNodeEventUnion(eventWithTime.eventType, eventWithTime.event(_))
    val atTime = EventTime.fromRaw(eventWithTime.eventTime)
    NodeEvent.WithTime(event, atTime)
  }

  private[this] def writeNodeEvent(
    builder: FlatBufferBuilder,
    event: NodeEvent
  ): Offset = {
    val TypeAndOffset(eventTyp, eventOff) = writeNodeEventUnion(builder, event)
    persistence.NodeEvent.createNodeEvent(builder, eventTyp, eventOff)
  }

  private[this] def readNodeEvent(
    event: persistence.NodeEvent
  ): NodeEvent =
    readNodeEventUnion(event.eventType, event.event(_))

  private[this] def writeNodeSnapshot(
    builder: FlatBufferBuilder,
    snapshot: NodeSnapshot
  ): Offset = {

    val time = snapshot.time.eventTime
    val properties: Offset = {
      val propertiesOffs: Array[Offset] = new Array[Offset](snapshot.properties.size)
      for (((propKey, propVal), i) <- snapshot.properties.zipWithIndex)
        propertiesOffs(i) = persistence.Property.createProperty(
          builder,
          builder.createString(propKey.name),
          persistence.Property.createValueVector(builder, propVal.serialized)
        )
      persistence.NodeSnapshot.createPropertiesVector(builder, propertiesOffs)
    }

    val edges: Offset = {
      val edgesArray = snapshot.edges.map(writeHalfEdge(builder, _)).toArray
      persistence.NodeSnapshot.createEdgesVector(builder, edgesArray)
    }

    val subscribers: Offset =
      if (snapshot.subscribersToThisNode.isEmpty) NoOffset
      else {
        val subscribersOffs: Array[Offset] = new Array[Offset](snapshot.subscribersToThisNode.size)
        for (
          (
            (
              node,
              DomainNodeIndexBehavior.SubscribersToThisNodeUtil.Subscription(
                notifiables,
                lastNotification,
                relatedQueries
              )
            ),
            i
          ) <- snapshot.subscribersToThisNode.zipWithIndex
        ) {
          val notifiableTypes: Array[Byte] = new Array[Byte](notifiables.size)
          val notifiableOffsets: Array[Offset] = new Array[Offset](notifiables.size)
          for ((notifiable, i) <- notifiables.zipWithIndex)
            notifiable match {
              case Left(nodeId) =>
                notifiableTypes(i) = persistence.Notifiable.QuineId
                notifiableOffsets(i) = writeQuineId(builder, nodeId)

              case Right(standingQueryId) =>
                notifiableTypes(i) = persistence.Notifiable.StandingQueryId
                notifiableOffsets(i) = writeStandingQueryId(builder, standingQueryId)
            }

          val notifiableType = persistence.Subscriber.createNotifiableTypeVector(builder, notifiableTypes)
          val notifiableOffset = persistence.Subscriber.createNotifiableVector(builder, notifiableOffsets)
          val lastNotificationEnum: Byte = lastNotification match {
            case None => persistence.LastNotification.None
            case Some(false) => persistence.LastNotification.False
            case Some(true) => persistence.LastNotification.True
          }

          val relatedQueriesOffsets = new Array[Offset](relatedQueries.size)
          for ((relatedQueries, i) <- relatedQueries.zipWithIndex)
            relatedQueriesOffsets(i) = writeStandingQueryId(builder, relatedQueries)
          val relatedQueriesOffset = persistence.Subscriber.createRelatedQueriesVector(builder, relatedQueriesOffsets)

          subscribersOffs(i) = persistence.Subscriber.createSubscriber(
            builder,
            node,
            notifiableType,
            notifiableOffset,
            lastNotificationEnum,
            relatedQueriesOffset
          )
        }
        persistence.NodeSnapshot.createSubscribersVector(builder, subscribersOffs)
      }

    val domainNodeIndex: Offset =
      if (snapshot.domainNodeIndex.isEmpty) NoOffset
      else {
        val domainNodeIndexOffs: Array[Offset] = new Array[Offset](snapshot.domainNodeIndex.size)
        for (((subscriberId, results), i) <- snapshot.domainNodeIndex.zipWithIndex) {
          val subscriberOff: Offset = writeQuineId(builder, subscriberId)
          val queries: Offset = {
            val queriesOffs: Array[Offset] = new Array[Offset](results.size)
            for (((branch, result), i) <- results.zipWithIndex) {
              val lastNotificationEnum: Byte = result match {
                case None => persistence.LastNotification.None
                case Some(false) => persistence.LastNotification.False
                case Some(true) => persistence.LastNotification.True
              }

              queriesOffs(i) = persistence.NodeIndexQuery.createNodeIndexQuery(
                builder,
                branch,
                lastNotificationEnum
              )
            }
            persistence.NodeIndex.createQueriesVector(builder, queriesOffs)
          }

          domainNodeIndexOffs(i) = persistence.NodeIndex.createNodeIndex(
            builder,
            subscriberOff,
            queries
          )
        }
        persistence.NodeSnapshot.createDomainNodeIndexVector(builder, domainNodeIndexOffs)
      }

    persistence.NodeSnapshot.createNodeSnapshot(
      builder,
      time,
      properties,
      edges,
      subscribers,
      domainNodeIndex
    )
  }

  private[this] def readNodeSnapshot(snapshot: persistence.NodeSnapshot): NodeSnapshot = {
    val time = EventTime.fromRaw(snapshot.time)
    val properties: Map[Symbol, PropertyValue] = {
      val builder = Map.newBuilder[Symbol, PropertyValue]
      var i: Int = 0
      val propertiesLength: Int = snapshot.propertiesLength
      while (i < propertiesLength) {
        val property: persistence.Property = snapshot.properties(i)
        builder += Symbol(property.key) -> PropertyValue.fromBytes(property.valueAsByteBuffer.remainingBytes)
        i += 1
      }
      builder.result()
    }

    val edges: Iterable[HalfEdge] = new AbstractIterable[HalfEdge] {
      def iterator: Iterator[HalfEdge] = Iterator.tabulate(snapshot.edgesLength)(i => readHalfEdge(snapshot.edges(i)))
    }

    val subscribersToThisNode = {
      val builder = mutable.Map.empty[
        DomainGraphNodeId,
        DomainNodeIndexBehavior.SubscribersToThisNodeUtil.Subscription
      ]
      var i: Int = 0
      val subscribersLength = snapshot.subscribersLength
      while (i < subscribersLength) {
        val subscriber: persistence.Subscriber = snapshot.subscribers(i)
        val dgnId = subscriber.dgnId
        val notifiables = mutable.Set.empty[Notifiable]
        var j: Int = 0
        val notifiableLength = subscriber.notifiableLength
        while (j < notifiableLength) {
          val notifiable = subscriber.notifiableType(j) match {
            case persistence.Notifiable.QuineId =>
              Left(
                readQuineId(
                  subscriber.notifiable(new persistence.QuineId(), j).asInstanceOf[persistence.QuineId]
                )
              )

            case persistence.Notifiable.StandingQueryId =>
              Right(
                readStandingQueryId(
                  subscriber.notifiable(new persistence.StandingQueryId(), j).asInstanceOf[persistence.StandingQueryId]
                )
              )

            case other =>
              throw new InvalidUnionType(other, persistence.Notifiable.names)
          }
          notifiables += notifiable
          j += 1
        }
        val lastNotification: Option[Boolean] = subscriber.lastNotification match {
          case persistence.LastNotification.None => None
          case persistence.LastNotification.False => Some(false)
          case persistence.LastNotification.True => Some(true)
          case other => throw new InvalidUnionType(other, persistence.LastNotification.names)
        }

        val relatedQueries = mutable.Set.empty[StandingQueryId]
        var k: Int = 0
        val relatedQueriesLength = subscriber.relatedQueriesLength
        while (k < relatedQueriesLength) {
          relatedQueries += readStandingQueryId(subscriber.relatedQueries(k))
          k += 1
        }

        builder += dgnId -> DomainNodeIndexBehavior.SubscribersToThisNodeUtil.Subscription(
          notifiables.toSet,
          lastNotification,
          relatedQueries.toSet
        )
        i += 1
      }
      builder
    }

    val domainNodeIndex = {
      val builder = mutable.Map.empty[
        QuineId,
        mutable.Map[DomainGraphNodeId, Option[Boolean]]
      ]

      var i: Int = 0
      val domainNodeIndexLength = snapshot.domainNodeIndexLength
      while (i < domainNodeIndexLength) {
        val nodeIndex: persistence.NodeIndex = snapshot.domainNodeIndex(i)
        val subscriber = readQuineId(nodeIndex.subscriber)
        val results = mutable.Map.empty[DomainGraphNodeId, Option[Boolean]]
        var j: Int = 0
        val queriesLength = nodeIndex.queriesLength
        while (j < queriesLength) {
          val query = nodeIndex.queries(j)
          val result = query.result match {
            case persistence.LastNotification.None => None
            case persistence.LastNotification.False => Some(false)
            case persistence.LastNotification.True => Some(true)
            case other => throw new InvalidUnionType(other, persistence.LastNotification.names)
          }
          results += query.dgnId -> result
          j += 1
        }
        builder += subscriber -> results
        i += 1
      }

      builder
    }

    NodeSnapshot(
      time,
      properties,
      edges,
      subscribersToThisNode,
      domainNodeIndex
    )
  }

  private[this] def writeCypherCrossStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery.Cross
  ): Offset = {
    val queriesTyps: Array[Byte] = new Array[Byte](query.queries.length)
    val queriesOffs: Array[Offset] = new Array[Offset](query.queries.length)
    for ((subQuery, i) <- query.queries.zipWithIndex) {
      val TypeAndOffset(subQueryTyp, subQueryOff) = writeCypherStandingQuery(builder, subQuery)
      queriesTyps(i) = subQueryTyp
      queriesOffs(i) = subQueryOff
    }
    persistence.CypherCrossStandingQuery.createCypherCrossStandingQuery(
      builder,
      persistence.CypherCrossStandingQuery.createQueriesTypeVector(builder, queriesTyps),
      persistence.CypherCrossStandingQuery.createQueriesVector(builder, queriesOffs),
      query.emitSubscriptionsLazily
    )
  }

  private[this] def readCypherCrossStandingQuery(
    query: persistence.CypherCrossStandingQuery
  ): cypher.StandingQuery.Cross = {
    var i = 0
    val queriesLength = query.queriesLength
    val queries: Array[cypher.StandingQuery] = new Array[cypher.StandingQuery](queriesLength)
    while (i < queriesLength) {
      queries(i) = readCypherStandingQuery(query.queriesType(i), query.queries(_, i))
      i += 1
    }
    cypher.StandingQuery.Cross(
      ArraySeq.unsafeWrapArray(queries),
      query.emitSubscriptionsLazily
    )
  }

  private[this] def writeCypherValueConstraint(
    builder: FlatBufferBuilder,
    constraint: cypher.StandingQuery.LocalProperty.ValueConstraint
  ): TypeAndOffset =
    constraint match {
      case cypher.StandingQuery.LocalProperty.Equal(value) =>
        val TypeAndOffset(compareToTyp, compareToOff) = writeCypherValue(builder, value)
        val off = persistence.CypherValueConstraintEqual.createCypherValueConstraintEqual(
          builder,
          compareToTyp,
          compareToOff
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintEqual, off)

      case cypher.StandingQuery.LocalProperty.NotEqual(value) =>
        val TypeAndOffset(compareToTyp, compareToOff) = writeCypherValue(builder, value)
        val off = persistence.CypherValueConstraintNotEqual.createCypherValueConstraintNotEqual(
          builder,
          compareToTyp,
          compareToOff
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintNotEqual, off)

      case cypher.StandingQuery.LocalProperty.Any =>
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintAny, emptyTable(builder))

      case cypher.StandingQuery.LocalProperty.None =>
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintNone, emptyTable(builder))

      case cypher.StandingQuery.LocalProperty.Regex(pattern) =>
        val patternOff = builder.createString(pattern)
        val off = persistence.CypherValueConstraintRegex.createCypherValueConstraintRegex(
          builder,
          patternOff
        )
        TypeAndOffset(persistence.CypherValueConstraint.CypherValueConstraintRegex, off)

      case cypher.StandingQuery.LocalProperty.ListContains(values) =>
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
  ): cypher.StandingQuery.LocalProperty.ValueConstraint =
    typ match {
      case persistence.CypherValueConstraint.CypherValueConstraintEqual =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintEqual())
          .asInstanceOf[persistence.CypherValueConstraintEqual]
        val value = readCypherValue(cons.compareToType, cons.compareTo(_))
        cypher.StandingQuery.LocalProperty.Equal(value)

      case persistence.CypherValueConstraint.CypherValueConstraintNotEqual =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintNotEqual())
          .asInstanceOf[persistence.CypherValueConstraintNotEqual]
        val value = readCypherValue(cons.compareToType, cons.compareTo(_))
        cypher.StandingQuery.LocalProperty.NotEqual(value)

      case persistence.CypherValueConstraint.CypherValueConstraintAny =>
        cypher.StandingQuery.LocalProperty.Any

      case persistence.CypherValueConstraint.CypherValueConstraintNone =>
        cypher.StandingQuery.LocalProperty.None

      case persistence.CypherValueConstraint.CypherValueConstraintRegex =>
        val cons = makeValueConstraint(new persistence.CypherValueConstraintRegex())
          .asInstanceOf[persistence.CypherValueConstraintRegex]
        val pattern = cons.pattern
        cypher.StandingQuery.LocalProperty.Regex(pattern)

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
        cypher.StandingQuery.LocalProperty.ListContains(values)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherValueConstraint.names)
    }

  private[this] def writeCypherLocalPropertyStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery.LocalProperty
  ): Offset = {
    val propertyKeyOff: Offset = builder.createString(query.propKey.name)
    val TypeAndOffset(consTyp, consOff) = writeCypherValueConstraint(builder, query.propConstraint)
    val aliasedAsOff: Offset = query.aliasedAs match {
      case None => NoOffset
      case Some(an) => builder.createString(an.name)
    }
    persistence.CypherLocalPropertyStandingQuery.createCypherLocalPropertyStandingQuery(
      builder,
      propertyKeyOff,
      consTyp,
      consOff,
      aliasedAsOff
    )
  }

  private[this] def readCypherLocalPropertyStandingQuery(
    query: persistence.CypherLocalPropertyStandingQuery
  ): cypher.StandingQuery.LocalProperty = {
    val propertyKey = Symbol(query.propertyKey)
    val propertyConstraint = readCypherValueConstraint(query.propertyConstraintType, query.propertyConstraint(_))
    val aliasedAs = Option(query.aliasedAs).map(Symbol.apply)
    cypher.StandingQuery.LocalProperty(propertyKey, propertyConstraint, aliasedAs)
  }

  private[this] def readCypherLocalIdStandingQuery(
    query: persistence.CypherLocalIdStandingQuery
  ): cypher.StandingQuery.LocalId =
    cypher.StandingQuery.LocalId(Symbol(query.aliasedAs), query.formatAsString)

  private[this] def writeCypherLocalIdStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery.LocalId
  ): Offset = {
    val aliasedAsOff: Offset = builder.createString(query.aliasedAs.name)
    persistence.CypherLocalIdStandingQuery.createCypherLocalIdStandingQuery(
      builder,
      aliasedAsOff,
      query.formatAsString
    )
  }

  private[this] def writeCypherSubscribeAcrossEdgeStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery.SubscribeAcrossEdge
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
    val TypeAndOffset(andThenTyp, andThenOff) = writeCypherStandingQuery(builder, query.andThen)
    persistence.CypherSubscribeAcrossEdgeStandingQuery.createCypherSubscribeAcrossEdgeStandingQuery(
      builder,
      edgeNameOff,
      edgeDirOff,
      andThenTyp,
      andThenOff
    )
  }

  private[this] def readCypherSubscribeAcrossEdgeStandingQuery(
    query: persistence.CypherSubscribeAcrossEdgeStandingQuery
  ): cypher.StandingQuery.SubscribeAcrossEdge = {
    val edgeName: Option[Symbol] = Option(query.edgeName).map(Symbol.apply)
    val edgeDirection: Option[EdgeDirection] = Option(query.edgeDirection).map { dir =>
      dir.edgeDirection match {
        case persistence.EdgeDirection.Outgoing => EdgeDirection.Outgoing
        case persistence.EdgeDirection.Incoming => EdgeDirection.Incoming
        case persistence.EdgeDirection.Undirected => EdgeDirection.Undirected
        case other => throw new InvalidUnionType(other, persistence.EdgeDirection.names)
      }
    }
    val andThen: cypher.StandingQuery = readCypherStandingQuery(query.andThenType, query.andThen(_))
    cypher.StandingQuery.SubscribeAcrossEdge(edgeName, edgeDirection, andThen)
  }

  private[this] def writeCypherEdgeSubscriptionReciprocalStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery.EdgeSubscriptionReciprocal
  ): Offset = {
    val halfEdgeOff: Offset = writeHalfEdge(builder, query.halfEdge)
    val andThenIdOff: Offset = writeStandingQueryPartId(builder, query.andThenId)
    persistence.CypherEdgeSubscriptionReciprocalStandingQuery.createCypherEdgeSubscriptionReciprocalStandingQuery(
      builder,
      halfEdgeOff,
      andThenIdOff
    )
  }

  private[this] def readCypherEdgeSubscriptionReciprocalStandingQuery(
    query: persistence.CypherEdgeSubscriptionReciprocalStandingQuery
  ): cypher.StandingQuery.EdgeSubscriptionReciprocal = {
    val halfEdge: HalfEdge = readHalfEdge(query.halfEdge)
    val andThenId: StandingQueryPartId = readStandingQueryPartId(query.andThenId)
    cypher.StandingQuery.EdgeSubscriptionReciprocal(halfEdge, andThenId)
  }

  private[this] def writeCypherFilterMapStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery.FilterMap
  ): Offset = {
    val TypeAndOffset(condTyp, condOff) = query.condition match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(cond) => writeCypherExpr(builder, cond)
    }
    val TypeAndOffset(toFilterTyp, toFilterOff) = writeCypherStandingQuery(builder, query.toFilter)
    val toAddOff: Offset = {
      val toAddOffs: Array[Offset] = new Array[Offset](query.toAdd.size)
      for (((key, valueExpr), i) <- query.toAdd.zipWithIndex) {
        val keyOff = builder.createString(key.name)
        val TypeAndOffset(valueTyp, valueOff) = writeCypherExpr(builder, valueExpr)
        toAddOffs(i) = persistence.CypherMapExprEntry.createCypherMapExprEntry(builder, keyOff, valueTyp, valueOff)
      }
      persistence.CypherFilterMapStandingQuery.createToAddVector(builder, toAddOffs)
    }
    persistence.CypherFilterMapStandingQuery.createCypherFilterMapStandingQuery(
      builder,
      condTyp,
      condOff,
      toFilterTyp,
      toFilterOff,
      query.dropExisting,
      toAddOff
    )
  }

  private[this] def readCypherFilterMapStandingQuery(
    query: persistence.CypherFilterMapStandingQuery
  ): cypher.StandingQuery.FilterMap = {
    val condition: Option[cypher.Expr] =
      if (query.conditionType == persistence.CypherExpr.NONE) None
      else Some(readCypherExpr(query.conditionType, query.condition(_)))
    val toFilter: cypher.StandingQuery = readCypherStandingQuery(query.toFilterType, query.toFilter)
    val toAdd: List[(Symbol, cypher.Expr)] = {
      val builder = List.newBuilder[(Symbol, cypher.Expr)]
      var i = 0
      val toAddLength = query.toAddLength
      while (i < toAddLength) {
        val entry: persistence.CypherMapExprEntry = query.toAdd(i)
        val key: Symbol = Symbol(entry.key)
        val value: cypher.Expr = readCypherExpr(entry.valueType, entry.value(_))
        builder += key -> value
        i += 1
      }
      builder.result()
    }
    cypher.StandingQuery.FilterMap(condition, toFilter, query.dropExisting, toAdd)
  }

  private[this] def writeCypherStandingQuery(
    builder: FlatBufferBuilder,
    query: cypher.StandingQuery
  ): TypeAndOffset =
    query match {
      case _: cypher.StandingQuery.UnitSq =>
        TypeAndOffset(persistence.CypherStandingQuery.CypherUnitStandingQuery, emptyTable(builder))

      case cross: cypher.StandingQuery.Cross =>
        val offset: Offset = writeCypherCrossStandingQuery(builder, cross)
        TypeAndOffset(persistence.CypherStandingQuery.CypherCrossStandingQuery, offset)

      case localProp: cypher.StandingQuery.LocalProperty =>
        val offset: Offset = writeCypherLocalPropertyStandingQuery(builder, localProp)
        TypeAndOffset(persistence.CypherStandingQuery.CypherLocalPropertyStandingQuery, offset)

      case localId: cypher.StandingQuery.LocalId =>
        val offset: Offset = writeCypherLocalIdStandingQuery(builder, localId)
        TypeAndOffset(persistence.CypherStandingQuery.CypherLocalIdStandingQuery, offset)

      case subscriber: cypher.StandingQuery.SubscribeAcrossEdge =>
        val offset: Offset = writeCypherSubscribeAcrossEdgeStandingQuery(builder, subscriber)
        TypeAndOffset(persistence.CypherStandingQuery.CypherSubscribeAcrossEdgeStandingQuery, offset)

      case reciprocal: cypher.StandingQuery.EdgeSubscriptionReciprocal =>
        val offset: Offset = writeCypherEdgeSubscriptionReciprocalStandingQuery(builder, reciprocal)
        TypeAndOffset(persistence.CypherStandingQuery.CypherEdgeSubscriptionReciprocalStandingQuery, offset)

      case filterMap: cypher.StandingQuery.FilterMap =>
        val offset: Offset = writeCypherFilterMapStandingQuery(builder, filterMap)
        TypeAndOffset(persistence.CypherStandingQuery.CypherFilterMapStandingQuery, offset)

    }

  private[this] def readCypherStandingQuery(
    typ: Byte,
    makeSq: Table => Table
  ): cypher.StandingQuery =
    typ match {
      case persistence.CypherStandingQuery.CypherUnitStandingQuery =>
        cypher.StandingQuery.UnitSq()

      case persistence.CypherStandingQuery.CypherCrossStandingQuery =>
        val cross =
          makeSq(new persistence.CypherCrossStandingQuery()).asInstanceOf[persistence.CypherCrossStandingQuery]
        readCypherCrossStandingQuery(cross)

      case persistence.CypherStandingQuery.CypherLocalPropertyStandingQuery =>
        val localProp = makeSq(new persistence.CypherLocalPropertyStandingQuery())
          .asInstanceOf[persistence.CypherLocalPropertyStandingQuery]
        readCypherLocalPropertyStandingQuery(localProp)

      case persistence.CypherStandingQuery.CypherLocalIdStandingQuery =>
        val localId =
          makeSq(new persistence.CypherLocalIdStandingQuery()).asInstanceOf[persistence.CypherLocalIdStandingQuery]
        readCypherLocalIdStandingQuery(localId)

      case persistence.CypherStandingQuery.CypherSubscribeAcrossEdgeStandingQuery =>
        val subscribeAcrossEdge = makeSq(new persistence.CypherSubscribeAcrossEdgeStandingQuery())
          .asInstanceOf[persistence.CypherSubscribeAcrossEdgeStandingQuery]
        readCypherSubscribeAcrossEdgeStandingQuery(subscribeAcrossEdge)

      case persistence.CypherStandingQuery.CypherEdgeSubscriptionReciprocalStandingQuery =>
        val reciprocal = makeSq(new persistence.CypherEdgeSubscriptionReciprocalStandingQuery())
          .asInstanceOf[persistence.CypherEdgeSubscriptionReciprocalStandingQuery]
        readCypherEdgeSubscriptionReciprocalStandingQuery(reciprocal)

      case persistence.CypherStandingQuery.CypherFilterMapStandingQuery =>
        val filterMap =
          makeSq(new persistence.CypherFilterMapStandingQuery()).asInstanceOf[persistence.CypherFilterMapStandingQuery]
        readCypherFilterMapStandingQuery(filterMap)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherStandingQuery.names)
    }

  private[this] def writeQueryContext(
    builder: FlatBufferBuilder,
    qc: cypher.QueryContext
  ): Offset = {
    val env = qc.environment
    val columnOffs: Array[Offset] = new Array[Offset](env.size)
    val valueTypOffs: Array[Byte] = new Array[Byte](env.size)
    val valueOffs: Array[Offset] = new Array[Offset](env.size)
    for (((col, value), i) <- env.zipWithIndex) {
      val TypeAndOffset(valueTyp, valueoff) = writeCypherValue(builder, value)
      columnOffs(i) = builder.createString(col.name)
      valueTypOffs(i) = valueTyp
      valueOffs(i) = valueoff
    }
    val columnsOff = persistence.QueryContext.createColumnsVector(builder, columnOffs)
    val valueTypsOff = persistence.QueryContext.createValuesTypeVector(builder, valueTypOffs)
    val valuesOff: Offset = persistence.QueryContext.createValuesVector(builder, valueOffs)
    persistence.QueryContext.createQueryContext(builder, columnsOff, valueTypsOff, valuesOff)
  }

  private[this] def readQueryContext(qc: persistence.QueryContext): cypher.QueryContext = {
    val env = Map.newBuilder[Symbol, cypher.Value]
    var i = 0
    val columnsLength = qc.columnsLength
    assert(qc.valuesLength == qc.columnsLength, "columns and values must have the same length")
    while (i < columnsLength) {
      val column = Symbol(qc.columns(i))
      val value = readCypherValue(qc.valuesType(i), qc.values(_, i))
      env += column -> value
      i += 1
    }
    cypher.QueryContext(env.result())
  }

  private[this] def writeNodePatternPropertyValuePattern(
    builder: FlatBufferBuilder,
    pattern: GraphQueryPattern.PropertyValuePattern
  ): TypeAndOffset =
    pattern match {
      case GraphQueryPattern.PropertyValuePattern.Value(value) =>
        val compareToOff = writeQuineValue(builder, value)
        val off = persistence.NodePatternPropertyValue.createNodePatternPropertyValue(
          builder,
          compareToOff
        )
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyValue, off)

      case GraphQueryPattern.PropertyValuePattern.AnyValueExcept(value) =>
        val compareToOff = writeQuineValue(builder, value)
        val off = persistence.NodePatternPropertyAnyValueExcept.createNodePatternPropertyAnyValueExcept(
          builder,
          compareToOff
        )
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValueExcept, off)

      case GraphQueryPattern.PropertyValuePattern.AnyValue =>
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValue, emptyTable(builder))

      case GraphQueryPattern.PropertyValuePattern.NoValue =>
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyNoValue, emptyTable(builder))

      case GraphQueryPattern.PropertyValuePattern.RegexMatch(pattern) =>
        val patternOff = builder.createString(pattern.pattern)
        val off = persistence.NodePatternPropertyRegexMatch.createNodePatternPropertyRegexMatch(
          builder,
          patternOff
        )
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyRegexMatch, off)
    }

  private[this] def readNodePatternPropertyValuePattern(
    typ: Byte,
    makeValueConstraint: Table => Table
  ): GraphQueryPattern.PropertyValuePattern =
    typ match {
      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyValue =>
        val cons = makeValueConstraint(new persistence.NodePatternPropertyValue())
          .asInstanceOf[persistence.NodePatternPropertyValue]
        val value = readQuineValue(cons.compareTo)
        GraphQueryPattern.PropertyValuePattern.Value(value)

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValueExcept =>
        val cons = makeValueConstraint(new persistence.NodePatternPropertyAnyValueExcept())
          .asInstanceOf[persistence.NodePatternPropertyAnyValueExcept]
        val value = readQuineValue(cons.compareTo)
        GraphQueryPattern.PropertyValuePattern.AnyValueExcept(value)

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValue =>
        GraphQueryPattern.PropertyValuePattern.AnyValue

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyNoValue =>
        GraphQueryPattern.PropertyValuePattern.NoValue

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyRegexMatch =>
        val cons = makeValueConstraint(new persistence.NodePatternPropertyRegexMatch())
          .asInstanceOf[persistence.NodePatternPropertyRegexMatch]
        val pattern = cons.pattern
        GraphQueryPattern.PropertyValuePattern.RegexMatch(Pattern.compile(pattern))

      case other =>
        throw new InvalidUnionType(other, persistence.NodePatternPropertyValuePattern.names)
    }

  private[this] def writeNodePattern(
    builder: FlatBufferBuilder,
    nodePattern: GraphQueryPattern.NodePattern
  ): Offset = {
    val labelsOff: Offset = {
      val labelOffs: Array[Offset] = new Array[Offset](nodePattern.labels.size)
      for ((label, i) <- nodePattern.labels.zipWithIndex)
        labelOffs(i) = builder.createString(label.name)
      persistence.NodePattern.createLabelsVector(builder, labelOffs)
    }
    val quineIdOff: Offset = nodePattern.qidOpt match {
      case None => NoOffset
      case Some(qid) => writeQuineId(builder, qid)
    }
    val propertiesOff: Offset = {
      val propertyOffs: Array[Offset] = new Array[Offset](nodePattern.properties.size)
      for (((propKey, propPat), i) <- nodePattern.properties.zipWithIndex) {
        val keyOff: Offset = builder.createString(propKey.name)
        val TypeAndOffset(patTyp, patOff) = writeNodePatternPropertyValuePattern(builder, propPat)
        propertyOffs(i) = persistence.NodePatternProperty.createNodePatternProperty(builder, keyOff, patTyp, patOff)
      }
      persistence.NodePattern.createPropertiesVector(builder, propertyOffs)
    }
    persistence.NodePattern.startNodePattern(builder)
    val patternIdOff: Offset = persistence.NodePatternId.createNodePatternId(
      builder,
      nodePattern.id.id
    )
    persistence.NodePattern.addPatternId(builder, patternIdOff)
    persistence.NodePattern.addLabels(builder, labelsOff)
    persistence.NodePattern.addQuineId(builder, quineIdOff)
    persistence.NodePattern.addProperties(builder, propertiesOff)
    persistence.NodePattern.endNodePattern(builder)
  }

  private[this] def readNodePattern(
    nodePattern: persistence.NodePattern
  ): GraphQueryPattern.NodePattern = {
    val labels: Set[Symbol] = {
      val builder = Set.newBuilder[Symbol]
      var i = 0
      val labelsLength = nodePattern.labelsLength
      while (i < labelsLength) {
        builder += Symbol(nodePattern.labels(i))
        i += 1
      }
      builder.result()
    }
    val quineIdOpt: Option[QuineId] = Option(nodePattern.quineId).map(readQuineId)
    val properties: Map[Symbol, GraphQueryPattern.PropertyValuePattern] = {
      val builder = Map.newBuilder[Symbol, GraphQueryPattern.PropertyValuePattern]
      var i = 0
      val propertiesLength = nodePattern.propertiesLength
      while (i < propertiesLength) {
        val property: persistence.NodePatternProperty = nodePattern.properties(i)
        val pattern = readNodePatternPropertyValuePattern(property.patternType, property.pattern(_))
        builder += Symbol(property.key) -> pattern
        i += 1
      }
      builder.result()
    }
    GraphQueryPattern.NodePattern(
      GraphQueryPattern.NodePatternId(nodePattern.patternId.id),
      labels,
      quineIdOpt,
      properties
    )
  }

  private[this] def writeEdgePattern(
    builder: FlatBufferBuilder,
    edgePattern: GraphQueryPattern.EdgePattern
  ): Offset = {
    val labelOff = builder.createString(edgePattern.label.name)
    persistence.EdgePattern.startEdgePattern(builder)
    val fromIdOff: Offset = persistence.NodePatternId.createNodePatternId(
      builder,
      edgePattern.from.id
    )
    persistence.EdgePattern.addFrom(builder, fromIdOff)
    val toIdOff: Offset = persistence.NodePatternId.createNodePatternId(
      builder,
      edgePattern.to.id
    )
    persistence.EdgePattern.addTo(builder, toIdOff)
    persistence.EdgePattern.addIsDirected(builder, edgePattern.isDirected)
    persistence.EdgePattern.addLabel(builder, labelOff)
    persistence.EdgePattern.endEdgePattern(builder)
  }

  private[this] def readEdgePattern(
    edgePattern: persistence.EdgePattern
  ): GraphQueryPattern.EdgePattern =
    GraphQueryPattern.EdgePattern(
      GraphQueryPattern.NodePatternId(edgePattern.from.id),
      GraphQueryPattern.NodePatternId(edgePattern.to.id),
      edgePattern.isDirected,
      Symbol(edgePattern.label)
    )

  private[this] def writeReturnColumn(
    builder: FlatBufferBuilder,
    returnCol: GraphQueryPattern.ReturnColumn
  ): TypeAndOffset =
    returnCol match {
      case GraphQueryPattern.ReturnColumn.Id(node, formatAsStr, aliasedAs) =>
        val aliasedAsOff: Offset = builder.createString(aliasedAs.name)
        persistence.ReturnColumnId.startReturnColumnId(builder)
        val nodeOff: Offset = persistence.NodePatternId.createNodePatternId(builder, node.id)
        persistence.ReturnColumnId.addNode(builder, nodeOff)
        persistence.ReturnColumnId.addFormatAsString(builder, formatAsStr)
        persistence.ReturnColumnId.addAliasedAs(builder, aliasedAsOff)
        val off: Offset = persistence.ReturnColumnId.endReturnColumnId(builder)
        TypeAndOffset(persistence.ReturnColumn.ReturnColumnId, off)

      case GraphQueryPattern.ReturnColumn.Property(node, propertyKey, aliasedAs) =>
        val propertyKeyOff: Offset = builder.createString(propertyKey.name)
        val aliasedAsOff: Offset = builder.createString(aliasedAs.name)
        persistence.ReturnColumnProperty.startReturnColumnProperty(builder)
        val nodeOff: Offset = persistence.NodePatternId.createNodePatternId(builder, node.id)
        persistence.ReturnColumnProperty.addNode(builder, nodeOff)
        persistence.ReturnColumnProperty.addPropertyKey(builder, propertyKeyOff)
        persistence.ReturnColumnProperty.addAliasedAs(builder, aliasedAsOff)
        val off: Offset = persistence.ReturnColumnProperty.endReturnColumnProperty(builder)
        TypeAndOffset(persistence.ReturnColumn.ReturnColumnProperty, off)
    }

  private[this] def readReturnColumn(
    typ: Byte,
    makeReturnCol: Table => Table
  ): GraphQueryPattern.ReturnColumn =
    typ match {
      case persistence.ReturnColumn.ReturnColumnId =>
        val col = makeReturnCol(new persistence.ReturnColumnId()).asInstanceOf[persistence.ReturnColumnId]
        GraphQueryPattern.ReturnColumn.Id(
          GraphQueryPattern.NodePatternId(col.node.id),
          col.formatAsString,
          Symbol(col.aliasedAs)
        )

      case persistence.ReturnColumn.ReturnColumnProperty =>
        val col = makeReturnCol(new persistence.ReturnColumnProperty()).asInstanceOf[persistence.ReturnColumnProperty]
        GraphQueryPattern.ReturnColumn.Property(
          GraphQueryPattern.NodePatternId(col.node.id),
          Symbol(col.propertyKey),
          Symbol(col.aliasedAs)
        )

      case other =>
        throw new InvalidUnionType(other, persistence.ReturnColumn.names)
    }

  private[this] def writeGraphQueryPattern(
    builder: FlatBufferBuilder,
    pattern: GraphQueryPattern
  ): Offset = {
    val nodesOff: Offset = {
      val nodeOffs: Array[Offset] = new Array(pattern.nodes.length)
      for ((node, i) <- pattern.nodes.zipWithIndex)
        nodeOffs(i) = writeNodePattern(builder, node)
      persistence.GraphQueryPattern.createNodesVector(builder, nodeOffs)
    }
    val edgesOff: Offset = {
      val edgeOffs: Array[Offset] = new Array(pattern.edges.length)
      for ((node, i) <- pattern.edges.zipWithIndex)
        edgeOffs(i) = writeEdgePattern(builder, node)
      persistence.GraphQueryPattern.createEdgesVector(builder, edgeOffs)
    }
    val (toExtractTypsOff, toExtractsOff) = {
      val toExtractTypOffs: Array[Byte] = new Array(pattern.toExtract.length)
      val toExtractOffs: Array[Offset] = new Array(pattern.toExtract.length)
      for ((col, i) <- pattern.toExtract.zipWithIndex) {
        val TypeAndOffset(colTyp, colOff) = writeReturnColumn(builder, col)
        toExtractTypOffs(i) = colTyp
        toExtractOffs(i) = colOff
      }
      val typs = persistence.GraphQueryPattern.createToExtractTypeVector(builder, toExtractTypOffs)
      val offs = persistence.GraphQueryPattern.createToExtractVector(builder, toExtractOffs)
      typs -> offs
    }
    val TypeAndOffset(filterCondTyp, filterCondOff) = pattern.filterCond match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(exp) => writeCypherExpr(builder, exp)
    }
    val toReturnsOff: Offset = {
      val toReturnOffs: Array[Offset] = new Array(pattern.toReturn.length)
      for (((returnAs, toReturn), i) <- pattern.toReturn.zipWithIndex) {
        val keyOff: Offset = builder.createString(returnAs.name)
        val TypeAndOffset(valueTyp, valueOff) = writeCypherExpr(builder, toReturn)
        toReturnOffs(i) = persistence.CypherMapExprEntry.createCypherMapExprEntry(
          builder,
          keyOff,
          valueTyp,
          valueOff
        )
      }
      persistence.GraphQueryPattern.createToReturnVector(builder, toReturnOffs)
    }
    persistence.GraphQueryPattern.startGraphQueryPattern(builder)
    persistence.GraphQueryPattern.addNodes(builder, nodesOff)
    persistence.GraphQueryPattern.addEdges(builder, edgesOff)
    val startingPointOffset: Offset = persistence.NodePatternId.createNodePatternId(builder, pattern.startingPoint.id)
    persistence.GraphQueryPattern.addStartingPoint(builder, startingPointOffset)
    persistence.GraphQueryPattern.addToExtractType(builder, toExtractTypsOff)
    persistence.GraphQueryPattern.addToExtract(builder, toExtractsOff)
    persistence.GraphQueryPattern.addFilterCondType(builder, filterCondTyp)
    persistence.GraphQueryPattern.addFilterCond(builder, filterCondOff)
    persistence.GraphQueryPattern.addToReturn(builder, toReturnsOff)
    persistence.GraphQueryPattern.addDistinct(builder, pattern.distinct)

    persistence.GraphQueryPattern.endGraphQueryPattern(builder)
  }

  private[this] def readGraphQueryPattern(
    pattern: persistence.GraphQueryPattern
  ): GraphQueryPattern = {
    val nodes: Seq[GraphQueryPattern.NodePattern] = {
      val builder = Seq.newBuilder[GraphQueryPattern.NodePattern]
      var i = 0
      val nodesLength = pattern.nodesLength
      while (i < nodesLength) {
        builder += readNodePattern(pattern.nodes(i))
        i += 1
      }
      builder.result()
    }
    val edges: Seq[GraphQueryPattern.EdgePattern] = {
      val builder = Seq.newBuilder[GraphQueryPattern.EdgePattern]
      var i = 0
      val edgesLength = pattern.edgesLength
      while (i < edgesLength) {
        builder += readEdgePattern(pattern.edges(i))
        i += 1
      }
      builder.result()
    }
    val startingPoint: GraphQueryPattern.NodePatternId = GraphQueryPattern.NodePatternId(pattern.startingPoint.id)
    val toExtract: Seq[GraphQueryPattern.ReturnColumn] = {
      val builder = Seq.newBuilder[GraphQueryPattern.ReturnColumn]
      var i = 0
      val toExtractLength = pattern.toExtractLength
      while (i < toExtractLength) {
        builder += readReturnColumn(pattern.toExtractType(i), pattern.toExtract(_, i))
        i += 1
      }
      builder.result()
    }
    val filterCond: Option[cypher.Expr] = pattern.filterCondType match {
      case persistence.CypherExpr.NONE => None
      case typ => Some(readCypherExpr(typ, pattern.filterCond(_)))
    }
    val toReturn: Seq[(Symbol, cypher.Expr)] = {
      val builder = Seq.newBuilder[(Symbol, cypher.Expr)]
      var i = 0
      val toReturnLength = pattern.toReturnLength
      while (i < toReturnLength) {
        val prop: persistence.CypherMapExprEntry = pattern.toReturn(i)
        val returnExp = readCypherExpr(prop.valueType, prop.value(_))
        builder += Symbol(prop.key) -> returnExp
        i += 1
      }
      builder.result()
    }
    val distinct: Boolean = pattern.distinct
    GraphQueryPattern(nodes, edges, startingPoint, toExtract, filterCond, toReturn, distinct)
  }

  private[this] def writeGraphPatternOrigin(
    builder: FlatBufferBuilder,
    graphPat: PatternOrigin.GraphPattern
  ): Offset = {
    val graphOff: Offset = writeGraphQueryPattern(builder, graphPat.pattern)
    val cypherOrigOff: Offset = graphPat.cypherOriginal match {
      case None => NoOffset
      case Some(original) => builder.createString(original)
    }
    persistence.GraphPatternOrigin.createGraphPatternOrigin(
      builder,
      graphOff,
      cypherOrigOff
    )
  }

  private[this] def readGraphPatternOrigin(
    graphPatOrigin: persistence.GraphPatternOrigin
  ): PatternOrigin.GraphPattern = {
    val graphPat = readGraphQueryPattern(graphPatOrigin.pattern)
    val originalCypher: Option[String] = Option(graphPatOrigin.cypherOriginal)
    PatternOrigin.GraphPattern(graphPat, originalCypher)
  }

  private[this] def writeBranchOrigin(
    builder: FlatBufferBuilder,
    origin: PatternOrigin.DgbOrigin
  ): TypeAndOffset =
    origin match {
      case PatternOrigin.DirectDgb =>
        TypeAndOffset(persistence.BranchOrigin.DirectDgb, NoOffset)

      case graphPat: PatternOrigin.GraphPattern =>
        val originOff: Offset = writeGraphPatternOrigin(builder, graphPat)
        TypeAndOffset(persistence.BranchOrigin.GraphPatternOrigin, originOff)
    }

  private[this] def readBranchOrigin(
    branchOriginTyp: Byte,
    makeBranchOrigin: Table => Table
  ): PatternOrigin.DgbOrigin =
    branchOriginTyp match {
      case persistence.BranchOrigin.DirectDgb =>
        PatternOrigin.DirectDgb

      case persistence.BranchOrigin.GraphPatternOrigin =>
        val graphPatOrigin = makeBranchOrigin(new persistence.GraphPatternOrigin())
          .asInstanceOf[persistence.GraphPatternOrigin]
        val readOrigin = readGraphPatternOrigin(graphPatOrigin)

        // DistinctId queries must include a `DISTINCT` keyword
        if (!readOrigin.pattern.distinct) {
          throw new InvalidPersistedQuineData(
            s"Detected an invalid DistinctId query pattern during deserialization. DistinctId queries must return a single `DISTINCT` value. Detected pattern was: `${readOrigin.cypherOriginal
              .getOrElse(readOrigin)}`"
          )
        }
        readOrigin

      case other =>
        throw new InvalidUnionType(other, persistence.BranchOrigin.names)
    }

  private[this] def writeSqV4Origin(
    builder: FlatBufferBuilder,
    origin: PatternOrigin.SqV4Origin
  ): TypeAndOffset =
    origin match {
      case PatternOrigin.DirectSqV4 =>
        TypeAndOffset(persistence.SqV4Origin.DirectSqV4, NoOffset)

      case graphPat: PatternOrigin.GraphPattern =>
        val originOff: Offset = writeGraphPatternOrigin(builder, graphPat)
        TypeAndOffset(persistence.SqV4Origin.GraphPatternOrigin, originOff)
    }

  private[this] def readSqV4Origin(
    branchOriginTyp: Byte,
    makeBranchOrigin: Table => Table
  ): PatternOrigin.SqV4Origin =
    branchOriginTyp match {
      case persistence.SqV4Origin.DirectSqV4 =>
        PatternOrigin.DirectSqV4

      case persistence.SqV4Origin.GraphPatternOrigin =>
        val graphPatOrigin = makeBranchOrigin(new persistence.GraphPatternOrigin())
          .asInstanceOf[persistence.GraphPatternOrigin]
        val readOrigin = readGraphPatternOrigin(graphPatOrigin)
        // MultipleValues queries do not yet support distinct (QU-568) so warn if that is requested
        if (readOrigin.pattern.distinct) {
          logger.warn(
            readOrigin.cypherOriginal match {
              case Some(cypherQuery) =>
                s"Read a GraphPattern for a MultipleValues query with a DISTINCT clause. This is not yet supported. Query was: '$cypherQuery'"
              case None =>
                s"Read a GraphPattern for a MultipleValues query that specifies `distinct`. This is not yet supported. Query pattern was: $readOrigin"
            }
          )
        }
        readOrigin

      case other =>
        throw new InvalidUnionType(other, persistence.SqV4Origin.names)
    }

  private[this] def writeDomainGraphNodeStandingQueryPattern(
    builder: FlatBufferBuilder,
    dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern
  ): Offset = {
    val aliasReturnAsOff: Offset = builder.createString(dgnPattern.aliasReturnAs.name)
    val TypeAndOffset(originTyp, originOff) = writeBranchOrigin(builder, dgnPattern.origin)
    persistence.BranchQuery.createBranchQuery(
      builder,
      dgnPattern.dgnId,
      dgnPattern.formatReturnAsStr,
      aliasReturnAsOff,
      dgnPattern.includeCancellation,
      originTyp,
      originOff
    )
  }

  private[this] def readDomainGraphNodeStandingQueryPattern(
    branchQuery: persistence.BranchQuery
  ): StandingQueryPattern.DomainGraphNodeStandingQueryPattern = {
    val origin = readBranchOrigin(branchQuery.originType, branchQuery.origin(_))
    StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
      branchQuery.dgnId,
      branchQuery.formatReturnAsString,
      Symbol(branchQuery.aliasReturnAs),
      branchQuery.includeCancellation,
      origin
    )
  }

  private[this] def writeSqV4StandingQuery(
    builder: FlatBufferBuilder,
    cypherQuery: StandingQueryPattern.SqV4
  ): Offset = {
    val TypeAndOffset(queryTyp, queryOff) = writeCypherStandingQuery(builder, cypherQuery.compiledQuery)
    val TypeAndOffset(originTyp, originOff) = writeSqV4Origin(builder, cypherQuery.origin)
    persistence.SqV4Query.createSqV4Query(
      builder,
      queryTyp,
      queryOff,
      cypherQuery.includeCancellation,
      originTyp,
      originOff
    )
  }

  private[this] def readSqV4StandingQuery(
    cypherQuery: persistence.SqV4Query
  ): StandingQueryPattern.SqV4 = {
    val query: cypher.StandingQuery = readCypherStandingQuery(cypherQuery.queryType, cypherQuery.query(_))
    val origin: PatternOrigin.SqV4Origin = readSqV4Origin(cypherQuery.originType, cypherQuery.origin(_))
    StandingQueryPattern.SqV4(query, cypherQuery.includeCancellation, origin)
  }

  private[this] def writeStandingQueryPattern(
    builder: FlatBufferBuilder,
    sqPat: StandingQueryPattern
  ): TypeAndOffset =
    sqPat match {
      case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
        val offset: Offset = writeDomainGraphNodeStandingQueryPattern(builder, dgnPattern)
        TypeAndOffset(persistence.StandingQueryPattern.BranchQuery, offset)

      case cypher: StandingQueryPattern.SqV4 =>
        val offset: Offset = writeSqV4StandingQuery(builder, cypher)
        TypeAndOffset(persistence.StandingQueryPattern.SqV4Query, offset)
    }

  private[this] def readStandingQueryPattern(
    typ: Byte,
    makeSQP: Table => Table
  ): StandingQueryPattern =
    typ match {
      case persistence.StandingQueryPattern.BranchQuery =>
        val branch = makeSQP(new persistence.BranchQuery()).asInstanceOf[persistence.BranchQuery]
        readDomainGraphNodeStandingQueryPattern(branch)

      case persistence.StandingQueryPattern.SqV4Query =>
        val sqv4 = makeSQP(new persistence.SqV4Query()).asInstanceOf[persistence.SqV4Query]
        readSqV4StandingQuery(sqv4)

      case other =>
        throw new InvalidUnionType(other, persistence.StandingQueryPattern.names)
    }

  private[this] def writeStandingQuery(
    builder: FlatBufferBuilder,
    standingQuery: StandingQuery
  ): Offset = {
    val nameOff: Offset = builder.createString(standingQuery.name)
    val idOff: Offset = writeStandingQueryId(builder, standingQuery.id)
    val TypeAndOffset(queryTyp, queryOff) = writeStandingQueryPattern(builder, standingQuery.query)
    persistence.StandingQuery.createStandingQuery(
      builder,
      nameOff,
      idOff,
      queryTyp,
      queryOff,
      standingQuery.queueBackpressureThreshold,
      standingQuery.queueMaxSize
    )
  }

  private[this] def readStandingQuery(
    standingQuery: persistence.StandingQuery
  ): StandingQuery = {
    val id: StandingQueryId = readStandingQueryId(standingQuery.id)
    val query: StandingQueryPattern = readStandingQueryPattern(standingQuery.queryType, standingQuery.query(_))
    StandingQuery(
      standingQuery.name,
      id,
      query,
      standingQuery.queueBackpressureThreshold,
      standingQuery.queueMaxSize,
      // do not support hash code on restored standing query,
      // because the hash code is calculated per-host and is not stored
      shouldCalculateResultHashCode = false
    )
  }

  private[this] def writeCypherUnitStandingQueryState(
    builder: FlatBufferBuilder,
    unitState: cypher.UnitState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, unitState.queryPartId)
    val resIdOff: Offset = unitState.resultId match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.CypherUnitStandingQueryState.createCypherUnitStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readCypherUnitStandingQueryState(
    unitState: persistence.CypherUnitStandingQueryState
  ): cypher.UnitState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(unitState.queryPartId)
    val resId: Option[ResultId] = Option(unitState.resultId).map(readStandingQueryResultId)
    cypher.UnitState(sqId, resId)
  }

  private[this] def writeCypherCrossStandingQueryState(
    builder: FlatBufferBuilder,
    crossState: cypher.CrossState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, crossState.queryPartId)
    val accumulatedResultsOff: Offset = {
      val accumulatedResultOffs: Array[Offset] = new Array(crossState.accumulatedResults.length)
      for ((accRes, i) <- crossState.accumulatedResults.zipWithIndex) {
        val resultsOff: Offset = {
          val resultOffs: Array[Offset] = new Array(accRes.size)
          for (((resId, qc), j) <- accRes.zipWithIndex) {
            val resIdOff: Offset = writeStandingQueryResultId(builder, resId)
            val resValueOff: Offset = writeQueryContext(builder, qc)
            resultOffs(j) = persistence.CypherStandingQueryResult.createCypherStandingQueryResult(
              builder,
              resIdOff,
              resValueOff
            )
          }
          persistence.AccumulatedResults.createResultVector(builder, resultOffs)
        }
        accumulatedResultOffs(i) = persistence.AccumulatedResults.createAccumulatedResults(
          builder,
          resultsOff
        )
      }
      persistence.CypherCrossStandingQueryState.createAccumulatedResultsVector(builder, accumulatedResultOffs)
    }
    val resultDependencyOff: Offset = {
      val resultDependencyOffs: Array[Offset] = new Array(crossState.resultDependency.size)
      for (((resId, depIds), i) <- crossState.resultDependency.zipWithIndex) {
        val resIdOff: Offset = writeStandingQueryResultId(builder, resId)
        val depsOff: Offset = {
          val depOffs: Array[Offset] = new Array[Offset](depIds.length)
          for ((depId, j) <- depIds.zipWithIndex)
            depOffs(j) = writeStandingQueryResultId(builder, depId)
          persistence.ResultDependency.createDependenciesVector(builder, depOffs)
        }
        resultDependencyOffs(i) = persistence.ResultDependency.createResultDependency(builder, resIdOff, depsOff)
      }
      persistence.CypherCrossStandingQueryState.createResultDependencyVector(builder, resultDependencyOffs)
    }
    persistence.CypherCrossStandingQueryState.createCypherCrossStandingQueryState(
      builder,
      sqIdOff,
      crossState.subscriptionsEmitted,
      accumulatedResultsOff,
      resultDependencyOff
    )
  }

  private[this] def readCypherCrossStandingQueryState(
    crossState: persistence.CypherCrossStandingQueryState
  ): cypher.CrossState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(crossState.queryPartId)
    val accumulatedResults: ArraySeq[mutable.Map[ResultId, cypher.QueryContext]] = {
      var i: Int = 0
      val accumulatedResultsLength = crossState.accumulatedResultsLength
      val underlying: Array[mutable.Map[ResultId, cypher.QueryContext]] = new Array(accumulatedResultsLength)
      while (i < accumulatedResultsLength) {
        val accRes: persistence.AccumulatedResults = crossState.accumulatedResults(i)
        var j: Int = 0
        val resultLength = accRes.resultLength
        val builder = mutable.Map.empty[ResultId, cypher.QueryContext]
        while (j < resultLength) {
          val qr: persistence.CypherStandingQueryResult = accRes.result(j)
          val resId: ResultId = readStandingQueryResultId(qr.resultId)
          val resValues: cypher.QueryContext = readQueryContext(qr.resultValues)
          builder += resId -> resValues
          j += 1
        }
        underlying(i) = builder
        i += 1
      }
      ArraySeq.unsafeWrapArray(underlying)
    }
    val resultDependency: mutable.Map[ResultId, List[ResultId]] = {
      val dependency = mutable.Map.empty[ResultId, List[ResultId]]
      var i: Int = 0
      val resultDependencyLength = crossState.resultDependencyLength
      while (i < resultDependencyLength) {
        val dep: persistence.ResultDependency = crossState.resultDependency(i)
        val resId: ResultId = readStandingQueryResultId(dep.resultId)
        val dependencies: List[ResultId] = {
          val builder = List.newBuilder[ResultId]
          var j: Int = 0
          val dependenciesLength = dep.dependenciesLength
          while (j < dependenciesLength) {
            builder += readStandingQueryResultId(dep.dependencies(j))
            j += 1
          }
          builder.result()
        }
        dependency += resId -> dependencies
        i += 1
      }
      dependency
    }
    cypher.CrossState(sqId, crossState.subscriptionsEmitted, accumulatedResults, resultDependency)
  }

  private[this] def writeCypherLocalPropertyStandingQueryState(
    builder: FlatBufferBuilder,
    localPropState: cypher.LocalPropertyState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, localPropState.queryPartId)
    val resIdOff: Offset = localPropState.currentResult match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.CypherLocalPropertyStandingQueryState.createCypherLocalPropertyStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readCypherLocalPropertyStandingQueryState(
    localPropState: persistence.CypherLocalPropertyStandingQueryState
  ): cypher.LocalPropertyState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(localPropState.queryPartId)
    val resId: Option[ResultId] = Option(localPropState.resultId).map(readStandingQueryResultId)
    cypher.LocalPropertyState(sqId, resId)
  }

  private[this] def writeCypherLocalIdStandingQueryState(
    builder: FlatBufferBuilder,
    localIdState: cypher.LocalIdState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, localIdState.queryPartId)
    val resIdOff: Offset = localIdState.resultId match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.CypherLocalIdStandingQueryState.createCypherLocalIdStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readCypherLocalIdStandingQueryState(
    localIdState: persistence.CypherLocalIdStandingQueryState
  ): cypher.LocalIdState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(localIdState.queryPartId)
    val resId: Option[ResultId] = Option(localIdState.resultId).map(readStandingQueryResultId)
    cypher.LocalIdState(sqId, resId)
  }

  private[this] def writeCypherSubscribeAcrossEdgeStandingQueryState(
    builder: FlatBufferBuilder,
    edgeState: cypher.SubscribeAcrossEdgeState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, edgeState.queryPartId)
    val edgesWatchedOff: Offset = {
      val edgeWatchedOffs: Array[Offset] = new Array(edgeState.edgesWatched.size)
      for (((he, (sqId, res)), i) <- edgeState.edgesWatched.zipWithIndex) {
        val halfEdgeOff = writeHalfEdge(builder, he)
        val sqIdOff = writeStandingQueryPartId(builder, sqId)
        val resOff: Offset = {
          val resOffs: Array[Offset] = new Array(res.size)
          for (((depId, (resId, qc)), i) <- res.zipWithIndex) {
            val depIdOff: Offset = writeStandingQueryResultId(builder, depId)
            val resIdOff: Offset = writeStandingQueryResultId(builder, resId)
            val qcOff: Offset = writeQueryContext(builder, qc)
            resOffs(i) = persistence.ReverseResultDependency.createReverseResultDependency(
              builder,
              depIdOff,
              resIdOff,
              qcOff
            )
          }
          persistence.EdgeWatched.createReverseResultDependencyVector(builder, resOffs)
        }
        edgeWatchedOffs(i) = persistence.EdgeWatched.createEdgeWatched(
          builder,
          halfEdgeOff,
          sqIdOff,
          resOff
        )
      }
      persistence.CypherSubscribeAcrossEdgeStandingQueryState.createEdgesWatchedVector(
        builder,
        edgeWatchedOffs
      )
    }
    persistence.CypherSubscribeAcrossEdgeStandingQueryState.createCypherSubscribeAcrossEdgeStandingQueryState(
      builder,
      sqIdOff,
      edgesWatchedOff
    )
  }

  private[this] def readCypherSubscribeAcrossEdgeStandingQueryState(
    edgeState: persistence.CypherSubscribeAcrossEdgeStandingQueryState
  ): cypher.SubscribeAcrossEdgeState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(edgeState.queryPartId)
    val edgesWatched: mutable.Map[
      HalfEdge,
      (StandingQueryPartId, mutable.Map[ResultId, (ResultId, cypher.QueryContext)])
    ] = mutable.Map.empty

    var i: Int = 0
    val edgesWatchedLength = edgeState.edgesWatchedLength
    while (i < edgesWatchedLength) {
      val edgeWatched: persistence.EdgeWatched = edgeState.edgesWatched(i)
      val halfEdge: HalfEdge = readHalfEdge(edgeWatched.halfEdge)
      val queryPartId: StandingQueryPartId = readStandingQueryPartId(edgeWatched.queryPartId)
      val reverseResultDependency: mutable.Map[ResultId, (ResultId, cypher.QueryContext)] = mutable.Map.empty

      var j: Int = 0
      val reverseResultDependencyLength = edgeWatched.reverseResultDependencyLength
      while (j < reverseResultDependencyLength) {
        val reverseResultDep: persistence.ReverseResultDependency = edgeWatched.reverseResultDependency(j)
        val depId: ResultId = readStandingQueryResultId(reverseResultDep.dependency)
        val resId: ResultId = readStandingQueryResultId(reverseResultDep.resultId)
        val resValues: cypher.QueryContext = readQueryContext(reverseResultDep.resultValues)
        reverseResultDependency += depId -> (resId -> resValues)
        j += 1
      }
      edgesWatched += halfEdge -> (queryPartId -> reverseResultDependency)
      i += 1
    }
    cypher.SubscribeAcrossEdgeState(sqId, edgesWatched)
  }

  private[this] def writeCypherEdgeSubscriptionReciprocalStandingQueryState(
    builder: FlatBufferBuilder,
    edgeState: cypher.EdgeSubscriptionReciprocalState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, edgeState.queryPartId)
    val halfEdgeOff: Offset = writeHalfEdge(builder, edgeState.halfEdge)
    val reverseDepsOff: Offset = {
      val reverseDepOffs: Array[Offset] = new Array(edgeState.reverseResultDependency.size)
      for (((depId, (resId, qc)), i) <- edgeState.reverseResultDependency.zipWithIndex) {
        val depIdOff: Offset = writeStandingQueryResultId(builder, depId)
        val resIdOff: Offset = writeStandingQueryResultId(builder, resId)
        val qcOff: Offset = writeQueryContext(builder, qc)
        reverseDepOffs(i) = persistence.ReverseResultDependency.createReverseResultDependency(
          builder,
          depIdOff,
          resIdOff,
          qcOff
        )
      }
      persistence.CypherEdgeSubscriptionReciprocalStandingQueryState.createReverseResultDependencyVector(
        builder,
        reverseDepOffs
      )
    }
    val andThenOff: Offset = writeStandingQueryPartId(builder, edgeState.andThenId)
    persistence.CypherEdgeSubscriptionReciprocalStandingQueryState
      .createCypherEdgeSubscriptionReciprocalStandingQueryState(
        builder,
        sqIdOff,
        halfEdgeOff,
        edgeState.currentlyMatching,
        reverseDepsOff,
        andThenOff
      )
  }

  private[this] def readCypherEdgeSubscriptionReciprocalStandingQueryState(
    edgeState: persistence.CypherEdgeSubscriptionReciprocalStandingQueryState
  ): cypher.EdgeSubscriptionReciprocalState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(edgeState.queryPartId)
    val halfEdge: HalfEdge = readHalfEdge(edgeState.halfEdge)
    val reverseDeps: mutable.Map[ResultId, (ResultId, cypher.QueryContext)] = {
      val builder = mutable.Map.empty[ResultId, (ResultId, cypher.QueryContext)]
      var i: Int = 0
      val reverseResultDependencyLength = edgeState.reverseResultDependencyLength
      while (i < reverseResultDependencyLength) {
        val reverseResultDep: persistence.ReverseResultDependency = edgeState.reverseResultDependency(i)
        val depId: ResultId = readStandingQueryResultId(reverseResultDep.dependency)
        val resId: ResultId = readStandingQueryResultId(reverseResultDep.resultId)
        val resValues: cypher.QueryContext = readQueryContext(reverseResultDep.resultValues)
        builder += depId -> (resId -> resValues)
        i += 1
      }
      builder
    }
    val andThenId: StandingQueryPartId = readStandingQueryPartId(edgeState.andThenId)
    cypher.EdgeSubscriptionReciprocalState(sqId, halfEdge, edgeState.currentlyMatching, reverseDeps, andThenId)
  }

  private[this] def writeCypherFilterMapStandingQueryState(
    builder: FlatBufferBuilder,
    filterState: cypher.FilterMapState
  ): Offset = {
    val sqIdOff: Offset = writeStandingQueryPartId(builder, filterState.queryPartId)
    val reverseDepsOff: Offset = {
      val reverseDepOffs: Array[Offset] = new Array(filterState.keptResults.size)
      for (((depId, (resId, qc)), i) <- filterState.keptResults.zipWithIndex) {
        val depIdOff: Offset = writeStandingQueryResultId(builder, depId)
        val resIdOff: Offset = writeStandingQueryResultId(builder, resId)
        val qcOff: Offset = writeQueryContext(builder, qc)
        reverseDepOffs(i) = persistence.ReverseResultDependency.createReverseResultDependency(
          builder,
          depIdOff,
          resIdOff,
          qcOff
        )
      }
      persistence.CypherFilterMapStandingQueryState.createReverseResultDependencyVector(
        builder,
        reverseDepOffs
      )
    }
    persistence.CypherFilterMapStandingQueryState.createCypherFilterMapStandingQueryState(
      builder,
      sqIdOff,
      reverseDepsOff
    )
  }

  private[this] def readCypherFilterMapStandingQueryState(
    filterState: persistence.CypherFilterMapStandingQueryState
  ): cypher.FilterMapState = {
    val sqId: StandingQueryPartId = readStandingQueryPartId(filterState.queryPartId)
    val reverseDeps: mutable.Map[ResultId, (ResultId, cypher.QueryContext)] = {
      val builder = mutable.Map.empty[ResultId, (ResultId, cypher.QueryContext)]
      var i: Int = 0
      val reverseResultDependencyLength = filterState.reverseResultDependencyLength
      while (i < reverseResultDependencyLength) {
        val reverseResultDep: persistence.ReverseResultDependency = filterState.reverseResultDependency(i)
        val depId: ResultId = readStandingQueryResultId(reverseResultDep.dependency)
        val resId: ResultId = readStandingQueryResultId(reverseResultDep.resultId)
        val resValues: cypher.QueryContext = readQueryContext(reverseResultDep.resultValues)
        builder += depId -> (resId -> resValues)
        i += 1
      }
      builder
    }
    cypher.FilterMapState(sqId, reverseDeps)
  }

  private[this] def writeCypherStandingQueryState(
    builder: FlatBufferBuilder,
    state: cypher.StandingQueryState
  ): TypeAndOffset =
    state match {
      case unitState: cypher.UnitState =>
        val offset: Offset = writeCypherUnitStandingQueryState(builder, unitState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherUnitStandingQueryState, offset)

      case crossState: cypher.CrossState =>
        val offset: Offset = writeCypherCrossStandingQueryState(builder, crossState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherCrossStandingQueryState, offset)

      case propState: cypher.LocalPropertyState =>
        val offset: Offset = writeCypherLocalPropertyStandingQueryState(builder, propState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherLocalPropertyStandingQueryState, offset)

      case idState: cypher.LocalIdState =>
        val offset: Offset = writeCypherLocalIdStandingQueryState(builder, idState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherLocalIdStandingQueryState, offset)

      case edgeState: cypher.SubscribeAcrossEdgeState =>
        val offset: Offset = writeCypherSubscribeAcrossEdgeStandingQueryState(builder, edgeState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherSubscribeAcrossEdgeStandingQueryState, offset)

      case edgeState: cypher.EdgeSubscriptionReciprocalState =>
        val offset: Offset = writeCypherEdgeSubscriptionReciprocalStandingQueryState(builder, edgeState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherEdgeSubscriptionReciprocalStandingQueryState, offset)

      case filterState: cypher.FilterMapState =>
        val offset: Offset = writeCypherFilterMapStandingQueryState(builder, filterState)
        TypeAndOffset(persistence.CypherStandingQueryState.CypherFilterMapStandingQueryState, offset)
    }

  private[this] def readCypherStandingQueryState(
    typ: Byte,
    makeState: Table => Table
  ): cypher.StandingQueryState =
    typ match {
      case persistence.CypherStandingQueryState.CypherUnitStandingQueryState =>
        val unitState = makeState(new persistence.CypherUnitStandingQueryState())
          .asInstanceOf[persistence.CypherUnitStandingQueryState]
        readCypherUnitStandingQueryState(unitState)

      case persistence.CypherStandingQueryState.CypherCrossStandingQueryState =>
        val crossState = makeState(new persistence.CypherCrossStandingQueryState())
          .asInstanceOf[persistence.CypherCrossStandingQueryState]
        readCypherCrossStandingQueryState(crossState)

      case persistence.CypherStandingQueryState.CypherLocalPropertyStandingQueryState =>
        val propState = makeState(new persistence.CypherLocalPropertyStandingQueryState())
          .asInstanceOf[persistence.CypherLocalPropertyStandingQueryState]
        readCypherLocalPropertyStandingQueryState(propState)

      case persistence.CypherStandingQueryState.CypherLocalIdStandingQueryState =>
        val idState = makeState(new persistence.CypherLocalIdStandingQueryState())
          .asInstanceOf[persistence.CypherLocalIdStandingQueryState]
        readCypherLocalIdStandingQueryState(idState)

      case persistence.CypherStandingQueryState.CypherSubscribeAcrossEdgeStandingQueryState =>
        val edgeState = makeState(new persistence.CypherSubscribeAcrossEdgeStandingQueryState())
          .asInstanceOf[persistence.CypherSubscribeAcrossEdgeStandingQueryState]
        readCypherSubscribeAcrossEdgeStandingQueryState(edgeState)

      case persistence.CypherStandingQueryState.CypherEdgeSubscriptionReciprocalStandingQueryState =>
        val edgeState = makeState(new persistence.CypherEdgeSubscriptionReciprocalStandingQueryState())
          .asInstanceOf[persistence.CypherEdgeSubscriptionReciprocalStandingQueryState]
        readCypherEdgeSubscriptionReciprocalStandingQueryState(edgeState)

      case persistence.CypherStandingQueryState.CypherFilterMapStandingQueryState =>
        val filterState = makeState(new persistence.CypherFilterMapStandingQueryState())
          .asInstanceOf[persistence.CypherFilterMapStandingQueryState]
        readCypherFilterMapStandingQueryState(filterState)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherStandingQueryState.names)
    }

  private[this] def writeCypherStandingQuerySubscriber(
    builder: FlatBufferBuilder,
    subscriber: CypherSubscriber
  ): TypeAndOffset =
    subscriber match {
      case CypherSubscriber.QuerySubscriber(onNode, globalId, queryId) =>
        val onNodeOff: Offset = writeQuineId(builder, onNode)
        val queryPartIdOff: Offset = writeStandingQueryPartId(builder, queryId)
        val globalQueryIdOff: Offset = writeStandingQueryId(builder, globalId)
        val offset: Offset = persistence.CypherNodeSubscriber.createCypherNodeSubscriber(
          builder,
          onNodeOff,
          queryPartIdOff,
          globalQueryIdOff
        )
        TypeAndOffset(persistence.CypherStandingQuerySubscriber.CypherNodeSubscriber, offset)

      case CypherSubscriber.GlobalSubscriber(globalId) =>
        val globalQueryIdOff: Offset = writeStandingQueryId(builder, globalId)
        val offset: Offset = persistence.CypherGlobalSubscriber.createCypherGlobalSubscriber(
          builder,
          globalQueryIdOff
        )
        TypeAndOffset(persistence.CypherStandingQuerySubscriber.CypherGlobalSubscriber, offset)
    }

  private[this] def readCypherStandingQuerySubscriber(
    typ: Byte,
    makeSubscriber: Table => Table
  ): CypherSubscriber =
    typ match {
      case persistence.CypherStandingQuerySubscriber.CypherNodeSubscriber =>
        val nodeSub =
          makeSubscriber(new persistence.CypherNodeSubscriber()).asInstanceOf[persistence.CypherNodeSubscriber]
        val onNode: QuineId = readQuineId(nodeSub.onNode)
        val queryPartId: StandingQueryPartId = readStandingQueryPartId(nodeSub.queryPartId)
        val globalQueryId: StandingQueryId = readStandingQueryId(nodeSub.globalQueryId)
        CypherSubscriber.QuerySubscriber(onNode, globalQueryId, queryPartId)

      case persistence.CypherStandingQuerySubscriber.CypherGlobalSubscriber =>
        val globalSub =
          makeSubscriber(new persistence.CypherGlobalSubscriber()).asInstanceOf[persistence.CypherGlobalSubscriber]
        val globalQueryId: StandingQueryId = readStandingQueryId(globalSub.globalQueryId)
        CypherSubscriber.GlobalSubscriber(globalQueryId)

      case other =>
        throw new InvalidUnionType(other, persistence.CypherStandingQueryState.names)
    }

  private[this] def writeCypherStandingQuerySubscribers(
    builder: FlatBufferBuilder,
    subscribers: StandingQuerySubscribers
  ): Offset = {
    val queryPartIdOff: Offset = writeStandingQueryPartId(builder, subscribers.forQuery)
    val globalQueryIdOff: Offset = writeStandingQueryId(builder, subscribers.globalId)
    val subTyps: Array[Byte] = new Array(subscribers.subscribers.size)
    val subOffs: Array[Offset] = new Array(subscribers.subscribers.size)
    for ((sub, i) <- subscribers.subscribers.zipWithIndex) {
      val TypeAndOffset(subTyp, subOff) = writeCypherStandingQuerySubscriber(builder, sub)
      subTyps(i) = subTyp
      subOffs(i) = subOff
    }
    val subsTypOff: Offset = persistence.CypherStandingQuerySubscribers.createSubscribersTypeVector(builder, subTyps)
    val subsOff: Offset = persistence.CypherStandingQuerySubscribers.createSubscribersVector(builder, subOffs)
    persistence.CypherStandingQuerySubscribers.createCypherStandingQuerySubscribers(
      builder,
      queryPartIdOff,
      globalQueryIdOff,
      subsTypOff,
      subsOff
    )
  }

  private[this] def readCypherStandingQuerySubscribers(
    subscribers: persistence.CypherStandingQuerySubscribers
  ): StandingQuerySubscribers = {
    val queryPartId: StandingQueryPartId = readStandingQueryPartId(subscribers.queryPartId)
    val globalQueryId: StandingQueryId = readStandingQueryId(subscribers.globalQueryId)
    val subs: mutable.Set[CypherSubscriber] = {
      val builder = mutable.Set.empty[CypherSubscriber]
      var i: Int = 0
      val subscribersLength = subscribers.subscribersLength
      while (i < subscribersLength) {
        builder += readCypherStandingQuerySubscriber(subscribers.subscribersType(i), subscribers.subscribers(_, i))
        i += 1
      }
      builder
    }
    StandingQuerySubscribers(queryPartId, globalQueryId, subs)
  }

  private[this] def writeCypherStandingQueryStateAndSubscribers(
    builder: FlatBufferBuilder,
    state: cypher.StandingQueryState,
    subscribers: StandingQuerySubscribers
  ): Offset = {
    val TypeAndOffset(stateTyp, stateOff) = writeCypherStandingQueryState(builder, state)
    val subscribersOff: Offset = writeCypherStandingQuerySubscribers(builder, subscribers)
    persistence.CypherStandingQueryStateAndSubscribers.createCypherStandingQueryStateAndSubscribers(
      builder,
      subscribersOff,
      stateTyp,
      stateOff
    )
  }

  private[this] def readCypherStandingQueryStateAndSubscribers(
    stateAndSubs: persistence.CypherStandingQueryStateAndSubscribers
  ): (StandingQuerySubscribers, cypher.StandingQueryState) = {
    val state = readCypherStandingQueryState(stateAndSubs.stateType, stateAndSubs.state(_))
    val subscribers = readCypherStandingQuerySubscribers(stateAndSubs.subscribers)
    subscribers -> state
  }

  val standingQueryFormat: BinaryFormat[StandingQuery] = new PackedFlatBufferBinaryFormat[StandingQuery] {
    def writeToBuffer(builder: FlatBufferBuilder, sq: StandingQuery): Offset =
      writeStandingQuery(builder, sq)

    def readFromBuffer(buffer: ByteBuffer): StandingQuery =
      readStandingQuery(persistence.StandingQuery.getRootAsStandingQuery(buffer))
  }

  val quineValueFormat: BinaryFormat[QuineValue] = new PackedFlatBufferBinaryFormat[QuineValue] {
    def writeToBuffer(builder: FlatBufferBuilder, quineValue: QuineValue): Offset =
      writeQuineValue(builder, quineValue)

    def readFromBuffer(buffer: ByteBuffer): QuineValue =
      readQuineValue(persistence.QuineValue.getRootAsQuineValue(buffer))
  }

  val domainGraphNodeFormat: BinaryFormat[DomainGraphNode] = new PackedFlatBufferBinaryFormat[DomainGraphNode] {
    def writeToBuffer(builder: FlatBufferBuilder, dgn: DomainGraphNode): Offset =
      writeBoxedDomainGraphNode(builder, dgn)

    def readFromBuffer(buffer: ByteBuffer): DomainGraphNode =
      readBoxedDomainGraphNode(persistence.BoxedDomainGraphNode.getRootAsBoxedDomainGraphNode(buffer))
  }

  val cypherExprFormat: BinaryFormat[Expr] = new PackedFlatBufferBinaryFormat[cypher.Expr] {
    def writeToBuffer(builder: FlatBufferBuilder, expr: cypher.Expr): Offset =
      writeBoxedCypherExpr(builder, expr)

    def readFromBuffer(buffer: ByteBuffer): cypher.Expr =
      readBoxedCypherExpr(persistence.BoxedCypherExpr.getRootAsBoxedCypherExpr(buffer))
  }

  val eventFormat: BinaryFormat[NodeEvent] = new PackedFlatBufferBinaryFormat[NodeEvent] {
    def writeToBuffer(builder: FlatBufferBuilder, event: NodeEvent): Offset =
      writeNodeEvent(builder, event)

    def readFromBuffer(buffer: ByteBuffer): NodeEvent =
      readNodeEvent(persistence.NodeEvent.getRootAsNodeEvent(buffer))
  }

  val eventWithTimeFormat: BinaryFormat[NodeEvent.WithTime] =
    new PackedFlatBufferBinaryFormat[NodeEvent.WithTime] {
      def writeToBuffer(builder: FlatBufferBuilder, event: NodeEvent.WithTime): Offset =
        writeNodeEventWithTime(builder, event)

      def readFromBuffer(buffer: ByteBuffer): NodeEvent.WithTime =
        readNodeEventWithTime(persistence.NodeEventWithTime.getRootAsNodeEventWithTime(buffer))
    }

  val standingQueryStateFormat: BinaryFormat[(StandingQuerySubscribers, StandingQueryState)] =
    new PackedFlatBufferBinaryFormat[(StandingQuerySubscribers, cypher.StandingQueryState)] {
      def writeToBuffer(
        builder: FlatBufferBuilder,
        state: (StandingQuerySubscribers, cypher.StandingQueryState)
      ): Offset =
        writeCypherStandingQueryStateAndSubscribers(builder, state._2, state._1)

      def readFromBuffer(buffer: ByteBuffer): (StandingQuerySubscribers, cypher.StandingQueryState) =
        readCypherStandingQueryStateAndSubscribers(
          persistence.CypherStandingQueryStateAndSubscribers.getRootAsCypherStandingQueryStateAndSubscribers(buffer)
        )
    }

  val nodeSnapshotFormat: BinaryFormat[NodeSnapshot] = new PackedFlatBufferBinaryFormat[NodeSnapshot] {
    def writeToBuffer(builder: FlatBufferBuilder, snapshot: NodeSnapshot): Offset =
      writeNodeSnapshot(builder, snapshot)

    def readFromBuffer(buffer: ByteBuffer): NodeSnapshot =
      readNodeSnapshot(persistence.NodeSnapshot.getRootAsNodeSnapshot(buffer))
  }
}

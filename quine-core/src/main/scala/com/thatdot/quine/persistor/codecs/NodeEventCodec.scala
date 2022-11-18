package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.{ByteBufferOps, EventTime, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.{HalfEdge, PropertyValue, QuineId}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}
object NodeEventCodec extends PersistenceCodec[NodeEvent] {

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

      case other =>
        throw new InvalidEventType(other, persistence.NodeEventUnion.names)

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
          QuineId(event.otherIdAsByteBuffer.remainingBytes)
        )
        NodeChangeEvent.EdgeAdded(halfEdge)

      case persistence.NodeEventUnion.RemoveEdge =>
        val event = makeEvent(new persistence.RemoveEdge()).asInstanceOf[persistence.RemoveEdge]
        val halfEdge = HalfEdge(
          Symbol(event.edgeType),
          byte2EdgeDirection(event.direction),
          QuineId(event.otherIdAsByteBuffer.remainingBytes)
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
    val event = readNodeEventUnion(eventWithTime.eventType, eventWithTime.event)
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
    readNodeEventUnion(event.eventType, event.event)

  val format: BinaryFormat[NodeEvent] = new PackedFlatBufferBinaryFormat[NodeEvent] {
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
}

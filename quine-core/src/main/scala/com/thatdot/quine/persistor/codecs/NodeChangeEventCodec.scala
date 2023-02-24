package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.{ByteBufferOps, EdgeEvent, NodeChangeEvent, PropertyEvent}
import com.thatdot.quine.model.{HalfEdge, PropertyValue, QuineId}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}
object NodeChangeEventCodec extends PersistenceCodec[NodeChangeEvent] {

  private[this] def writeNodeEventUnion(
    builder: FlatBufferBuilder,
    event: NodeChangeEvent
  ): TypeAndOffset =
    event match {
      case EdgeEvent.EdgeAdded(HalfEdge(edgeType, dir, other)) =>
        val event = persistence.AddEdge.createAddEdge(
          builder,
          builder.createString(edgeType.name),
          edgeDirection2Byte(dir),
          builder.createByteVector(other.array)
        )
        TypeAndOffset(persistence.NodeEventUnion.AddEdge, event)

      case EdgeEvent.EdgeRemoved(HalfEdge(edgeType, dir, other)) =>
        val event = persistence.RemoveEdge.createRemoveEdge(
          builder,
          builder.createString(edgeType.name),
          edgeDirection2Byte(dir),
          builder.createByteVector(other.array)
        )
        TypeAndOffset(persistence.NodeEventUnion.RemoveEdge, event)

      case PropertyEvent.PropertySet(propKey, value) =>
        val event = persistence.AddProperty.createAddProperty(
          builder,
          builder.createString(propKey.name),
          builder.createByteVector(value.serialized)
        )
        TypeAndOffset(persistence.NodeEventUnion.AddProperty, event)

      case PropertyEvent.PropertyRemoved(propKey, value) =>
        val event = persistence.RemoveProperty.createRemoveProperty(
          builder,
          builder.createString(propKey.name),
          builder.createByteVector(value.serialized)
        )
        TypeAndOffset(persistence.NodeEventUnion.RemoveProperty, event)

      case other =>
        throw new InvalidEventType(other, persistence.NodeEventUnion.names)

    }
  private[this] def readNodeChangeEventUnion(
    typ: Byte,
    makeEvent: Table => Table
  ): NodeChangeEvent =
    typ match {
      case persistence.NodeEventUnion.AddEdge =>
        val event = makeEvent(new persistence.AddEdge()).asInstanceOf[persistence.AddEdge]
        val halfEdge = HalfEdge(
          Symbol(event.edgeType),
          byte2EdgeDirection(event.direction),
          QuineId(event.otherIdAsByteBuffer.remainingBytes)
        )
        EdgeEvent.EdgeAdded(halfEdge)

      case persistence.NodeEventUnion.RemoveEdge =>
        val event = makeEvent(new persistence.RemoveEdge()).asInstanceOf[persistence.RemoveEdge]
        val halfEdge = HalfEdge(
          Symbol(event.edgeType),
          byte2EdgeDirection(event.direction),
          QuineId(event.otherIdAsByteBuffer.remainingBytes)
        )
        EdgeEvent.EdgeRemoved(halfEdge)

      case persistence.NodeEventUnion.AddProperty =>
        val event = makeEvent(new persistence.AddProperty()).asInstanceOf[persistence.AddProperty]
        val propertyKey = Symbol(event.key)
        val propertyValue = PropertyValue.fromBytes(event.valueAsByteBuffer.remainingBytes)
        PropertyEvent.PropertySet(propertyKey, propertyValue)

      case persistence.NodeEventUnion.RemoveProperty =>
        val event = makeEvent(new persistence.RemoveProperty()).asInstanceOf[persistence.RemoveProperty]
        val propertyKey = Symbol(event.key)
        val propertyValue = PropertyValue.fromBytes(event.valueAsByteBuffer.remainingBytes)
        PropertyEvent.PropertyRemoved(propertyKey, propertyValue)

      case other =>
        throw new InvalidUnionType(other, persistence.NodeEventUnion.names)
    }

  private[this] def writeNodeChangeEvent(
    builder: FlatBufferBuilder,
    event: NodeChangeEvent
  ): Offset = {
    val TypeAndOffset(eventTyp, eventOff) = writeNodeEventUnion(builder, event)
    persistence.NodeEvent.createNodeEvent(builder, eventTyp, eventOff)
  }

  private[this] def readNodeEvent(
    event: persistence.NodeEvent
  ): NodeChangeEvent =
    readNodeChangeEventUnion(event.eventType, event.event)

  val format: BinaryFormat[NodeChangeEvent] = new PackedFlatBufferBinaryFormat[NodeChangeEvent] {
    def writeToBuffer(builder: FlatBufferBuilder, event: NodeChangeEvent): Offset =
      writeNodeChangeEvent(builder, event)

    def readFromBuffer(buffer: ByteBuffer): NodeChangeEvent =
      readNodeEvent(persistence.NodeEvent.getRootAsNodeEvent(buffer))
  }

}

package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.{ByteBufferOps, DomainIndexEvent, EventTime, NodeChangeEvent, NodeEvent, StandingQueryId}
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
          builder.createByteVector(replyToNode.array),
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

package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.{ByteBufferOps, DomainIndexEvent, EventTime, NodeEvent, StandingQueryId}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object DomainIndexEventCodec extends PersistenceCodec[DomainIndexEvent] {

  private[this] def writeDomainIndexEventUnion(
    builder: FlatBufferBuilder,
    event: DomainIndexEvent
  ): TypeAndOffset =
    event match {

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
        TypeAndOffset(persistence.DomainIndexEventUnion.CreateDomainNodeSubscription, event)

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
        TypeAndOffset(persistence.DomainIndexEventUnion.CreateDomainStandingQuerySubscription, event)

      case DomainIndexEvent.DomainNodeSubscriptionResult(from, testBranch, result) =>
        val event: Offset = persistence.DomainNodeSubscriptionResult.createDomainNodeSubscriptionResult(
          builder,
          builder.createByteVector(from.array),
          testBranch,
          result
        )
        TypeAndOffset(persistence.DomainIndexEventUnion.DomainNodeSubscriptionResult, event)

      case DomainIndexEvent.CancelDomainNodeSubscription(testBranch, alreadyCancelledSubscriber) =>
        val event = persistence.CancelDomainNodeSubscription.createCancelDomainNodeSubscription(
          builder,
          testBranch,
          builder.createByteVector(alreadyCancelledSubscriber.array)
        )
        TypeAndOffset(persistence.DomainIndexEventUnion.CancelDomainNodeSubscription, event)
    }

  private[this] def readDomainIndexEventUnion(
    typ: Byte,
    makeEvent: Table => Table
  ): DomainIndexEvent =
    typ match {
      case persistence.DomainIndexEventUnion.CreateDomainNodeSubscription =>
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

      case persistence.DomainIndexEventUnion.CreateDomainStandingQuerySubscription =>
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

      case persistence.DomainIndexEventUnion.DomainNodeSubscriptionResult =>
        val event = makeEvent(new persistence.DomainNodeSubscriptionResult())
          .asInstanceOf[persistence.DomainNodeSubscriptionResult]
        val from = new QuineId(event.fromIdAsByteBuffer.remainingBytes)
        val dgnId = event.testDgnId()
        val result = event.result()
        DomainIndexEvent.DomainNodeSubscriptionResult(from, dgnId, result)

      case persistence.DomainIndexEventUnion.CancelDomainNodeSubscription =>
        val event = makeEvent(new persistence.CancelDomainNodeSubscription())
          .asInstanceOf[persistence.CancelDomainNodeSubscription]
        val dgnId = event.testDgnId()
        val subscriber = new QuineId(event.alreadyCancelledSubscriberAsByteBuffer.remainingBytes)
        DomainIndexEvent.CancelDomainNodeSubscription(dgnId, subscriber)

      case other =>
        throw new InvalidUnionType(other, persistence.DomainIndexEventUnion.names)
    }

  private[this] def writeDomainIndexEventWithTime(
    builder: FlatBufferBuilder,
    eventWithTime: NodeEvent.WithTime
  ): Offset = {
    val TypeAndOffset(eventTyp, eventOff) =
      writeDomainIndexEventUnion(builder, eventWithTime.event.asInstanceOf[DomainIndexEvent])
    persistence.DomainIndexEventWithTime.createDomainIndexEventWithTime(
      builder,
      eventWithTime.atTime.eventTime,
      eventTyp,
      eventOff
    )
  }

  private[this] def readDomainIndexEventWithTime(
    eventWithTime: persistence.DomainIndexEventWithTime
  ): NodeEvent.WithTime = {
    val event = readDomainIndexEventUnion(eventWithTime.eventType, eventWithTime.event)
    val atTime = EventTime.fromRaw(eventWithTime.eventTime)
    NodeEvent.WithTime(event, atTime)
  }

  private[this] def writeDomainIndexEvent(
    builder: FlatBufferBuilder,
    event: DomainIndexEvent
  ): Offset = {
    val TypeAndOffset(eventTyp, eventOff) = writeDomainIndexEventUnion(builder, event)
    persistence.DomainIndexEvent.createDomainIndexEvent(builder, eventTyp, eventOff)
  }

  private[this] def readDomainIndexEvent(
    event: persistence.DomainIndexEvent
  ): DomainIndexEvent =
    readDomainIndexEventUnion(event.eventType, event.event)

  val format: BinaryFormat[DomainIndexEvent] = new PackedFlatBufferBinaryFormat[DomainIndexEvent] {
    def writeToBuffer(builder: FlatBufferBuilder, event: DomainIndexEvent): Offset =
      writeDomainIndexEvent(builder, event)

    def readFromBuffer(buffer: ByteBuffer): DomainIndexEvent =
      readDomainIndexEvent(persistence.DomainIndexEvent.getRootAsDomainIndexEvent(buffer))
  }

  val domainIndexEventWithTimeFormat: BinaryFormat[NodeEvent.WithTime] =
    new PackedFlatBufferBinaryFormat[NodeEvent.WithTime] {
      def writeToBuffer(builder: FlatBufferBuilder, event: NodeEvent.WithTime): Offset =
        writeDomainIndexEventWithTime(builder, event)

      def readFromBuffer(buffer: ByteBuffer): NodeEvent.WithTime =
        readDomainIndexEventWithTime(persistence.DomainIndexEventWithTime.getRootAsDomainIndexEventWithTime(buffer))
    }
}

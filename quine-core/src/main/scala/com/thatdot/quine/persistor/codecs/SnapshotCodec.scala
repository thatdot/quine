package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.{AbstractIterable, mutable}

import com.google.flatbuffers.FlatBufferBuilder

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior
import com.thatdot.quine.graph.{AbstractNodeSnapshot, ByteBufferOps, EventTime, Notifiable, StandingQueryId}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{HalfEdge, PropertyValue}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

/** A codec for snapshots, sans logic for dealing with reserved fields. In Quine, these wil always have fixed values.
  * All implementers will be binary-compatible with the `NodeSnapshot` flatbuffers type
  */
abstract class AbstractSnapshotCodec[SnapshotT <: AbstractNodeSnapshot] extends PersistenceCodec[SnapshotT] {
  // Compute the value of the reserved property in preparation for writing (always false in Quine)
  def determineReserved(snapshot: SnapshotT): Boolean
  // Emit a final result, given a baseline snapshot and the value of the reserved property (always false in Quine)
  def constructDeserialized(
    time: EventTime,
    properties: Map[Symbol, PropertyValue],
    edges: Iterable[HalfEdge],
    subscribersToThisNode: MutableMap[
      DomainGraphNodeId,
      DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription,
    ],
    domainNodeIndex: MutableMap[
      QuineId,
      MutableMap[DomainGraphNodeId, Option[Boolean]],
    ],
    reserved: Boolean,
  ): SnapshotT

  private[codecs] def writeNodeSnapshot(
    builder: FlatBufferBuilder,
    snapshot: SnapshotT,
  ): Offset = {

    val time = snapshot.time.eventTime
    val properties: Offset = {
      val propertiesOffs: Array[Offset] = new Array[Offset](snapshot.properties.size)
      for (((propKey, propVal), i) <- snapshot.properties.zipWithIndex)
        propertiesOffs(i) = persistence.Property.createProperty(
          builder,
          builder.createString(propKey.name),
          persistence.Property.createValueVector(builder, propVal.serialized),
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
              DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription(
                notifiables,
                lastNotification,
                relatedQueries,
              ),
            ),
            i,
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
            relatedQueriesOffset,
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
                lastNotificationEnum,
              )
            }
            persistence.NodeIndex.createQueriesVector(builder, queriesOffs)
          }

          domainNodeIndexOffs(i) = persistence.NodeIndex.createNodeIndex(
            builder,
            subscriberOff,
            queries,
          )
        }
        persistence.NodeSnapshot.createDomainNodeIndexVector(builder, domainNodeIndexOffs)
      }

    val reserved = determineReserved(snapshot)

    persistence.NodeSnapshot.createNodeSnapshot(
      builder,
      time,
      properties,
      edges,
      subscribers,
      domainNodeIndex,
      reserved,
    )
  }

  private[codecs] def readNodeSnapshot(snapshot: persistence.NodeSnapshot): SnapshotT = {
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
        DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription,
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
                  subscriber.notifiable(new persistence.QuineId(), j).asInstanceOf[persistence.QuineId],
                ),
              )

            case persistence.Notifiable.StandingQueryId =>
              Right(
                readStandingQueryId(
                  subscriber.notifiable(new persistence.StandingQueryId(), j).asInstanceOf[persistence.StandingQueryId],
                ),
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

        builder += dgnId -> DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription(
          notifiables.toSet,
          lastNotification,
          relatedQueries.toSet,
        )
        i += 1
      }
      builder
    }

    val domainNodeIndex = {
      val builder = mutable.Map.empty[
        QuineId,
        mutable.Map[DomainGraphNodeId, Option[Boolean]],
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

    val reserved = snapshot.reserved

    constructDeserialized(
      time,
      properties,
      edges,
      subscribersToThisNode,
      domainNodeIndex,
      reserved,
    )
  }

  val format: BinaryFormat[SnapshotT] = new PackedFlatBufferBinaryFormat[SnapshotT] {
    def writeToBuffer(builder: FlatBufferBuilder, snapshot: SnapshotT): Offset =
      writeNodeSnapshot(builder, snapshot)

    def readFromBuffer(buffer: ByteBuffer): SnapshotT =
      readNodeSnapshot(persistence.NodeSnapshot.getRootAsNodeSnapshot(buffer))
  }
}

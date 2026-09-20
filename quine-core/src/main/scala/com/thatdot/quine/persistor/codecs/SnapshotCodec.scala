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
      MutableMap[DomainGraphNodeId, DomainNodeIndexBehavior.DomainNodeIndex.DomainIndexResult],
    ],
    reserved: Boolean,
  ): SnapshotT

  private[this] def writeLastNotification(value: Option[Boolean]): Byte = value match {
    case None => persistence.LastNotification.None
    case Some(false) => persistence.LastNotification.False
    case Some(true) => persistence.LastNotification.True
  }

  private[this] def readLastNotification(value: Byte): Option[Boolean] = value match {
    case persistence.LastNotification.None => None
    case persistence.LastNotification.False => Some(false)
    case persistence.LastNotification.True => Some(true)
    case other => throw new InvalidUnionType(other, persistence.LastNotification.names)
  }

  private[this] def readNotifiable(
    unionType: Byte,
    read: com.google.flatbuffers.Table => com.google.flatbuffers.Table,
  ): Notifiable =
    unionType match {
      case persistence.Notifiable.QuineId =>
        Left(readQuineId(read(new persistence.QuineId()).asInstanceOf[persistence.QuineId]))
      case persistence.Notifiable.StandingQueryId =>
        Right(readStandingQueryId(read(new persistence.StandingQueryId()).asInstanceOf[persistence.StandingQueryId]))
      case other =>
        throw new InvalidUnionType(other, persistence.Notifiable.names)
    }

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
        for (((node, subscription), i) <- snapshot.subscribersToThisNode.zipWithIndex) {
          val latestAnswer = subscription.latestAnswer
          val queriesPerSubscriber = subscription.queriesPerSubscriber
          val lastNotificationEnum: Byte = writeLastNotification(latestAnswer)

          // `notifiable` and `related_queries` are not written. Both exist only so that a snapshot from 2.1.1 can
          // still be read: the first listed the subscribers, the second the union of their queries, and
          // `queries_per_subscriber` now carries both. Downgrading from 2.2.0 is not supported, so writing them
          // would put bytes in every snapshot -- largest on exactly the supernodes this work is trimming -- for a
          // reader that will never exist.

          val queriesPerSubscriberOffsets = new Array[Offset](queriesPerSubscriber.size)
          for (((subscriber, queries), j) <- queriesPerSubscriber.zipWithIndex) {
            val queriesOffs = new Array[Offset](queries.size)
            for ((query, k) <- queries.zipWithIndex) queriesOffs(k) = writeStandingQueryId(builder, query)
            val queriesOff = persistence.SubscriberQueries.createQueriesVector(builder, queriesOffs)
            val (subscriberType, subscriberOff) = subscriber match {
              case Left(nodeId) => (persistence.Notifiable.QuineId, writeQuineId(builder, nodeId))
              case Right(sqId) => (persistence.Notifiable.StandingQueryId, writeStandingQueryId(builder, sqId))
            }
            queriesPerSubscriberOffsets(j) =
              persistence.SubscriberQueries.createSubscriberQueries(builder, subscriberType, subscriberOff, queriesOff)
          }
          val queriesPerSubscriberOffset =
            persistence.Subscriber.createQueriesPerSubscriberVector(builder, queriesPerSubscriberOffsets)

          subscribersOffs(i) = persistence.Subscriber.createSubscriber(
            builder,
            node,
            NoOffset, // notifiable_type
            NoOffset, // notifiable
            lastNotificationEnum,
            NoOffset, // related_queries
            queriesPerSubscriberOffset,
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
              val lastNotificationEnum: Byte = writeLastNotification(result.answer)
              val forQueriesOff = persistence.NodeIndexQuery.createForQueriesVector(
                builder,
                result.forQueries.toArray.map(writeStandingQueryId(builder, _)),
              )
              queriesOffs(i) = persistence.NodeIndexQuery.createNodeIndexQuery(
                builder,
                branch,
                lastNotificationEnum,
                forQueriesOff,
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
          notifiables += readNotifiable(subscriber.notifiableType(j), subscriber.notifiable(_, j))
          j += 1
        }
        // `subscriber.lastNotification` is the generated accessor for the wire field, which keeps its name.
        val latestAnswer: Option[Boolean] = readLastNotification(subscriber.lastNotification)
        val recordedQueriesPerSubscriber = Map.newBuilder[Notifiable, Set[StandingQueryId]]
        var n: Int = 0
        val queriesPerSubscriberLength = subscriber.queriesPerSubscriberLength
        val hasQueriesPerSubscriber = queriesPerSubscriberLength > 0
        while (n < queriesPerSubscriberLength) {
          val entry = subscriber.queriesPerSubscriber(n)
          val queries = Set.newBuilder[StandingQueryId]
          var q: Int = 0
          val queriesLength = entry.queriesLength
          while (q < queriesLength) {
            queries += readStandingQueryId(entry.queries(q))
            q += 1
          }
          recordedQueriesPerSubscriber += readNotifiable(entry.subscriberType, entry.subscriber(_)) -> queries.result()
          n += 1
        }

        val relatedQueries = mutable.Set.empty[StandingQueryId]
        var k: Int = 0
        val relatedQueriesLength = subscriber.relatedQueriesLength
        while (k < relatedQueriesLength) {
          relatedQueries += readStandingQueryId(subscriber.relatedQueries(k))
          k += 1
        }

        val subscribers = notifiables.toSet
        val related = relatedQueries.toSet

        // Which release wrote this, and what that means for the query attribution, is stated in one place.
        val attributedQueriesPerSubscriber = SnapshotMigration.subscriberFormat(hasQueriesPerSubscriber) match {
          case SnapshotMigration.Version.V2_2_0 => recordedQueriesPerSubscriber.result()
          case SnapshotMigration.Version.V2_1_1 =>
            SnapshotMigration.From_2_1_1_to_2_2_0.queriesPerSubscriber(
              recorded = recordedQueriesPerSubscriber.result(),
              subscribers = subscribers,
              union = related,
            )
        }

        builder += dgnId -> DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription(
          latestAnswer,
          attributedQueriesPerSubscriber,
        )
        i += 1
      }
      builder
    }

    val domainNodeIndex = {
      val builder = mutable.Map.empty[
        QuineId,
        mutable.Map[DomainGraphNodeId, DomainNodeIndexBehavior.DomainNodeIndex.DomainIndexResult],
      ]

      var i: Int = 0
      val domainNodeIndexLength = snapshot.domainNodeIndexLength
      while (i < domainNodeIndexLength) {
        val nodeIndex: persistence.NodeIndex = snapshot.domainNodeIndex(i)
        val subscriber = readQuineId(nodeIndex.subscriber)
        val results = mutable.Map.empty[DomainGraphNodeId, DomainNodeIndexBehavior.DomainNodeIndex.DomainIndexResult]
        var j: Int = 0
        val queriesLength = nodeIndex.queriesLength
        while (j < queriesLength) {
          val query = nodeIndex.queries(j)
          val forQueries = Set.newBuilder[StandingQueryId]
          var a: Int = 0
          val forQueriesLength = query.forQueriesLength
          while (a < forQueriesLength) {
            forQueries += readStandingQueryId(query.forQueries(a))
            a += 1
          }
          results += query.dgnId ->
          DomainNodeIndexBehavior.DomainNodeIndex.DomainIndexResult(
            readLastNotification(query.result),
            forQueries.result(),
          )
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

package com.thatdot.quine.graph

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ConcurrentNavigableMap}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer}

import cats.data.NonEmptyList
import com.google.flatbuffers.FlatBufferBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  HalfEdge,
  PropertyComparisonFunctions,
  PropertyValue,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.NoOffset
import com.thatdot.quine.persistor.codecs.SnapshotMigration
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.Packing
import com.thatdot.quine.util.TestLogging._

/** Reading a snapshot written by Quine 2.1.1 on a Quine 2.2.0 node.
  *
  * 2.2.0 changed what a node records about its DistinctId subscriptions, and added two fields neither of which
  * can be recovered from what a 2.1.1 snapshot holds:
  *
  *   - `Subscriber.named`, the queries each subscriber depends on this node for. A 2.1.1 snapshot carries only
  *     `related_queries`, the union across every subscriber, so the per-subscriber breakdown was never written.
  *   - `NodeIndexQuery.asked_by`, who an answer is held for. A 2.1.1 snapshot carries the answer alone.
  *
  * The store version (`PersistenceAgent.CurrentVersion`) moves 13.2.0 -> 13.3.0 across the same release, so a
  * 2.2.0 build is expected to read 2.1.1 data rather than reject it.
  *
  * The decoder therefore has to make something up, and what it makes up has to be safe in the direction that
  * matters: never dropping a subscription a running query still needs, and never reporting an answer nobody is
  * maintaining. The first three tests pin those choices, and pass today.
  *
  * What does not exist yet is any way to tell a fabricated attribution from a real one. Once a 2.1.1 snapshot is
  * read and written back, the guess is indistinguishable from fact and is never corrected. The last test states
  * what the migration should do about that, and is ignored until that logic is written.
  */
class SnapshotFormatMigrationTest extends AnyFlatSpec with should.Matchers {

  implicit private val askTimeout: org.apache.pekko.util.Timeout = org.apache.pekko.util.Timeout(30.seconds)

  private val dgnId: DomainGraphNodeId = 1L
  private val childDgnId: DomainGraphNodeId = 2L
  private val peerNode: QuineId = QuineId(Array(7.toByte))
  private val peerSubscriber: Notifiable = Left(peerNode)
  private val querySubscriber: Notifiable = Right(StandingQueryId(new UUID(0L, 1L)))
  private val queryA: StandingQueryId = StandingQueryId(new UUID(0L, 1L))
  private val queryB: StandingQueryId = StandingQueryId(new UUID(0L, 2L))

  private def writeQuineId(builder: FlatBufferBuilder, qid: QuineId): Int =
    persistence.QuineId.createQuineId(builder, persistence.QuineId.createIdVector(builder, qid.array))

  private def writeStandingQueryId(builder: FlatBufferBuilder, sqId: StandingQueryId): Int =
    persistence.StandingQueryId.createStandingQueryId(
      builder,
      sqId.uuid.getLeastSignificantBits,
      sqId.uuid.getMostSignificantBits,
    )

  /** A snapshot in exactly the shape Quine 2.1.1 wrote: a `Subscriber` with no `named` vector and a
    * `NodeIndexQuery` with no `asked_by` vector. FlatBuffers writes nothing for a vector offset of zero, so
    * passing [[NoOffset]] reproduces the 2.1.1 bytes from the 2.2.0 generated builders.
    */
  private def snapshotBytes_2_1_1(
    subscribers: Seq[Notifiable],
    relatedQueries: Set[StandingQueryId],
    latestAnswer: Byte,
    indexAnswer: Option[Byte],
    subscribedPattern: DomainGraphNodeId = dgnId,
    indexChild: DomainGraphNodeId = childDgnId,
    indexPeer: QuineId = peerNode,
    properties: Map[String, QuineValue] = Map.empty,
    edges: List[HalfEdge] = Nil,
  ): Array[Byte] = {
    val builder = new FlatBufferBuilder()

    val subscriberOffset = {
      val (types, offsets) = subscribers.map {
        case Left(qid) => (persistence.Notifiable.QuineId, writeQuineId(builder, qid))
        case Right(sqId) => (persistence.Notifiable.StandingQueryId, writeStandingQueryId(builder, sqId))
      }.unzip
      val notifiableType = persistence.Subscriber.createNotifiableTypeVector(builder, types.toArray)
      val notifiable = persistence.Subscriber.createNotifiableVector(builder, offsets.toArray)
      val related = persistence.Subscriber.createRelatedQueriesVector(
        builder,
        relatedQueries.toArray.map(writeStandingQueryId(builder, _)),
      )
      persistence.Subscriber.createSubscriber(
        builder,
        subscribedPattern,
        notifiableType,
        notifiable,
        latestAnswer,
        related,
        NoOffset, // `named` did not exist in 2.1.1
      )
    }
    val subscribersVector = persistence.NodeSnapshot.createSubscribersVector(builder, Array(subscriberOffset))

    val indexVector = indexAnswer match {
      case None => NoOffset
      case Some(answer) =>
        val query = persistence.NodeIndexQuery.createNodeIndexQuery(
          builder,
          indexChild,
          answer,
          NoOffset, // `for_queries` did not exist in 2.1.1
        )
        val queries = persistence.NodeIndex.createQueriesVector(builder, Array(query))
        val nodeIndex = persistence.NodeIndex.createNodeIndex(builder, writeQuineId(builder, indexPeer), queries)
        persistence.NodeSnapshot.createDomainNodeIndexVector(builder, Array(nodeIndex))
    }

    val propertiesOff =
      if (properties.isEmpty) NoOffset
      else
        persistence.NodeSnapshot.createPropertiesVector(
          builder,
          properties.toArray.map { case (k, v) =>
            persistence.Property.createProperty(
              builder,
              builder.createString(k),
              persistence.Property.createValueVector(builder, PropertyValue(v).serialized),
            )
          },
        )
    val edgesOff =
      if (edges.isEmpty) NoOffset
      else persistence.NodeSnapshot.createEdgesVector(builder, edges.toArray.map(writeHalfEdgeLocal(builder, _)))

    val snapshot = persistence.NodeSnapshot.createNodeSnapshot(
      builder,
      EventTime.MinValue.eventTime,
      propertiesOff,
      edgesOff,
      subscribersVector,
      indexVector,
      false,
    )
    builder.prep(8, 0)
    builder.finish(snapshot)
    Packing.pack(builder.sizedByteArray())
  }

  private def writeHalfEdgeLocal(builder: FlatBufferBuilder, e: HalfEdge): Int =
    persistence.HalfEdge.createHalfEdge(
      builder,
      builder.createString(e.edgeType.name),
      e.direction match {
        case EdgeDirection.Outgoing => persistence.EdgeDirection.Outgoing
        case EdgeDirection.Incoming => persistence.EdgeDirection.Incoming
        case EdgeDirection.Undirected => persistence.EdgeDirection.Undirected
      },
      writeQuineId(builder, e.other),
    )

  private def decode(bytes: Array[Byte]): NodeSnapshot =
    NodeSnapshot.snapshotCodec.format.read(bytes).get

  behavior of "a Quine 2.2.0 node reading a Quine 2.1.1 snapshot"

  it should "give every subscriber the whole related-query union" in {
    // 2.1.1 never wrote the breakdown, so the only safe guess is that each subscriber depends on all of them: a
    // subscriber is then kept while any of those queries runs, which is what 2.1.1 itself did for it. Guessing
    // the other way would drop a subscription a running query still needs.
    val decoded = decode(
      snapshotBytes_2_1_1(
        subscribers = Seq(peerSubscriber, querySubscriber),
        relatedQueries = Set(queryA, queryB),
        latestAnswer = persistence.LastNotification.True,
        indexAnswer = None,
      ),
    )
    val restored = decoded.subscribersToThisNode(dgnId)

    restored.subscribers shouldBe Set(peerSubscriber, querySubscriber)
    restored.queriesFor(peerSubscriber) shouldBe Set(queryA, queryB)
    restored.queriesFor(querySubscriber) shouldBe Set(queryA, queryB)
    restored.relatedQueries shouldBe Set(queryA, queryB)
    restored.latestAnswer shouldBe Some(true)
  }

  it should "leave an index answer recording no queries, so that it is asked for again" in {
    // `for_queries` is what says which queries a peer is still maintaining an answer for. Empty reads as
    // unknown, which makes `childAnswersAreMaintained` false and sends the question again rather than
    // reporting an answer nobody is correcting.
    val decoded = decode(
      snapshotBytes_2_1_1(
        subscribers = Seq(peerSubscriber),
        relatedQueries = Set(queryA),
        latestAnswer = persistence.LastNotification.True,
        indexAnswer = Some(persistence.LastNotification.True),
      ),
    )
    val result = decoded.domainNodeIndex(peerNode)(childDgnId)

    result.answer shouldBe Some(true)
    result.forQueries shouldBe Set.empty[StandingQueryId]
  }

  it should "decode a subscription that had never reported an answer" in {
    val decoded = decode(
      snapshotBytes_2_1_1(
        subscribers = Seq(peerSubscriber),
        relatedQueries = Set.empty,
        latestAnswer = persistence.LastNotification.None,
        indexAnswer = None,
      ),
    )
    val restored = decoded.subscribersToThisNode(dgnId)

    restored.latestAnswer shouldBe None
    restored.subscribers shouldBe Set(peerSubscriber)
    restored.queriesFor(peerSubscriber) shouldBe Set.empty[StandingQueryId]
  }

  behavior of "migrating a Quine 2.1.1 snapshot to the 2.2.0 format"

  /** What replaced an earlier requirement here, which asked that a fabricated attribution be *distinguishable*
    * from a real one so it could later be replaced.
    *
    * That was written when the guess was frozen: attributed once at decode, written back, and thereafter
    * indistinguishable from fact. Two things since have made it wrong to ask for. The sweeps now drop cancelled
    * queries from what survives, so the guess narrows towards the queries that really are live rather than
    * standing still. And distinguishing it would mean a field in the snapshot written only to describe the
    * snapshot -- bytes in every record, forever, to serve an upgrade each node performs once.
    *
    * So the over-approximation is accepted, and what is asserted is the property that makes it safe: it is only
    * ever too generous. A subscriber may be credited with a query it never depended on, which keeps a subscription
    * alive longer than it need be; it is never credited with too few, which would drop results.
    */
  it should "credit each subscriber with at least the queries it really depended on, never fewer" in {
    val decoded = decode(
      snapshotBytes_2_1_1(
        subscribers = Seq(peerSubscriber, querySubscriber),
        relatedQueries = Set(queryA, queryB),
        latestAnswer = persistence.LastNotification.True,
        indexAnswer = None,
      ),
    )
    val restored = decoded.subscribersToThisNode(dgnId)

    // Whatever any subscriber really depended on was inside the union, because the union is what 2.1.1 recorded.
    // Crediting each with the whole union therefore cannot omit anything.
    val union = Set(queryA, queryB)
    restored.subscribers.foreach { subscriber =>
      withClue(s"$subscriber: ")(restored.queriesFor(subscriber) should contain allElementsOf union)
    }
    withClue("and no subscriber is invented: ")(restored.subscribers shouldBe Set(peerSubscriber, querySubscriber))
  }

  it should "name the release that wrote a record by what the record carries, without a version stamp" in {
    // The upgrade needs no field of its own: an absent `queries_per_subscriber` is already the evidence.
    SnapshotMigration.subscriberFormat(hasQueriesPerSubscriber = false) shouldBe SnapshotMigration.Version.V2_1_1
    SnapshotMigration.subscriberFormat(hasQueriesPerSubscriber = true) shouldBe SnapshotMigration.Version.V2_2_0
    SnapshotMigration.Version.current shouldBe SnapshotMigration.Version.V2_2_0
  }

  behavior of "every shape a 2.1.1 snapshot can take"

  /** One 2.1.1 snapshot worth reading, and what makes it different from the others.
    *
    * Enumerated rather than generated: the variables are few and their combinations are what matters, so naming
    * each one makes a failure say which shape broke rather than which seed did.
    */
  private case class Variant(
    label: String,
    subscribers: Seq[Notifiable],
    relatedQueries: Set[StandingQueryId],
    latestAnswer: Byte,
    indexAnswer: Option[Byte],
    properties: Map[String, QuineValue] = Map.empty,
    edges: List[HalfEdge] = Nil,
  )

  private val T = persistence.LastNotification.True
  private val F = persistence.LastNotification.False
  private val N = persistence.LastNotification.None

  private val variants: List[Variant] = List(
    Variant("a node subscriber, one query, answered true", Seq(peerSubscriber), Set(queryA), T, Some(T)),
    Variant("a node subscriber, one query, answered false", Seq(peerSubscriber), Set(queryA), F, Some(F)),
    Variant("a node subscriber, never answered", Seq(peerSubscriber), Set(queryA), N, None),
    Variant("a query subscriber, which is the pattern's own root", Seq(querySubscriber), Set(queryA), T, Some(T)),
    Variant("both kinds of subscriber at once", Seq(peerSubscriber, querySubscriber), Set(queryA, queryB), T, Some(T)),
    Variant("several queries in the union", Seq(peerSubscriber), Set(queryA, queryB), T, Some(T)),
    Variant("an empty union, which 2.1.1 read as a dead subscription", Seq(peerSubscriber), Set.empty, T, Some(T)),
    Variant("no subscribers at all", Seq.empty, Set(queryA), N, Some(T)),
    Variant("an answer held with no subscription to explain it", Seq.empty, Set.empty, N, Some(T)),
    Variant("a peer asked but not yet answered", Seq(peerSubscriber), Set(queryA), T, Some(N)),
    Variant("no index entry, so nothing was ever asked", Seq(peerSubscriber), Set(queryA), T, None),
    Variant(
      "properties and edges alongside the bookkeeping",
      Seq(peerSubscriber),
      Set(queryA),
      T,
      Some(T),
      properties = Map("kind" -> QuineValue.Str("k")),
      edges = List(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, peerNode)),
    ),
  )

  variants.foreach { v =>
    it should s"read ${v.label}" in {
      val decoded = decode(
        snapshotBytes_2_1_1(
          subscribers = v.subscribers,
          relatedQueries = v.relatedQueries,
          latestAnswer = v.latestAnswer,
          indexAnswer = v.indexAnswer,
          properties = v.properties,
          edges = v.edges,
        ),
      )

      // Properties and edges are unchanged by the migration and are checked so that a variant carrying them
      // cannot pass by having quietly lost them.
      withClue("properties: ")(
        decoded.properties.map { case (k, pv) => k.name -> pv.deserialized.get } shouldBe
        v.properties,
      )
      withClue("edges: ")(decoded.edges.toSet shouldBe v.edges.toSet)

      val subscription = decoded.subscribersToThisNode.get(dgnId)
      if (v.subscribers.isEmpty)
        // 2.1.1 wrote a `Subscriber` record whichever way; with nobody in it there is nothing to attribute, and
        // the entry decodes as a subscription with no subscribers.
        withClue("a record with no subscribers: ")(subscription.map(_.subscribers) shouldBe Some(Set.empty))
      else {
        val restored = subscription.getOrElse(fail(s"no subscription decoded for $dgnId"))
        withClue("every subscriber listed survives: ")(restored.subscribers shouldBe v.subscribers.toSet)
        withClue("the answer is carried across exactly: ")(
          restored.latestAnswer shouldBe readExpected(v.latestAnswer),
        )
        // The heart of the migration: never fewer queries than the subscriber really depended on, because
        // whatever that was is inside the union 2.1.1 recorded.
        restored.subscribers.foreach { sub =>
          withClue(s"$sub is credited with at least the union: ")(
            restored.queriesFor(sub) should contain allElementsOf v.relatedQueries,
          )
        }
      }

      v.indexAnswer.foreach { answer =>
        val result = decoded
          .domainNodeIndex(peerNode)
          .getOrElse(childDgnId, fail("no index entry decoded"))
        withClue("the peer's answer is carried across exactly: ")(result.answer shouldBe readExpected(answer))
        withClue("and records no queries, which the node fills in at its next wake: ")(
          result.forQueries shouldBe Set.empty[StandingQueryId],
        )
      }
    }
  }

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private def readExpected(b: Byte): Option[Boolean] =
    if (b == T) Some(true) else if (b == F) Some(false) else None

  private def captureSink(
    into: java.util.concurrent.ConcurrentLinkedQueue[StandingQueryResult],
  ): org.apache.pekko.stream.scaladsl.Sink[StandingQueryResult, org.apache.pekko.stream.UniqueKillSwitch] =
    Flow[StandingQueryResult]
      .viaMat(KillSwitches.single)(Keep.right)
      .map { r => into.add(r); MasterStream.SqResultsExecToken("migration") }
      .to(Sink.ignore)

  /** Register a DistinctId standing query for `pkg` under `sqId`, and wait for it to reach the nodes. */
  private def registerDistinctId(
    graph: GraphService,
    pkg: com.thatdot.quine.model.DomainGraphNodePackage,
    sqId: StandingQueryId,
    outputs: Map[
      String,
      org.apache.pekko.stream.scaladsl.Sink[StandingQueryResult, org.apache.pekko.stream.UniqueKillSwitch],
    ],
  ): Unit = {
    Await.result(
      graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(pkg, sqId, skipPersistor = true),
      30.seconds,
    )
    val sqns = graph.standingQueries(defaultNamespaceId).getOrElse(fail("default namespace"))
    sqns.createStandingQuery(
      name = s"migrated-${sqId.uuid}",
      pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
        dgnId = pkg.dgnId,
        formatReturnAsStr = false,
        aliasReturnAs = Symbol("id"),
        includeCancellation = false,
        origin = PatternOrigin.DirectDgb,
      ),
      outputs = outputs,
      sqId = sqId,
    )
    Await.result(sqns.propagateStandingQueries(None), 30.seconds)
  }

  /** Journals kept per graph, so a test can see what one node caused another to write. A message sent to a peer
    * is journaled by the peer when it applies it, so the peer's journal is the record of what it was sent.
    */
  private class Journals {
    val nodeChange: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] =
      new ConcurrentHashMap()
    val domainIndex: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
      new ConcurrentHashMap()
    def domainIndexEventsOn(qid: QuineId): List[DomainIndexEvent] =
      Option(domainIndex.get(qid)).toList.flatMap(_.values.asScala)
  }

  private def withMigrationGraph(name: String, idProvider: QuineIdLongProvider, journals: Journals)(
    body: GraphService => Any,
  ): Unit = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        PersistenceConfig(),
        None,
        (pc, ns) =>
          new InMemoryPersistor(
            journals = journals.nodeChange,
            domainIndexEvents = journals.domainIndex,
            persistenceConfig = pc,
            namespace = ns,
          ),
      )(Materializer.matFromSystem(system), logConfig)
    val graph = Await.result(
      GraphService(
        name,
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      30.seconds,
    )
    try {
      graph.requiredGraphIsReady()
      val _ = body(graph)
    } finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  private def withMigrationGraph(name: String, idProvider: QuineIdLongProvider)(body: GraphService => Any): Unit = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        PersistenceConfig(),
        None,
        (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
      )(Materializer.matFromSystem(system), logConfig)
    val graph = Await.result(
      GraphService(
        name,
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      30.seconds,
    )
    try {
      graph.requiredGraphIsReady()
      val _ = body(graph)
    } finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  behavior of "a live 2.2.0 node restored from a 2.1.1 snapshot"

  /** The migration end to end: not "do these bytes decode" but "does a node built from them work".
    *
    * Two nodes are seeded with 2.1.1 snapshots describing a two-hop pattern that already matched and had already
    * been reported. Nothing else exists -- no journal, no 2.2.0 snapshot -- so everything each node knows came
    * through the migration. Then the leaf stops matching and matches again, which is a rise the query is owed a
    * report for, and getting it requires every migrated piece to be right: the root has to have the query as a
    * subscriber, the leaf has to have the root as one, and the root has to know what it last reported so the rise
    * reads as a change.
    */
  it should "report a change that depends on every part of the migrated state" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)

    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val twoHopPackage = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()

    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        PersistenceConfig(),
        None,
        (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
      )(Materializer.matFromSystem(system), logConfig)

    val graph = Await.result(
      GraphService(
        "migration-live-restore",
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      30.seconds,
    )
    try {
      graph.requiredGraphIsReady()
      val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor"))

      // The root: matches locally, holds an edge to the leaf, holds the leaf's `true`, and has already told the
      // query so. Written the way 2.1.1 wrote it -- the union in `related_queries`, nothing per subscriber, and
      // nothing at all about which queries its index entry was asked for.
      Await.result(
        persistor.persistSnapshot(
          rootNode,
          EventTime.MinValue,
          snapshotBytes_2_1_1(
            subscribers = Seq(Right(sqId)),
            relatedQueries = Set(sqId),
            latestAnswer = persistence.LastNotification.True,
            indexAnswer = Some(persistence.LastNotification.True),
            subscribedPattern = twoHopPackage.dgnId,
            indexChild = regionDgn,
            indexPeer = leafNode,
            properties = Map("kind" -> QuineValue.Str("k")),
            edges = List(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, leafNode)),
          ),
        ),
        30.seconds,
      )

      // The leaf: matches, and has the root as a subscriber for the shared child pattern.
      Await.result(
        persistor.persistSnapshot(
          leafNode,
          EventTime.MinValue,
          snapshotBytes_2_1_1(
            subscribers = Seq(Left(rootNode)),
            relatedQueries = Set(sqId),
            latestAnswer = persistence.LastNotification.True,
            indexAnswer = None,
            subscribedPattern = regionDgn,
            properties = Map("region" -> QuineValue.Str("r")),
          ),
        ),
        30.seconds,
      )

      val captured = new java.util.concurrent.ConcurrentLinkedQueue[StandingQueryResult]()
      Await.result(
        graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(twoHopPackage, sqId, skipPersistor = true),
        30.seconds,
      )
      val sqns = graph.standingQueries(defaultNamespaceId).getOrElse(fail("default namespace"))
      sqns.createStandingQuery(
        name = "migrated",
        pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
          dgnId = twoHopPackage.dgnId,
          formatReturnAsStr = false,
          aliasReturnAs = Symbol("id"),
          includeCancellation = false,
          origin = PatternOrigin.DirectDgb,
        ),
        outputs = Map(
          "capture" -> Flow[StandingQueryResult]
            .viaMat(KillSwitches.single)(Keep.right)
            .map { r => captured.add(r); MasterStream.SqResultsExecToken("migration-live-restore") }
            .to(Sink.ignore),
        ),
        sqId = sqId,
      )
      Await.result(sqns.propagateStandingQueries(None), 30.seconds)

      // Waking the root is the restore. Reading it is also how the migrated state is inspected.
      val rootState = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)
      val subs = rootState.sqStateResults.subscribers.filter(_.dgnId == twoHopPackage.dgnId)
      withClue("the query is a subscriber, migrated from `notifiable`: ")(
        subs.flatMap(_.subscriberQuery).toSet shouldBe Set(sqId),
      )
      withClue("and is credited with the query it depends on, migrated from `related_queries`: ")(
        subs.flatMap(_.forQueries).toSet shouldBe Set(sqId),
      )
      withClue("and the root knows what it last reported, so a rise will read as a change: ")(
        subs.flatMap(_.lastResult).toSet shouldBe Set(true),
      )
      val held = rootState.sqStateResults.subscriptions.filter(_.dgnId == regionDgn)
      withClue("the leaf's answer came across: ")(held.flatMap(_.answer).toSet shouldBe Set(true))
      withClue("and has been attributed at wake, since 2.1.1 recorded no queries for it: ")(
        held.flatMap(_.forQueries).toSet shouldBe Set(sqId),
      )

      withClue("a restore must not re-report the match 2.1.1 had already reported: ")(
        captured.asScala.count(_.meta.isPositiveMatch) shouldBe 0,
      )

      // Now make it fall and rise. Reporting the rise needs every migrated piece above to be right.
      val ops = graph.literalOps(defaultNamespaceId)
      Await.result(ops.removeProp(leafNode, "region"), 30.seconds)
      Await.result(ops.setProp(leafNode, "region", QuineValue.Str("r2")), 30.seconds)

      val deadline = System.nanoTime() + 30.seconds.toNanos
      while (captured.asScala.count(_.meta.isPositiveMatch) < 1 && System.nanoTime() < deadline) Thread.sleep(10)
      withClue("the query is owed a match once the leaf matches again: ")(
        captured.asScala.count(_.meta.isPositiveMatch) shouldBe 1,
      )
    } finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  behavior of "every shape a 2.1.1 snapshot can take, exhaustively"

  /** A 2.1.1 snapshot of arbitrary shape: any number of subscription records, each listing any number of
    * subscribers and any union of queries, and any number of peers each holding any number of answers.
    */
  private def snapshotBytes_2_1_1_general(
    properties: Map[String, QuineValue],
    edges: List[HalfEdge],
    subscriptions: List[(DomainGraphNodeId, Seq[Notifiable], Set[StandingQueryId], Byte)],
    index: List[(QuineId, List[(DomainGraphNodeId, Byte)])],
  ): Array[Byte] = {
    val builder = new FlatBufferBuilder()

    val subscriberOffs = subscriptions.map { case (dgn, subscribers, related, latest) =>
      val (types, offsets) = subscribers.map {
        case Left(qid) => (persistence.Notifiable.QuineId, writeQuineId(builder, qid))
        case Right(sqId) => (persistence.Notifiable.StandingQueryId, writeStandingQueryId(builder, sqId))
      }.unzip
      val notifiableType = persistence.Subscriber.createNotifiableTypeVector(builder, types.toArray)
      val notifiable = persistence.Subscriber.createNotifiableVector(builder, offsets.toArray)
      val relatedOff = persistence.Subscriber.createRelatedQueriesVector(
        builder,
        related.toArray.map(writeStandingQueryId(builder, _)),
      )
      persistence.Subscriber.createSubscriber(
        builder,
        dgn,
        notifiableType,
        notifiable,
        latest,
        relatedOff,
        NoOffset, // `queries_per_subscriber` did not exist in 2.1.1
      )
    }
    val subscribersVector =
      if (subscriberOffs.isEmpty) NoOffset
      else persistence.NodeSnapshot.createSubscribersVector(builder, subscriberOffs.toArray)

    val indexOffs = index.map { case (peer, children) =>
      val queryOffs = children.map { case (child, answer) =>
        persistence.NodeIndexQuery.createNodeIndexQuery(builder, child, answer, NoOffset) // no `for_queries` in 2.1.1
      }
      val queries = persistence.NodeIndex.createQueriesVector(builder, queryOffs.toArray)
      persistence.NodeIndex.createNodeIndex(builder, writeQuineId(builder, peer), queries)
    }
    val indexVector =
      if (indexOffs.isEmpty) NoOffset
      else persistence.NodeSnapshot.createDomainNodeIndexVector(builder, indexOffs.toArray)

    val propertiesOff =
      if (properties.isEmpty) NoOffset
      else
        persistence.NodeSnapshot.createPropertiesVector(
          builder,
          properties.toArray.map { case (k, v) =>
            persistence.Property.createProperty(
              builder,
              builder.createString(k),
              persistence.Property.createValueVector(builder, PropertyValue(v).serialized),
            )
          },
        )
    val edgesOff =
      if (edges.isEmpty) NoOffset
      else persistence.NodeSnapshot.createEdgesVector(builder, edges.toArray.map(writeHalfEdgeLocal(builder, _)))

    val snapshot = persistence.NodeSnapshot.createNodeSnapshot(
      builder,
      EventTime.MinValue.eventTime,
      propertiesOff,
      edgesOff,
      subscribersVector,
      indexVector,
      false,
    )
    builder.prep(8, 0)
    builder.finish(snapshot)
    Packing.pack(builder.sizedByteArray())
  }

  /** Every collection is exercised at each size from empty to three. Empty and singleton are where a decoder
    * goes wrong -- nothing to read, and no iteration to speak of -- and two and three distinguish "handles a
    * loop" from "handles the first element twice".
    */
  private val cardinalities = List(0, 1, 2, 3)
  private val answerBytes = List(T, F, N)

  /** The subscriber list varies in kind as well as in size, because the two kinds decode through different union
    * branches and a standing query subscriber was invisible to `logState` until recently.
    */
  private val subscriberShapes: List[(String, Seq[Notifiable])] = List(
    "no subscribers" -> Seq.empty,
    "one node" -> Seq(Left(QuineId(Array(11.toByte)))),
    "one query" -> Seq(Right(StandingQueryId(new UUID(0L, 11L)))),
    "two, both kinds" -> Seq(Left(QuineId(Array(11.toByte))), Right(StandingQueryId(new UUID(0L, 11L)))),
    "three, both kinds" -> Seq(
      Left(QuineId(Array(11.toByte))),
      Left(QuineId(Array(12.toByte))),
      Right(StandingQueryId(new UUID(0L, 11L))),
    ),
  )

  private def queriesOfSize(n: Int): Set[StandingQueryId] =
    (1 to n).map(i => StandingQueryId(new UUID(1L, i.toLong))).toSet

  it should "deserialise sensibly in every combination of shapes" in {
    var checked = 0
    for {
      propCount <- cardinalities
      edgeCount <- cardinalities
      subscriptionCount <- cardinalities
      (shapeLabel, subscriberShape) <- subscriberShapes
      relatedCount <- cardinalities
      latest <- answerBytes
      peerCount <- cardinalities
      childCount <- cardinalities
      idxAnswer <- answerBytes
    } {
      val props = (1 to propCount).map(i => s"p$i" -> QuineValue.Integer(i.toLong)).toMap
      val halfEdges =
        (1 to edgeCount).map(i => HalfEdge(Symbol(s"e$i"), EdgeDirection.Outgoing, QuineId(Array(i.toByte)))).toList
      val related = queriesOfSize(relatedCount)
      val subs = (1 to subscriptionCount).map(i => (i.toLong * 100L, subscriberShape, related, latest)).toList
      val peers = (1 to peerCount).map { p =>
        QuineId(Array((50 + p).toByte)) -> (1 to childCount).map(c => (c.toLong * 7L, idxAnswer)).toList
      }.toList

      val clue =
        s"props=$propCount edges=$edgeCount subscriptions=$subscriptionCount subscribers=$shapeLabel " +
        s"related=$relatedCount latest=$latest peers=$peerCount children=$childCount answer=$idxAnswer"

      withClue(s"$clue: ") {
        val decoded = decode(snapshotBytes_2_1_1_general(props, halfEdges, subs, peers))

        withClue("properties: ")(decoded.properties.size shouldBe propCount)
        withClue("edges: ")(decoded.edges.toSet.size shouldBe edgeCount)
        withClue("subscription records: ")(decoded.subscribersToThisNode.size shouldBe subscriptionCount)

        decoded.subscribersToThisNode.foreach { case (_, subscription) =>
          withClue("subscribers survive: ")(subscription.subscribers shouldBe subscriberShape.toSet)
          withClue("the answer is exact: ")(subscription.latestAnswer shouldBe readExpected(latest))
          // The migration's safety property, over every shape: never fewer queries than were recorded.
          subscription.subscribers.foreach { sub =>
            withClue(s"$sub credited with at least the union: ")(
              subscription.queriesFor(sub) should contain allElementsOf related,
            )
          }
        }

        withClue("index peers: ")(decoded.domainNodeIndex.size shouldBe peerCount)
        decoded.domainNodeIndex.foreach { case (_, byChild) =>
          withClue("answers per peer: ")(byChild.size shouldBe childCount)
          byChild.foreach { case (_, result) =>
            withClue("the peer's answer is exact: ")(result.answer shouldBe readExpected(idxAnswer))
            withClue("and records no queries, to be attributed at wake: ")(
              result.forQueries shouldBe Set.empty[StandingQueryId],
            )
          }
        }

        // Writing what was read and reading it again must not move: the migration happens once, and a snapshot
        // this build writes has to be a fixed point of its own decoder.
        val again = decode(NodeSnapshot.snapshotCodec.format.write(decoded))
        withClue("re-encoding is stable, properties: ")(again.properties shouldBe decoded.properties)
        withClue("re-encoding is stable, edges: ")(again.edges.toSet shouldBe decoded.edges.toSet)
        withClue("re-encoding is stable, subscribers: ")(
          again.subscribersToThisNode shouldBe decoded.subscribersToThisNode,
        )
        withClue("re-encoding is stable, index: ")(again.domainNodeIndex shouldBe decoded.domainNodeIndex)
      }
      checked += 1
    }
    val expected = List(
      cardinalities.size, // properties
      cardinalities.size, // edges
      cardinalities.size, // subscription records
      subscriberShapes.size, // subscribers within each record, by size and kind
      cardinalities.size, // queries in the union
      answerBytes.size, // last notification
      cardinalities.size, // peers in the index
      cardinalities.size, // answers per peer
      answerBytes.size, // each answer
    ).product
    withClue(s"every combination was exercised ($expected of them): ")(checked shouldBe expected)
  }

  behavior of "a 2.1.1 snapshot with 2.1.1 journal events written after it"

  /** The shape a live upgrade actually produces.
    *
    * A node that was running 2.1.1 had a snapshot and then went on journaling. Restarting into 2.2.0 means reading
    * that snapshot *and* applying the events written after it -- which the tests above do not cover, because they
    * seed a snapshot and nothing else.
    *
    * The journal itself needs no migration: only `snapshot.fbs` changed between the releases, so `DomainIndexEvent`
    * and `NodeChangeEvent` records written by 2.1.1 decode identically under 2.2.0. What has to work is applying
    * those unchanged events on top of a *migrated* snapshot, and having the result be what the node would have
    * held had it never restarted.
    */
  private def t(seq: Long): EventTime = EventTime(1_000_000L, timestampSequence = seq)

  it should "apply the journal tail on top of the migrated snapshot" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)

    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val twoHopPackage = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()

    withMigrationGraph("migration-snapshot-plus-tail", idProvider) { graph =>
      val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor"))

      // 2.1.1 snapshot: matches, holds the leaf's `true`, and has told the query so.
      Await.result(
        persistor.persistSnapshot(
          rootNode,
          t(0),
          snapshotBytes_2_1_1(
            subscribers = Seq(Right(sqId)),
            relatedQueries = Set(sqId),
            latestAnswer = persistence.LastNotification.True,
            indexAnswer = Some(persistence.LastNotification.True),
            subscribedPattern = twoHopPackage.dgnId,
            indexChild = regionDgn,
            indexPeer = leafNode,
            properties = Map("kind" -> QuineValue.Str("k")),
            edges = List(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, leafNode)),
          ),
        ),
        30.seconds,
      )

      // 2.1.1 journal, written after that snapshot: the leaf withdrew its answer, and a property was set.
      Await.result(
        persistor.persistDomainIndexEvents(
          rootNode,
          NonEmptyList.of(
            NodeEvent.WithTime(DomainIndexEvent.DomainNodeSubscriptionResult(leafNode, regionDgn, result = false), t(1)),
          ),
        ),
        30.seconds,
      )
      Await.result(
        persistor.persistNodeChangeEvents(
          rootNode,
          NonEmptyList.of(
            NodeEvent.WithTime(PropertyEvent.PropertySet(Symbol("extra"), PropertyValue(QuineValue.Str("x"))), t(2)),
          ),
        ),
        30.seconds,
      )

      registerDistinctId(graph, twoHopPackage, sqId, Map.empty)

      val state = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)

      withClue("the property from the journal tail is applied on top of the snapshot: ")(
        state.properties.keySet.map(_.name) shouldBe Set("kind", "extra"),
      )
      val held = state.sqStateResults.subscriptions.filter(_.dgnId == regionDgn)
      withClue("the tail's answer wins over the snapshot's: ")(held.flatMap(_.answer).toSet shouldBe Set(false))
      withClue("and the entry is attributed at wake, since 2.1.1 recorded no queries for it: ")(
        held.flatMap(_.forQueries).toSet shouldBe Set(sqId),
      )
      val subs = state.sqStateResults.subscribers.filter(_.dgnId == twoHopPackage.dgnId)
      withClue("the query is still a subscriber, migrated from `notifiable`: ")(
        subs.flatMap(_.subscriberQuery).toSet shouldBe Set(sqId),
      )
      withClue("and the node re-derived its own answer from the tail rather than keeping the snapshot's: ")(
        subs.flatMap(_.lastResult).toSet shouldBe Set(false),
      )
    }
  }

  it should "report correctly after restoring from a 2.1.1 snapshot plus its journal tail" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)

    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val twoHopPackage = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()
    val captured = new java.util.concurrent.ConcurrentLinkedQueue[StandingQueryResult]()

    withMigrationGraph("migration-tail-then-report", idProvider) { graph =>
      val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor"))

      // The root, as above: snapshot says the leaf matched, the tail says it stopped.
      Await.result(
        persistor.persistSnapshot(
          rootNode,
          t(0),
          snapshotBytes_2_1_1(
            subscribers = Seq(Right(sqId)),
            relatedQueries = Set(sqId),
            latestAnswer = persistence.LastNotification.True,
            indexAnswer = Some(persistence.LastNotification.True),
            subscribedPattern = twoHopPackage.dgnId,
            indexChild = regionDgn,
            indexPeer = leafNode,
            properties = Map("kind" -> QuineValue.Str("k")),
            edges = List(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, leafNode)),
          ),
        ),
        30.seconds,
      )
      Await.result(
        persistor.persistDomainIndexEvents(
          rootNode,
          NonEmptyList.of(
            NodeEvent.WithTime(DomainIndexEvent.DomainNodeSubscriptionResult(leafNode, regionDgn, result = false), t(1)),
          ),
        ),
        30.seconds,
      )

      // The leaf: no `region`, and it last told the root `false`, which is consistent with the root's tail.
      Await.result(
        persistor.persistSnapshot(
          leafNode,
          t(0),
          snapshotBytes_2_1_1(
            subscribers = Seq(Left(rootNode)),
            relatedQueries = Set(sqId),
            latestAnswer = persistence.LastNotification.False,
            indexAnswer = None,
            subscribedPattern = regionDgn,
          ),
        ),
        30.seconds,
      )

      registerDistinctId(graph, twoHopPackage, sqId, Map("capture" -> captureSink(captured)))

      // Wake both, which is the restore.
      val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)
      val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(leafNode), 30.seconds)

      withClue("nothing matches yet, so nothing should have been reported: ")(
        captured.asScala.count(_.meta.isPositiveMatch) shouldBe 0,
      )

      // Now the leaf matches. Reporting this needs the migrated subscription on the leaf, the migrated one on the
      // root, and the root's own answer to have come from the tail rather than the snapshot -- otherwise the root
      // still thinks it reported `true` and this rise is not a change to it.
      Await.result(
        graph.literalOps(defaultNamespaceId).setProp(leafNode, "region", QuineValue.Str("r")),
        30.seconds,
      )

      val deadline = System.nanoTime() + 30.seconds.toNanos
      while (captured.asScala.count(_.meta.isPositiveMatch) < 1 && System.nanoTime() < deadline) Thread.sleep(10)
      withClue("the query is owed a match once the leaf matches: ")(
        captured.asScala.count(_.meta.isPositiveMatch) shouldBe 1,
      )
    }
  }

  behavior of "a 2.1.1 journal with no snapshot at all"

  /** The fourth shape an upgrade can present, and the one that needs no migration.
    *
    * A node below `snapshot-after-events` never wrote a snapshot, so all 2.2.0 has to read is a 2.1.1 journal.
    * Only `snapshot.fbs` changed between the releases -- `DomainIndexEvent` and `NodeChangeEvent` records are
    * byte-identical -- so nothing is translated here. It is covered because "nothing to translate" is a claim
    * worth testing rather than assuming, and because replay reaches the query attribution by a different route
    * than the snapshot path does: the ask is re-made during replay, so `forQueries` is recorded properly rather
    * than being attributed after the fact.
    */
  private def seedJournalOnly(
    graph: GraphService,
    rootNode: QuineId,
    leafNode: QuineId,
    twoHopDgn: DomainGraphNodeId,
    regionDgn: DomainGraphNodeId,
    sqId: StandingQueryId,
    leafMatches: Boolean,
  ): Unit = {
    val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor"))

    // The root: it became a `kind`, gained an edge to the leaf, was asked about the pattern, and heard back.
    Await.result(
      persistor.persistNodeChangeEvents(
        rootNode,
        NonEmptyList.of(
          NodeEvent.WithTime(PropertyEvent.PropertySet(Symbol("kind"), PropertyValue(QuineValue.Str("k"))), t(0)),
          NodeEvent.WithTime(EdgeEvent.EdgeAdded(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, leafNode)), t(1)),
        ),
      ),
      30.seconds,
    )
    Await.result(
      persistor.persistDomainIndexEvents(
        rootNode,
        NonEmptyList.of(
          NodeEvent
            .WithTime(DomainIndexEvent.CreateDomainStandingQuerySubscription(twoHopDgn, sqId, Set(sqId)), t(2)),
          NodeEvent.WithTime(
            DomainIndexEvent.DomainNodeSubscriptionResult(leafNode, regionDgn, result = leafMatches),
            t(3),
          ),
        ),
      ),
      30.seconds,
    )

    // The leaf: the root asked it about the child pattern, and it became a `region` if this case wants a match.
    if (leafMatches)
      Await.result(
        persistor.persistNodeChangeEvents(
          leafNode,
          NonEmptyList.of(
            NodeEvent.WithTime(PropertyEvent.PropertySet(Symbol("region"), PropertyValue(QuineValue.Str("r"))), t(0)),
          ),
        ),
        30.seconds,
      )
    Await.result(
      persistor.persistDomainIndexEvents(
        leafNode,
        NonEmptyList.of(
          NodeEvent.WithTime(DomainIndexEvent.CreateDomainNodeSubscription(regionDgn, rootNode, Set(sqId)), t(1)),
        ),
      ),
      30.seconds,
    )
  }

  it should "rebuild a node from a 2.1.1 journal with nothing translated" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)
    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val pkg = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()

    withMigrationGraph("migration-journal-only", idProvider) { graph =>
      seedJournalOnly(graph, rootNode, leafNode, pkg.dgnId, regionDgn, sqId, leafMatches = true)
      registerDistinctId(graph, pkg, sqId, Map.empty)

      val state = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)
      withClue("properties from the journal: ")(state.properties.keySet.map(_.name) shouldBe Set("kind"))
      withClue("edges from the journal: ")(state.edges.map(_.other) shouldBe Set(leafNode))

      val subs = state.sqStateResults.subscribers.filter(_.dgnId == pkg.dgnId)
      withClue("the query is a subscriber: ")(subs.flatMap(_.subscriberQuery).toSet shouldBe Set(sqId))
      withClue("credited with the query the subscription event named: ")(
        subs.flatMap(_.forQueries).toSet shouldBe Set(sqId),
      )
      withClue("and the node knows it reported a match: ")(subs.flatMap(_.lastResult).toSet shouldBe Set(true))

      val held = state.sqStateResults.subscriptions.filter(_.dgnId == regionDgn)
      withClue("the peer's answer from the journal: ")(held.flatMap(_.answer).toSet shouldBe Set(true))
      // The contrast with the snapshot path: replay re-makes the ask, so the queries are recorded as they happen
      // rather than being attributed after the fact at wake.
      withClue("with its queries recorded by the replayed ask, not attributed afterwards: ")(
        held.flatMap(_.forQueries).toSet shouldBe Set(sqId),
      )
    }
  }

  it should "report correctly after restoring from a 2.1.1 journal alone" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)
    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val pkg = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()
    val captured = new java.util.concurrent.ConcurrentLinkedQueue[StandingQueryResult]()

    withMigrationGraph("migration-journal-only-report", idProvider) { graph =>
      // The leaf never matched, and the root's journal records it hearing so.
      seedJournalOnly(graph, rootNode, leafNode, pkg.dgnId, regionDgn, sqId, leafMatches = false)
      registerDistinctId(graph, pkg, sqId, Map("capture" -> captureSink(captured)))

      val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)
      val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(leafNode), 30.seconds)
      withClue("nothing matched, so nothing should have been reported by the restore: ")(
        captured.asScala.count(_.meta.isPositiveMatch) shouldBe 0,
      )

      Await.result(
        graph.literalOps(defaultNamespaceId).setProp(leafNode, "region", QuineValue.Str("r")),
        30.seconds,
      )

      val deadline = System.nanoTime() + 30.seconds.toNanos
      while (captured.asScala.count(_.meta.isPositiveMatch) < 1 && System.nanoTime() < deadline) Thread.sleep(10)
      withClue("the query is owed a match once the leaf matches: ")(
        captured.asScala.count(_.meta.isPositiveMatch) shouldBe 1,
      )
    }
  }

  behavior of "replaying a journal"

  /** Replay repopulates a node and sends nothing.
    *
    * Folding a journal is how a node remembers what it already did, so it must not *redo* it: every message the
    * recorded events originally caused has already been delivered, and sending them again would subscribe peers
    * afresh and re-report matches. `shouldCauseSideEffects = false` is threaded through the whole fold for this
    * reason, and this is the test of that rather than of the flag.
    *
    * Observed through the peer's journal: a node that is sent a subscription journals it when it applies it, so an
    * empty journal on the peer is the evidence that nothing was sent.
    *
    * The same holds for the rest of waking, which the test below covers: a node finishes waking before it handles
    * its mailbox, and the answer to whatever it asked before it slept is very likely already sitting there.
    *
    * Reading the leaf's journal needs an ordering argument, not a wait. A send from the root's wake is enqueued for
    * the leaf before the root replies to `logState`, so probing the *leaf* after that reply puts our probe behind
    * any such message in the leaf's FIFO mailbox: when the probe answers, the leaf has already journaled anything
    * it was sent.
    */
  it should "send nothing to a peer while folding the journal" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)
    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val pkg = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()
    val journals = new Journals

    withMigrationGraph("replay-sends-nothing", idProvider, journals) { graph =>
      // Every question in this journal was answered, so there is nothing for the wake to resume either. Anything
      // arriving at the leaf would therefore have come from the fold.
      seedJournalOnly(graph, rootNode, leafNode, pkg.dgnId, regionDgn, sqId, leafMatches = true)
      // Only what the root asks the leaf counts. Waking the leaf also enrols it in the top-level pattern in its own
      // right -- every node is a candidate root for a DistinctId query -- and that is not a message from the root.
      def askedByRoot: List[DomainIndexEvent.CreateDomainNodeSubscription] =
        journals.domainIndexEventsOn(leafNode).collect {
          case e: DomainIndexEvent.CreateDomainNodeSubscription if e.replyTo == rootNode => e
        }
      val leafSeeded = askedByRoot.size
      registerDistinctId(graph, pkg, sqId, Map.empty)

      val state = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)
      withClue("the fold did repopulate the node, or this proves nothing: ")(
        state.sqStateResults.subscriptions.filter(_.dgnId == regionDgn).flatMap(_.answer).toSet shouldBe Set(true),
      )
      // Puts this read behind anything the root's wake sent to the leaf; see the ordering note above.
      val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(leafNode), 30.seconds)
      withClue("and the leaf was sent nothing by the root: ")(
        askedByRoot.size shouldBe leafSeeded,
      )
    }
  }

  /** Waking sends nothing either, not even for a question whose answer never arrived.
    *
    * This is the tempting case: the journal records that a standing query subscribed here, and the node holds no
    * answer from the peer that pattern depends on. Re-evaluating the pattern at wake would ask the peer again --
    * and must not. A result is not dropped for a sleeping node, so if the question was asked before the sleep the
    * answer is waiting in the mailbox and will be handled as soon as waking finishes; asking again would race it
    * and leave the peer maintaining a second subscription for a question it already answers.
    *
    * Same ordering argument as above: the probe to the leaf is enqueued behind anything the root's wake sent it.
    */
  it should "send nothing to a peer at wake, even for a question with no answer yet" in {
    val idProvider = QuineIdLongProvider()
    val rootNode = idProvider.customIdToQid(1L)
    val leafNode = idProvider.customIdToQid(2L)
    val regionBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
    val twoHopBranch = SingleBranch(
      hasProperty("kind"),
      nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
    )
    val pkg = twoHopBranch.toDomainGraphNodePackage
    val regionDgn = regionBranch.toDomainGraphNodePackage.dgnId
    val sqId = StandingQueryId.fresh()
    val journals = new Journals

    withMigrationGraph("wake-resumes-questions", idProvider, journals) { graph =>
      val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("persistor"))
      // A standing query subscribed to the root, and the root holds no answer for the child pattern.
      Await.result(
        persistor.persistNodeChangeEvents(
          rootNode,
          NonEmptyList.of(
            NodeEvent.WithTime(PropertyEvent.PropertySet(Symbol("kind"), PropertyValue(QuineValue.Str("k"))), t(0)),
            NodeEvent.WithTime(EdgeEvent.EdgeAdded(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, leafNode)), t(1)),
          ),
        ),
        30.seconds,
      )
      Await.result(
        persistor.persistDomainIndexEvents(
          rootNode,
          NonEmptyList.of(
            NodeEvent
              .WithTime(DomainIndexEvent.CreateDomainStandingQuerySubscription(pkg.dgnId, sqId, Set(sqId)), t(2)),
          ),
        ),
        30.seconds,
      )

      registerDistinctId(graph, pkg, sqId, Map.empty)
      val state = Await.result(graph.literalOps(defaultNamespaceId).logState(rootNode), 30.seconds)
      withClue("the fold did restore the standing query's subscription, or this proves nothing: ")(
        state.sqStateResults.subscribers.map(_.dgnId) should contain(pkg.dgnId),
      )

      val _ = Await.result(graph.literalOps(defaultNamespaceId).logState(leafNode), 30.seconds)
      val asked = journals.domainIndexEventsOn(leafNode).collect {
        case e: DomainIndexEvent.CreateDomainNodeSubscription => e
      }
      withClue("the leaf should not have been asked anything by waking: ")(
        asked.filter(e => e.dgnId == regionDgn && e.replyTo == rootNode) shouldBe empty,
      )
    }
  }
}

package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import scala.collection.mutable

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber
import com.thatdot.quine.graph.{ByteBufferOps, MultipleValuesStandingQueryPartId, StandingQueryId, cypher}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

/** Write and read methods for MultipleValuesStandingQueryState values. These translate between the in-memory
  * representation held by nodes to execute multiple-value standing queries (id of the query part, cached results, etc),
  * and the corresponding FlatBuffers representations defined in the interface-definition-language (.fbs file).
  */
object MultipleValuesStandingQueryStateCodec
    extends PersistenceCodec[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)] {

  private[this] def writeQueryContext(
    builder: FlatBufferBuilder,
    qc: cypher.QueryContext,
  ): Offset = {
    // Write reference values
    val env = qc.environment
    val columnOffsets = new Array[Offset](env.size)
    val valueTypeBytes = new Array[Byte](env.size)
    val valueOffsets = new Array[Offset](env.size)
    for (((col, value), i) <- env.zipWithIndex) {
      val TypeAndOffset(valueType, valueOffset) = writeCypherValue(builder, value)
      columnOffsets(i) = builder.createString(col.name)
      valueTypeBytes(i) = valueType
      valueOffsets(i) = valueOffset
    }

    import persistence.{QueryContext => queryC}
    val columnsOffset = queryC.createColumnsVector(builder, columnOffsets)
    val valueTypesOffset = queryC.createValuesTypeVector(builder, valueTypeBytes)
    val valuesOffset = queryC.createValuesVector(builder, valueOffsets)

    // Start, set fields, end
    queryC.createQueryContext(builder, columnsOffset, valueTypesOffset, valuesOffset)
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

  /** Write a series of QueryContext tables followed by a vector containing their offsets.
    * The offset of the vector itself may be absent, allowing this to encode Option[ Seq[cypher.QueryContext] ]
    */
  private[this] def writeMaybeQueryContexts(
    builder: FlatBufferBuilder,
    maybeQueryContexts: Option[Seq[cypher.QueryContext]],
  ): Offset = maybeQueryContexts match {
    case Some(results) =>
      val queryContextOffsets = new Array[Offset](results.length)
      for ((result, i) <- results.zipWithIndex) {
        val resultOffset = writeQueryContext(builder, result)
        queryContextOffsets(i) = resultOffset
      }
      builder.createVectorOfTables(queryContextOffsets)
    case None => NoOffset
  }

  /** Read a vector of persistence.QueryContext tables as an Option[ Seq[cypher.QueryContext] ], allowing a null
    * reference for the vector to indicate None.
    */
  private[this] def readMaybeQueryContexts(
    maybeNullQueryContextVec: persistence.QueryContext.Vector,
  ): Option[Seq[cypher.QueryContext]] =
    Option(maybeNullQueryContextVec).map { queryContextVec =>
      val length = queryContextVec.length()
      var i = 0
      val results = Seq.newBuilder[cypher.QueryContext]
      while (i < length) {
        results += readQueryContext(queryContextVec.get(i))
        i += 1
      }

      results.result()
    }

  /** Write the given Option[ Seq[cypher.QueryContext] ] as a persistence.MultipleValuesStandingQueryResults table.
    * This wrapping is only necessary when used as an element of a containing vector. FlatBuffers doesn't allow nested
    * vectors, so the indirection provided by the table in required. When only encoding a single optional vector as a
    * table field, the field type should just be a vector of persistence.QueryContext, since the field can already
    * represent None by not being set.
    */
  private[this] def writeMultipleValuesStandingQueryResults(
    builder: FlatBufferBuilder,
    maybeResults: Option[Seq[cypher.QueryContext]],
  ): Offset = {
    val vectorOffset = writeMaybeQueryContexts(builder, maybeResults)
    persistence.MultipleValuesStandingQueryResults.createMultipleValuesStandingQueryResults(builder, vectorOffset)
  }

  /** Read a persistence.MultipleValuesStandingQueryResults as an Option[ Seq[cypher.QueryContext] ]. This extra
    * container is only necessary when it is used as an element in a vector to allow optionality.
    */
  private[this] def readMultipleValuesStandingQueryResults(
    results: persistence.MultipleValuesStandingQueryResults,
  ): Option[Seq[cypher.QueryContext]] = readMaybeQueryContexts(results.resultsVector())

  private[this] def writeMultipleValuesCrossStandingQueryState(
    builder: FlatBufferBuilder,
    crossState: cypher.CrossState,
  ): Offset = {
    import persistence.{MultipleValuesCrossStandingQueryState => cross}

    // Write reference values
    val resultsAccumulatorSize = crossState.resultsAccumulator.size
    // results_accumulator_keys is a vector of structs, so we start it, write all the values inline in reverse order,
    // then end it rather than creating it by passing in offsets and letting the create method do the
    // (start, write-in-reverse-order, end) steps for us.
    cross.startResultsAccumulatorKeysVector(builder, resultsAccumulatorSize)
    val reversedKeys = crossState.resultsAccumulator.keys.toIndexedSeq.reverseIterator
    for (partId <- reversedKeys) writeMultipleValuesStandingQueryPartId2(builder, partId)
    val keysVecOffset = builder.endVector()

    val valueOffsets = new Array[Offset](resultsAccumulatorSize)
    for ((maybeResults, i) <- crossState.resultsAccumulator.values.zipWithIndex) {
      val valueOff = writeMultipleValuesStandingQueryResults(builder, maybeResults)
      valueOffsets(i) = valueOff
    }
    val valuesVecOffset = cross.createResultsAccumulatorValuesVector(builder, valueOffsets)

    cross.startMultipleValuesCrossStandingQueryState(builder)

    // Set fields
    val queryPartIdOffset: Offset = writeMultipleValuesStandingQueryPartId2(builder, crossState.queryPartId) // struct
    cross.addQueryPartId(builder, queryPartIdOffset)
    cross.addResultsAccumulatorKeys(builder, keysVecOffset)
    cross.addResultsAccumulatorValues(builder, valuesVecOffset)

    cross.endMultipleValuesCrossStandingQueryState(builder)
  }

  private[this] def readMultipleValuesCrossStandingQueryState(
    crossState: persistence.MultipleValuesCrossStandingQueryState,
  ): cypher.CrossState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId2(crossState.queryPartId)
    val state = cypher.CrossState(sqId)

    val resultsLength = crossState.resultsAccumulatorKeysLength()
    val keysVec = crossState.resultsAccumulatorKeysVector()
    val valuesVec = crossState.resultsAccumulatorValuesVector()

    var i = 0
    while (i < resultsLength) {
      val queryPartId = readMultipleValuesStandingQueryPartId2(keysVec.get(i))
      val maybeResults = readMultipleValuesStandingQueryResults(valuesVec.get(i))
      state.resultsAccumulator.update(queryPartId, maybeResults)
      i += 1
    }
    state
  }

  private[this] def writeMultipleValuesLocalPropertyStandingQueryState(
    builder: FlatBufferBuilder,
    localPropState: cypher.LocalPropertyState,
  ): Offset = {
    import persistence.{MultipleValuesLocalPropertyStandingQueryState => lp}

    lp.startMultipleValuesLocalPropertyStandingQueryState(builder)

    // Set fields
    val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, localPropState.queryPartId) // struct
    lp.addQueryPartId(builder, queryPartIdOffset)

    lp.endMultipleValuesLocalPropertyStandingQueryState(builder)
  }

  private[this] def readMultipleValuesLocalPropertyStandingQueryState(
    localPropState: persistence.MultipleValuesLocalPropertyStandingQueryState,
  ): cypher.LocalPropertyState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId2(localPropState.queryPartId)
    cypher.LocalPropertyState(sqId)
  }

  private[this] def writeMultipleValuesLocalIdStandingQueryState(
    builder: FlatBufferBuilder,
    localIdState: cypher.LocalIdState,
  ): Offset = {
    import persistence.{MultipleValuesLocalIdStandingQueryState => lid}

    lid.startMultipleValuesLocalIdStandingQueryState(builder)

    // Set fields
    val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, localIdState.queryPartId) // struct
    lid.addQueryPartId(builder, queryPartIdOffset)

    lid.endMultipleValuesLocalIdStandingQueryState(builder)
  }

  private[this] def readMultipleValuesLocalIdStandingQueryState(
    localIdState: persistence.MultipleValuesLocalIdStandingQueryState,
  ): cypher.LocalIdState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId2(localIdState.queryPartId)
    cypher.LocalIdState(sqId)
  }

  private[this] def writeMultipleValuesSubscribeAcrossEdgeStandingQueryState(
    builder: FlatBufferBuilder,
    edgeState: cypher.SubscribeAcrossEdgeState,
  ): Offset = {
    import persistence.{MultipleValuesSubscribeAcrossEdgeStandingQueryState => sub}

    // Write reference values
    val size = edgeState.edgeResults.size
    val keyOffsets = new Array[Offset](size)
    val valueOffsets = new Array[Offset](size)
    for (((halfEdge, maybeResults), i) <- edgeState.edgeResults.zipWithIndex) {
      keyOffsets(i) = writeHalfEdge2(builder, halfEdge)
      valueOffsets(i) = writeMultipleValuesStandingQueryResults(builder, maybeResults)
    }
    val edgeResultsKeysOffset = sub.createEdgeResultsKeysVector(builder, keyOffsets)
    val edgeResultsValuesOffset = sub.createEdgeResultsValuesVector(builder, valueOffsets)

    sub.startMultipleValuesSubscribeAcrossEdgeStandingQueryState(builder)

    // Set fields
    val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, edgeState.queryPartId) // struct
    sub.addQueryPartId(builder, queryPartIdOffset)
    sub.addEdgeResultsKeys(builder, edgeResultsKeysOffset)
    sub.addEdgeResultsValues(builder, edgeResultsValuesOffset)

    sub.endMultipleValuesSubscribeAcrossEdgeStandingQueryState(builder)
  }

  private[this] def readMultipleValuesSubscribeAcrossEdgeStandingQueryState(
    edgeState: persistence.MultipleValuesSubscribeAcrossEdgeStandingQueryState,
  ): cypher.SubscribeAcrossEdgeState = {
    val queryPartId = readMultipleValuesStandingQueryPartId2(edgeState.queryPartId)

    val resultsLength = edgeState.edgeResultsKeysLength()
    val edgeResultsKeysVec = edgeState.edgeResultsKeysVector()
    val edgeResultsValuesVec = edgeState.edgeResultsValuesVector()
    var i = 0
    val state = cypher.SubscribeAcrossEdgeState(queryPartId)
    while (i < resultsLength) {
      val halfEdge = readHalfEdge2(edgeResultsKeysVec.get(i))
      val resultValue = edgeResultsValuesVec.get(i)
      val maybeResults = readMultipleValuesStandingQueryResults(resultValue)
      state.edgeResults += (halfEdge -> maybeResults)
      i += 1
    }
    state
  }

  private[this] def writeMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(
    builder: FlatBufferBuilder,
    edgeState: cypher.EdgeSubscriptionReciprocalState,
  ): Offset = {
    import persistence.{MultipleValuesEdgeSubscriptionReciprocalStandingQueryState => subRec}

    // Write reference values
    val halfEdgeOffset = writeHalfEdge2(builder, edgeState.halfEdge)
    val maybeResultsOffset = writeMaybeQueryContexts(builder, edgeState.cachedResult)

    subRec.startMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(builder)

    // Set fields
    val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, edgeState.queryPartId) // struct
    subRec.addQueryPartId(builder, queryPartIdOffset)
    subRec.addHalfEdge(builder, halfEdgeOffset)
    val andThenIdOffset = writeMultipleValuesStandingQueryPartId2(builder, edgeState.andThenId) // struct
    subRec.addAndThenId(builder, andThenIdOffset)
    subRec.addCurrentlyMatching(builder, edgeState.currentlyMatching)
    subRec.addCachedResult(builder, maybeResultsOffset)

    subRec.endMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(builder)
  }

  private[this] def readMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(
    edgeState: persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState,
  ): cypher.EdgeSubscriptionReciprocalState = {
    val sqId = readMultipleValuesStandingQueryPartId2(edgeState.queryPartId)
    val halfEdge = readHalfEdge2(edgeState.halfEdge)
    val andThenId = readMultipleValuesStandingQueryPartId2(edgeState.andThenId)
    val currentlyMatching = edgeState.currentlyMatching
    val cachedResult = readMaybeQueryContexts(edgeState.cachedResultVector)
    val state = cypher.EdgeSubscriptionReciprocalState(sqId, halfEdge, andThenId)
    state.currentlyMatching = currentlyMatching
    state.cachedResult = cachedResult
    state
  }

  private[this] def writeMultipleValuesFilterMapStandingQueryState(
    builder: FlatBufferBuilder,
    filterState: cypher.FilterMapState,
  ): Offset = {
    import persistence.{MultipleValuesFilterMapStandingQueryState => fm}

    // Write reference values
    val keptResultsOffset = writeMaybeQueryContexts(builder, filterState.keptResults)

    fm.startMultipleValuesFilterMapStandingQueryState(builder)

    // Set fields
    val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, filterState.queryPartId) // struct
    fm.addQueryPartId(builder, queryPartIdOffset)
    fm.addKeptResults(builder, keptResultsOffset)

    fm.endMultipleValuesFilterMapStandingQueryState(builder)
  }

  private[this] def readMultipleValuesFilterMapStandingQueryState(
    filterState: persistence.MultipleValuesFilterMapStandingQueryState,
  ): cypher.FilterMapState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId2(filterState.queryPartId)
    val maybeKeptResults = readMaybeQueryContexts(filterState.keptResultsVector())
    val state = cypher.FilterMapState(sqId)
    state.keptResults = maybeKeptResults
    state
  }

  private[this] def writeMultipleValuesAllPropertiesStandingQueryState(
    builder: FlatBufferBuilder,
    localPropState: cypher.AllPropertiesState,
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, localPropState.queryPartId)
    persistence.MultipleValuesAllPropertiesStandingQueryState.createMultipleValuesAllPropertiesStandingQueryState(
      builder,
      sqIdOff,
    )
  }

  private[this] def readMultipleValuesAllPropertiesStandingQueryState(
    localPropState: persistence.MultipleValuesAllPropertiesStandingQueryState,
  ): cypher.AllPropertiesState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(localPropState.queryPartId)
    cypher.AllPropertiesState(sqId)
  }

  private[this] def writeMultipleValuesLabelsStandingQueryState(
    builder: FlatBufferBuilder,
    labelsState: cypher.LabelsState,
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, labelsState.queryPartId)
    persistence.MultipleValuesLabelsStandingQueryState.createMultipleValuesLabelsStandingQueryState(
      builder,
      sqIdOff,
    )
  }

  private[this] def readMultipleValuesLabelsStandingQueryState(
    labelsState: persistence.MultipleValuesLabelsStandingQueryState,
  ): cypher.LabelsState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(labelsState.queryPartId)
    cypher.LabelsState(sqId)
  }

  private[this] def writeMultipleValuesStandingQuerySubscriber(
    builder: FlatBufferBuilder,
    subscriber: MultipleValuesStandingQuerySubscriber,
  ): TypeAndOffset =
    subscriber match {
      case MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalId, queryId) =>
        import persistence.{CypherNodeSubscriber => ns}

        // Write reference values
        val onNodeOffset = builder.createByteVector(onNode.array)

        ns.startCypherNodeSubscriber(builder)

        // Set fields
        ns.addOnNode(builder, onNodeOffset)
        val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, queryId) // struct
        ns.addQueryPartId(builder, queryPartIdOffset)
        val globalQueryIdOffset = writeStandingQueryId2(builder, globalId) // struct
        ns.addGlobalQueryId(builder, globalQueryIdOffset)

        val nodeSubscriberOffset = ns.endCypherNodeSubscriber(builder)

        TypeAndOffset(persistence.MultipleValuesStandingQuerySubscriber.CypherNodeSubscriber, nodeSubscriberOffset)

      case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(globalId) =>
        import persistence.{CypherGlobalSubscriber => gs}

        gs.startCypherGlobalSubscriber(builder)

        // Set fields
        val globalQueryIdOffset = writeStandingQueryId2(builder, globalId) // struct
        gs.addGlobalQueryId(builder, globalQueryIdOffset)

        val globalSubscriberOffset = gs.endCypherGlobalSubscriber(builder)

        TypeAndOffset(persistence.MultipleValuesStandingQuerySubscriber.CypherGlobalSubscriber, globalSubscriberOffset)
    }

  private[this] def readMultipleValuesStandingQuerySubscriber(
    typ: Byte,
    makeSubscriber: Table => Table,
  ): MultipleValuesStandingQuerySubscriber =
    typ match {
      case persistence.MultipleValuesStandingQuerySubscriber.CypherNodeSubscriber =>
        val nodeSub =
          makeSubscriber(new persistence.CypherNodeSubscriber()).asInstanceOf[persistence.CypherNodeSubscriber]
        val onNode: QuineId = QuineId(nodeSub.onNodeAsByteBuffer.remainingBytes)
        val queryPartId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId2(nodeSub.queryPartId)
        val globalQueryId: StandingQueryId = readStandingQueryId2(nodeSub.globalQueryId)
        MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalQueryId, queryPartId)

      case persistence.MultipleValuesStandingQuerySubscriber.CypherGlobalSubscriber =>
        val globalSub =
          makeSubscriber(new persistence.CypherGlobalSubscriber()).asInstanceOf[persistence.CypherGlobalSubscriber]
        val globalQueryId: StandingQueryId = readStandingQueryId2(globalSub.globalQueryId)
        MultipleValuesStandingQuerySubscriber.GlobalSubscriber(globalQueryId)

      case other =>
        throw new InvalidUnionType(other, persistence.MultipleValuesStandingQueryState.names)
    }

  private[this] def writeMultipleValuesStandingQuerySubscribers(
    builder: FlatBufferBuilder,
    subscribers: MultipleValuesStandingQueryPartSubscription,
  ): Offset = {
    // Write reference values
    val subscriberTypeBytes = new Array[Byte](subscribers.subscribers.size)
    val subscriberOffsets = new Array[Offset](subscribers.subscribers.size)
    for ((sub, i) <- subscribers.subscribers.zipWithIndex) {
      val TypeAndOffset(subTyp, subOff) = writeMultipleValuesStandingQuerySubscriber(builder, sub)
      subscriberTypeBytes(i) = subTyp
      subscriberOffsets(i) = subOff
    }

    import persistence.{MultipleValuesStandingQuerySubscribers => subs}

    // Vectors of unions are represented as two vectors of the same length. One has type tags as bytes, and the other
    // the values. See https://github.com/dvidelabs/flatcc/blob/master/doc/binary-format.md#unions
    val subscriberTypesOffset = subs.createSubscribersTypeVector(builder, subscriberTypeBytes)
    val subscribersOffset = subs.createSubscribersVector(builder, subscriberOffsets)

    subs.startMultipleValuesStandingQuerySubscribers(builder)

    // Set fields
    val queryPartIdOffset = writeMultipleValuesStandingQueryPartId2(builder, subscribers.forQuery) // struct
    subs.addQueryPartId(builder, queryPartIdOffset)
    val globalQueryIdOffset = writeStandingQueryId2(builder, subscribers.globalId) // struct
    subs.addGlobalQueryId(builder, globalQueryIdOffset)
    subs.addSubscribersType(builder, subscriberTypesOffset)
    subs.addSubscribers(builder, subscribersOffset)

    subs.endMultipleValuesStandingQuerySubscribers(builder)
  }

  private[this] def readMultipleValuesStandingQuerySubscribers(
    subscribers: persistence.MultipleValuesStandingQuerySubscribers,
  ): MultipleValuesStandingQueryPartSubscription = {
    val queryPartId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId2(
      subscribers.queryPartId,
    )
    val globalQueryId: StandingQueryId = readStandingQueryId2(subscribers.globalQueryId)
    val subs: mutable.Set[MultipleValuesStandingQuerySubscriber] = {
      val builder = mutable.Set.empty[MultipleValuesStandingQuerySubscriber]
      var i: Int = 0
      val subscribersLength = subscribers.subscribersLength
      while (i < subscribersLength) {
        builder += readMultipleValuesStandingQuerySubscriber(
          subscribers.subscribersType(i),
          subscribers.subscribers(_, i),
        )
        i += 1
      }
      builder
    }
    MultipleValuesStandingQueryPartSubscription(queryPartId, globalQueryId, subs)
  }

  private[this] def writeMultipleValuesStandingQueryState(
    builder: FlatBufferBuilder,
    state: MultipleValuesStandingQueryState,
  ): TypeAndOffset =
    state match {
      case _: cypher.UnitState =>
        persistence.MultipleValuesUnitStandingQueryState.startMultipleValuesUnitStandingQueryState(builder)
        val offset: Offset =
          persistence.MultipleValuesUnitStandingQueryState.endMultipleValuesUnitStandingQueryState(builder)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesUnitStandingQueryState, offset)

      case crossState: cypher.CrossState =>
        val offset: Offset = writeMultipleValuesCrossStandingQueryState(builder, crossState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesCrossStandingQueryState, offset)

      case propState: cypher.LocalPropertyState =>
        val offset: Offset = writeMultipleValuesLocalPropertyStandingQueryState(builder, propState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesLocalPropertyStandingQueryState,
          offset,
        )

      case idState: cypher.LocalIdState =>
        val offset: Offset = writeMultipleValuesLocalIdStandingQueryState(builder, idState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesLocalIdStandingQueryState, offset)

      case edgeState: cypher.SubscribeAcrossEdgeState =>
        val offset: Offset = writeMultipleValuesSubscribeAcrossEdgeStandingQueryState(builder, edgeState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesSubscribeAcrossEdgeStandingQueryState,
          offset,
        )

      case edgeState: cypher.EdgeSubscriptionReciprocalState =>
        val offset: Offset = writeMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(builder, edgeState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState,
          offset,
        )

      case filterState: cypher.FilterMapState =>
        val offset: Offset = writeMultipleValuesFilterMapStandingQueryState(builder, filterState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesFilterMapStandingQueryState, offset)

      case allPropertiesState: cypher.AllPropertiesState =>
        val offset: Offset = writeMultipleValuesAllPropertiesStandingQueryState(builder, allPropertiesState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesAllPropertiesStandingQueryState,
          offset,
        )
      case labelsState: cypher.LabelsState =>
        val offset: Offset = writeMultipleValuesLabelsStandingQueryState(builder, labelsState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesLabelsStandingQueryState, offset)
    }

  private[this] def readMultipleValuesStandingQueryState(
    typ: Byte,
    makeState: Table => Table,
  ): MultipleValuesStandingQueryState =
    typ match {
      case persistence.MultipleValuesStandingQueryState.MultipleValuesUnitStandingQueryState =>
        cypher.UnitState()

      case persistence.MultipleValuesStandingQueryState.MultipleValuesCrossStandingQueryState =>
        val crossState = makeState(new persistence.MultipleValuesCrossStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesCrossStandingQueryState]
        readMultipleValuesCrossStandingQueryState(crossState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesLocalPropertyStandingQueryState =>
        val propState = makeState(new persistence.MultipleValuesLocalPropertyStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesLocalPropertyStandingQueryState]
        readMultipleValuesLocalPropertyStandingQueryState(propState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesLocalIdStandingQueryState =>
        val idState = makeState(new persistence.MultipleValuesLocalIdStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesLocalIdStandingQueryState]
        readMultipleValuesLocalIdStandingQueryState(idState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesSubscribeAcrossEdgeStandingQueryState =>
        val edgeState = makeState(new persistence.MultipleValuesSubscribeAcrossEdgeStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesSubscribeAcrossEdgeStandingQueryState]
        readMultipleValuesSubscribeAcrossEdgeStandingQueryState(edgeState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState =>
        val edgeState = makeState(new persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState]
        readMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(edgeState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesFilterMapStandingQueryState =>
        val filterState = makeState(new persistence.MultipleValuesFilterMapStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesFilterMapStandingQueryState]
        readMultipleValuesFilterMapStandingQueryState(filterState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesAllPropertiesStandingQueryState =>
        val propState = makeState(new persistence.MultipleValuesAllPropertiesStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesAllPropertiesStandingQueryState]
        readMultipleValuesAllPropertiesStandingQueryState(propState)

      case persistence.MultipleValuesStandingQueryState.MultipleValuesLabelsStandingQueryState =>
        val labelsState = makeState(new persistence.MultipleValuesLabelsStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesLabelsStandingQueryState]
        readMultipleValuesLabelsStandingQueryState(labelsState)

      case other =>
        throw new InvalidUnionType(other, persistence.MultipleValuesStandingQueryState.names)
    }

  private[this] def writeMultipleValuesStandingQueryStateAndSubscribers(
    builder: FlatBufferBuilder,
    state: MultipleValuesStandingQueryState,
    subscribers: MultipleValuesStandingQueryPartSubscription,
  ): Offset = {
    val TypeAndOffset(stateType, stateOffset) = writeMultipleValuesStandingQueryState(builder, state)
    val subscribersOffset = writeMultipleValuesStandingQuerySubscribers(builder, subscribers)
    persistence.MultipleValuesStandingQueryStateAndSubscribers.createMultipleValuesStandingQueryStateAndSubscribers(
      builder,
      subscribersOffset,
      stateType,
      stateOffset,
    )
  }

  private[this] def readMultipleValuesStandingQueryStateAndSubscribers(
    stateAndSubs: persistence.MultipleValuesStandingQueryStateAndSubscribers,
  ): (MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState) = {
    val state = readMultipleValuesStandingQueryState(stateAndSubs.stateType, stateAndSubs.state)
    val subscribers = readMultipleValuesStandingQuerySubscribers(stateAndSubs.subscribers)
    subscribers -> state
  }
  val format: BinaryFormat[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)] =
    new PackedFlatBufferBinaryFormat[(MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState)] {
      def writeToBuffer(
        builder: FlatBufferBuilder,
        state: (MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState),
      ): Offset =
        writeMultipleValuesStandingQueryStateAndSubscribers(builder, state._2, state._1)

      def readFromBuffer(
        buffer: ByteBuffer,
      ): (MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState) =
        readMultipleValuesStandingQueryStateAndSubscribers(
          persistence.MultipleValuesStandingQueryStateAndSubscribers
            .getRootAsMultipleValuesStandingQueryStateAndSubscribers(buffer),
        )
    }
}

package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.behavior.MultipleValuesStandingQuerySubscribers
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{MultipleValuesStandingQuerySubscriber, ResultId}
import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, StandingQueryId, cypher}
import com.thatdot.quine.model.{HalfEdge, QuineId}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object MultipleValuesStandingQueryStateCodec
    extends PersistenceCodec[(MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)] {

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

  private[this] def readStandingQueryResultId(resId: persistence.StandingQueryResultId): ResultId =
    ResultId(new UUID(resId.highBytes, resId.lowBytes))

  private[this] def writeStandingQueryResultId(builder: FlatBufferBuilder, resId: ResultId): Offset =
    persistence.StandingQueryResultId.createStandingQueryResultId(
      builder,
      resId.uuid.getLeastSignificantBits,
      resId.uuid.getMostSignificantBits
    )

  private[this] def writeMultipleValuesUnitStandingQueryState(
    builder: FlatBufferBuilder,
    unitState: cypher.UnitState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, unitState.queryPartId)
    val resIdOff: Offset = unitState.resultId match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.MultipleValuesUnitStandingQueryState.createMultipleValuesUnitStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readMultipleValuesUnitStandingQueryState(
    unitState: persistence.MultipleValuesUnitStandingQueryState
  ): cypher.UnitState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(unitState.queryPartId)
    val resId: Option[ResultId] = Option(unitState.resultId).map(readStandingQueryResultId)
    cypher.UnitState(sqId, resId)
  }

  private[this] def writeMultipleValuesCrossStandingQueryState(
    builder: FlatBufferBuilder,
    crossState: cypher.CrossState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, crossState.queryPartId)
    val accumulatedResultsOff: Offset = {
      val accumulatedResultOffs: Array[Offset] = new Array(crossState.accumulatedResults.length)
      for ((accRes, i) <- crossState.accumulatedResults.zipWithIndex) {
        val resultsOff: Offset = {
          val resultOffs: Array[Offset] = new Array(accRes.size)
          for (((resId, qc), j) <- accRes.zipWithIndex) {
            val resIdOff: Offset = writeStandingQueryResultId(builder, resId)
            val resValueOff: Offset = writeQueryContext(builder, qc)
            resultOffs(j) = persistence.MultipleValuesStandingQueryResult.createMultipleValuesStandingQueryResult(
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
      persistence.MultipleValuesCrossStandingQueryState.createAccumulatedResultsVector(builder, accumulatedResultOffs)
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
      persistence.MultipleValuesCrossStandingQueryState.createResultDependencyVector(builder, resultDependencyOffs)
    }
    persistence.MultipleValuesCrossStandingQueryState.createMultipleValuesCrossStandingQueryState(
      builder,
      sqIdOff,
      crossState.subscriptionsEmitted,
      accumulatedResultsOff,
      resultDependencyOff
    )
  }

  private[this] def readMultipleValuesCrossStandingQueryState(
    crossState: persistence.MultipleValuesCrossStandingQueryState
  ): cypher.CrossState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(crossState.queryPartId)
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
          val qr: persistence.MultipleValuesStandingQueryResult = accRes.result(j)
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

  private[this] def writeMultipleValuesLocalPropertyStandingQueryState(
    builder: FlatBufferBuilder,
    localPropState: cypher.LocalPropertyState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, localPropState.queryPartId)
    val resIdOff: Offset = localPropState.currentResult match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.MultipleValuesLocalPropertyStandingQueryState.createMultipleValuesLocalPropertyStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readMultipleValuesLocalPropertyStandingQueryState(
    localPropState: persistence.MultipleValuesLocalPropertyStandingQueryState
  ): cypher.LocalPropertyState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(localPropState.queryPartId)
    val resId: Option[ResultId] = Option(localPropState.resultId).map(readStandingQueryResultId)
    cypher.LocalPropertyState(sqId, resId)
  }

  private[this] def writeMultipleValuesLocalIdStandingQueryState(
    builder: FlatBufferBuilder,
    localIdState: cypher.LocalIdState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, localIdState.queryPartId)
    val resIdOff: Offset = localIdState.resultId match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.MultipleValuesLocalIdStandingQueryState.createMultipleValuesLocalIdStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readMultipleValuesLocalIdStandingQueryState(
    localIdState: persistence.MultipleValuesLocalIdStandingQueryState
  ): cypher.LocalIdState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(localIdState.queryPartId)
    val resId: Option[ResultId] = Option(localIdState.resultId).map(readStandingQueryResultId)
    cypher.LocalIdState(sqId, resId)
  }

  private[this] def writeMultipleValuesSubscribeAcrossEdgeStandingQueryState(
    builder: FlatBufferBuilder,
    edgeState: cypher.SubscribeAcrossEdgeState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, edgeState.queryPartId)
    val edgesWatchedOff: Offset = {
      val edgeWatchedOffs: Array[Offset] = new Array(edgeState.edgesWatched.size)
      for (((he, (sqId, res)), i) <- edgeState.edgesWatched.zipWithIndex) {
        val halfEdgeOff = writeHalfEdge(builder, he)
        val sqIdOff = writeMultipleValuesStandingQueryPartId(builder, sqId)
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
      persistence.MultipleValuesSubscribeAcrossEdgeStandingQueryState.createEdgesWatchedVector(
        builder,
        edgeWatchedOffs
      )
    }
    persistence.MultipleValuesSubscribeAcrossEdgeStandingQueryState
      .createMultipleValuesSubscribeAcrossEdgeStandingQueryState(
        builder,
        sqIdOff,
        edgesWatchedOff
      )
  }

  private[this] def readMultipleValuesSubscribeAcrossEdgeStandingQueryState(
    edgeState: persistence.MultipleValuesSubscribeAcrossEdgeStandingQueryState
  ): cypher.SubscribeAcrossEdgeState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(edgeState.queryPartId)
    val edgesWatched: mutable.Map[
      HalfEdge,
      (MultipleValuesStandingQueryPartId, mutable.Map[ResultId, (ResultId, cypher.QueryContext)])
    ] = mutable.Map.empty

    var i: Int = 0
    val edgesWatchedLength = edgeState.edgesWatchedLength
    while (i < edgesWatchedLength) {
      val edgeWatched: persistence.EdgeWatched = edgeState.edgesWatched(i)
      val halfEdge: HalfEdge = readHalfEdge(edgeWatched.halfEdge)
      val queryPartId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(
        edgeWatched.queryPartId
      )
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

  private[this] def writeMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(
    builder: FlatBufferBuilder,
    edgeState: cypher.EdgeSubscriptionReciprocalState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, edgeState.queryPartId)
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
      persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState.createReverseResultDependencyVector(
        builder,
        reverseDepOffs
      )
    }
    val andThenOff: Offset = writeMultipleValuesStandingQueryPartId(builder, edgeState.andThenId)
    persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState
      .createMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(
        builder,
        sqIdOff,
        halfEdgeOff,
        edgeState.currentlyMatching,
        reverseDepsOff,
        andThenOff
      )
  }

  private[this] def readMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(
    edgeState: persistence.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState
  ): cypher.EdgeSubscriptionReciprocalState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(edgeState.queryPartId)
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
    val andThenId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(edgeState.andThenId)
    cypher.EdgeSubscriptionReciprocalState(sqId, halfEdge, edgeState.currentlyMatching, reverseDeps, andThenId)
  }

  private[this] def writeMultipleValuesFilterMapStandingQueryState(
    builder: FlatBufferBuilder,
    filterState: cypher.FilterMapState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, filterState.queryPartId)
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
      persistence.MultipleValuesFilterMapStandingQueryState.createReverseResultDependencyVector(
        builder,
        reverseDepOffs
      )
    }
    persistence.MultipleValuesFilterMapStandingQueryState.createMultipleValuesFilterMapStandingQueryState(
      builder,
      sqIdOff,
      reverseDepsOff
    )
  }

  private[this] def readMultipleValuesFilterMapStandingQueryState(
    filterState: persistence.MultipleValuesFilterMapStandingQueryState
  ): cypher.FilterMapState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(filterState.queryPartId)
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

  private[this] def writeMultipleValuesAllPropertiesStandingQueryState(
    builder: FlatBufferBuilder,
    localPropState: cypher.AllPropertiesState
  ): Offset = {
    val sqIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, localPropState.queryPartId)
    val resIdOff: Offset = localPropState.currentResult match {
      case None => NoOffset
      case Some(resId) => writeStandingQueryResultId(builder, resId)
    }
    persistence.MultipleValuesAllPropertiesStandingQueryState.createMultipleValuesAllPropertiesStandingQueryState(
      builder,
      sqIdOff,
      resIdOff
    )
  }

  private[this] def readMultipleValuesAllPropertiesStandingQueryState(
    localPropState: persistence.MultipleValuesAllPropertiesStandingQueryState
  ): cypher.AllPropertiesState = {
    val sqId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(localPropState.queryPartId)
    val resId: Option[ResultId] = Option(localPropState.resultId).map(readStandingQueryResultId)
    cypher.AllPropertiesState(sqId, resId)
  }

  private[this] def writeMultipleValuesStandingQuerySubscriber(
    builder: FlatBufferBuilder,
    subscriber: MultipleValuesStandingQuerySubscriber
  ): TypeAndOffset =
    subscriber match {
      case MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalId, queryId) =>
        val onNodeOff: Offset = writeQuineId(builder, onNode)
        val queryPartIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, queryId)
        val globalQueryIdOff: Offset = writeStandingQueryId(builder, globalId)
        val offset: Offset = persistence.CypherNodeSubscriber.createCypherNodeSubscriber(
          builder,
          onNodeOff,
          queryPartIdOff,
          globalQueryIdOff
        )
        TypeAndOffset(persistence.MultipleValuesStandingQuerySubscriber.CypherNodeSubscriber, offset)

      case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(globalId) =>
        val globalQueryIdOff: Offset = writeStandingQueryId(builder, globalId)
        val offset: Offset = persistence.CypherGlobalSubscriber.createCypherGlobalSubscriber(
          builder,
          globalQueryIdOff
        )
        TypeAndOffset(persistence.MultipleValuesStandingQuerySubscriber.CypherGlobalSubscriber, offset)
    }

  private[this] def readMultipleValuesStandingQuerySubscriber(
    typ: Byte,
    makeSubscriber: Table => Table
  ): MultipleValuesStandingQuerySubscriber =
    typ match {
      case persistence.MultipleValuesStandingQuerySubscriber.CypherNodeSubscriber =>
        val nodeSub =
          makeSubscriber(new persistence.CypherNodeSubscriber()).asInstanceOf[persistence.CypherNodeSubscriber]
        val onNode: QuineId = readQuineId(nodeSub.onNode)
        val queryPartId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(nodeSub.queryPartId)
        val globalQueryId: StandingQueryId = readStandingQueryId(nodeSub.globalQueryId)
        MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalQueryId, queryPartId)

      case persistence.MultipleValuesStandingQuerySubscriber.CypherGlobalSubscriber =>
        val globalSub =
          makeSubscriber(new persistence.CypherGlobalSubscriber()).asInstanceOf[persistence.CypherGlobalSubscriber]
        val globalQueryId: StandingQueryId = readStandingQueryId(globalSub.globalQueryId)
        MultipleValuesStandingQuerySubscriber.GlobalSubscriber(globalQueryId)

      case other =>
        throw new InvalidUnionType(other, persistence.MultipleValuesStandingQueryState.names)
    }

  private[this] def writeMultipleValuesStandingQuerySubscribers(
    builder: FlatBufferBuilder,
    subscribers: MultipleValuesStandingQuerySubscribers
  ): Offset = {
    val queryPartIdOff: Offset = writeMultipleValuesStandingQueryPartId(builder, subscribers.forQuery)
    val globalQueryIdOff: Offset = writeStandingQueryId(builder, subscribers.globalId)
    val subTyps: Array[Byte] = new Array(subscribers.subscribers.size)
    val subOffs: Array[Offset] = new Array(subscribers.subscribers.size)
    for ((sub, i) <- subscribers.subscribers.zipWithIndex) {
      val TypeAndOffset(subTyp, subOff) = writeMultipleValuesStandingQuerySubscriber(builder, sub)
      subTyps(i) = subTyp
      subOffs(i) = subOff
    }
    val subsTypOff: Offset =
      persistence.MultipleValuesStandingQuerySubscribers.createSubscribersTypeVector(builder, subTyps)
    val subsOff: Offset = persistence.MultipleValuesStandingQuerySubscribers.createSubscribersVector(builder, subOffs)
    persistence.MultipleValuesStandingQuerySubscribers.createMultipleValuesStandingQuerySubscribers(
      builder,
      queryPartIdOff,
      globalQueryIdOff,
      subsTypOff,
      subsOff
    )
  }

  private[this] def readMultipleValuesStandingQuerySubscribers(
    subscribers: persistence.MultipleValuesStandingQuerySubscribers
  ): MultipleValuesStandingQuerySubscribers = {
    val queryPartId: MultipleValuesStandingQueryPartId = readMultipleValuesStandingQueryPartId(subscribers.queryPartId)
    val globalQueryId: StandingQueryId = readStandingQueryId(subscribers.globalQueryId)
    val subs: mutable.Set[MultipleValuesStandingQuerySubscriber] = {
      val builder = mutable.Set.empty[MultipleValuesStandingQuerySubscriber]
      var i: Int = 0
      val subscribersLength = subscribers.subscribersLength
      while (i < subscribersLength) {
        builder += readMultipleValuesStandingQuerySubscriber(
          subscribers.subscribersType(i),
          subscribers.subscribers(_, i)
        )
        i += 1
      }
      builder
    }
    MultipleValuesStandingQuerySubscribers(queryPartId, globalQueryId, subs)
  }

  private[this] def writeMultipleValuesStandingQueryState(
    builder: FlatBufferBuilder,
    state: MultipleValuesStandingQueryState
  ): TypeAndOffset =
    state match {
      case unitState: cypher.UnitState =>
        val offset: Offset = writeMultipleValuesUnitStandingQueryState(builder, unitState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesUnitStandingQueryState, offset)

      case crossState: cypher.CrossState =>
        val offset: Offset = writeMultipleValuesCrossStandingQueryState(builder, crossState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesCrossStandingQueryState, offset)

      case propState: cypher.LocalPropertyState =>
        val offset: Offset = writeMultipleValuesLocalPropertyStandingQueryState(builder, propState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesLocalPropertyStandingQueryState,
          offset
        )

      case idState: cypher.LocalIdState =>
        val offset: Offset = writeMultipleValuesLocalIdStandingQueryState(builder, idState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesLocalIdStandingQueryState, offset)

      case edgeState: cypher.SubscribeAcrossEdgeState =>
        val offset: Offset = writeMultipleValuesSubscribeAcrossEdgeStandingQueryState(builder, edgeState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesSubscribeAcrossEdgeStandingQueryState,
          offset
        )

      case edgeState: cypher.EdgeSubscriptionReciprocalState =>
        val offset: Offset = writeMultipleValuesEdgeSubscriptionReciprocalStandingQueryState(builder, edgeState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesEdgeSubscriptionReciprocalStandingQueryState,
          offset
        )

      case filterState: cypher.FilterMapState =>
        val offset: Offset = writeMultipleValuesFilterMapStandingQueryState(builder, filterState)
        TypeAndOffset(persistence.MultipleValuesStandingQueryState.MultipleValuesFilterMapStandingQueryState, offset)

      case allPropertiesState: cypher.AllPropertiesState =>
        val offset: Offset = writeMultipleValuesAllPropertiesStandingQueryState(builder, allPropertiesState)
        TypeAndOffset(
          persistence.MultipleValuesStandingQueryState.MultipleValuesAllPropertiesStandingQueryState,
          offset
        )
    }

  private[this] def readMultipleValuesStandingQueryState(
    typ: Byte,
    makeState: Table => Table
  ): MultipleValuesStandingQueryState =
    typ match {
      case persistence.MultipleValuesStandingQueryState.MultipleValuesUnitStandingQueryState =>
        val unitState = makeState(new persistence.MultipleValuesUnitStandingQueryState())
          .asInstanceOf[persistence.MultipleValuesUnitStandingQueryState]
        readMultipleValuesUnitStandingQueryState(unitState)

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

      case other =>
        throw new InvalidUnionType(other, persistence.MultipleValuesStandingQueryState.names)
    }

  private[this] def writeMultipleValuesStandingQueryStateAndSubscribers(
    builder: FlatBufferBuilder,
    state: MultipleValuesStandingQueryState,
    subscribers: MultipleValuesStandingQuerySubscribers
  ): Offset = {
    val TypeAndOffset(stateTyp, stateOff) = writeMultipleValuesStandingQueryState(builder, state)
    val subscribersOff: Offset = writeMultipleValuesStandingQuerySubscribers(builder, subscribers)
    persistence.MultipleValuesStandingQueryStateAndSubscribers.createMultipleValuesStandingQueryStateAndSubscribers(
      builder,
      subscribersOff,
      stateTyp,
      stateOff
    )
  }

  private[this] def readMultipleValuesStandingQueryStateAndSubscribers(
    stateAndSubs: persistence.MultipleValuesStandingQueryStateAndSubscribers
  ): (MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState) = {
    val state = readMultipleValuesStandingQueryState(stateAndSubs.stateType, stateAndSubs.state)
    val subscribers = readMultipleValuesStandingQuerySubscribers(stateAndSubs.subscribers)
    subscribers -> state
  }
  val format: BinaryFormat[(MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)] =
    new PackedFlatBufferBinaryFormat[(MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)] {
      def writeToBuffer(
        builder: FlatBufferBuilder,
        state: (MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState)
      ): Offset =
        writeMultipleValuesStandingQueryStateAndSubscribers(builder, state._2, state._1)

      def readFromBuffer(
        buffer: ByteBuffer
      ): (MultipleValuesStandingQuerySubscribers, MultipleValuesStandingQueryState) =
        readMultipleValuesStandingQueryStateAndSubscribers(
          persistence.MultipleValuesStandingQueryStateAndSubscribers
            .getRootAsMultipleValuesStandingQueryStateAndSubscribers(buffer)
        )
    }

}

package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.behavior.StandingQuerySubscribers
import com.thatdot.quine.graph.cypher.StandingQueryState
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{CypherSubscriber, ResultId}
import com.thatdot.quine.graph.{StandingQueryId, StandingQueryPartId, cypher}
import com.thatdot.quine.model.{HalfEdge, QuineId}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset, TypeAndOffset}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object StandingQueryStateCodec extends PersistenceCodec[(StandingQuerySubscribers, StandingQueryState)] {

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
    val state = readCypherStandingQueryState(stateAndSubs.stateType, stateAndSubs.state)
    val subscribers = readCypherStandingQuerySubscribers(stateAndSubs.subscribers)
    subscribers -> state
  }
  val format: BinaryFormat[(StandingQuerySubscribers, StandingQueryState)] =
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

}

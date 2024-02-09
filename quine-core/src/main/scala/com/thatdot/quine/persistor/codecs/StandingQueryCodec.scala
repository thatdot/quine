package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer
import java.util.regex.Pattern

import cats.data.NonEmptyList
import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.{
  GraphQueryPattern,
  PatternOrigin,
  StandingQuery,
  StandingQueryId,
  StandingQueryPattern,
  cypher
}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistence
import com.thatdot.quine.persistence.ReturnColumnAllProperties
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset, TypeAndOffset, emptyTable}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object StandingQueryCodec extends PersistenceCodec[StandingQuery] {

  private[this] def writeReturnColumn(
    builder: FlatBufferBuilder,
    returnCol: GraphQueryPattern.ReturnColumn
  ): TypeAndOffset =
    returnCol match {
      case GraphQueryPattern.ReturnColumn.Id(node, formatAsStr, aliasedAs) =>
        val aliasedAsOff: Offset = builder.createString(aliasedAs.name)
        persistence.ReturnColumnId.startReturnColumnId(builder)
        val nodeOff: Offset = persistence.NodePatternId.createNodePatternId(builder, node.id)
        persistence.ReturnColumnId.addNode(builder, nodeOff)
        persistence.ReturnColumnId.addFormatAsString(builder, formatAsStr)
        persistence.ReturnColumnId.addAliasedAs(builder, aliasedAsOff)
        val off: Offset = persistence.ReturnColumnId.endReturnColumnId(builder)
        TypeAndOffset(persistence.ReturnColumn.ReturnColumnId, off)

      case GraphQueryPattern.ReturnColumn.Property(node, propertyKey, aliasedAs) =>
        val propertyKeyOff: Offset = builder.createString(propertyKey.name)
        val aliasedAsOff: Offset = builder.createString(aliasedAs.name)
        persistence.ReturnColumnProperty.startReturnColumnProperty(builder)
        val nodeOff: Offset = persistence.NodePatternId.createNodePatternId(builder, node.id)
        persistence.ReturnColumnProperty.addNode(builder, nodeOff)
        persistence.ReturnColumnProperty.addPropertyKey(builder, propertyKeyOff)
        persistence.ReturnColumnProperty.addAliasedAs(builder, aliasedAsOff)
        val off: Offset = persistence.ReturnColumnProperty.endReturnColumnProperty(builder)
        TypeAndOffset(persistence.ReturnColumn.ReturnColumnProperty, off)

      case GraphQueryPattern.ReturnColumn.AllProperties(node, aliasedAs) =>
        val aliasedAsOff: Offset = builder.createString(aliasedAs.name)
        persistence.ReturnColumnAllProperties.startReturnColumnAllProperties(builder)
        val nodeOff: Offset = persistence.NodePatternId.createNodePatternId(builder, node.id)
        persistence.ReturnColumnAllProperties.addNode(builder, nodeOff)
        persistence.ReturnColumnAllProperties.addAliasedAs(builder, aliasedAsOff)
        val off: Offset = persistence.ReturnColumnAllProperties.endReturnColumnAllProperties(builder)
        TypeAndOffset(persistence.ReturnColumn.ReturnColumnAllProperties, off)
    }

  private[this] def readReturnColumn(
    typ: Byte,
    makeReturnCol: Table => Table
  ): GraphQueryPattern.ReturnColumn =
    typ match {
      case persistence.ReturnColumn.ReturnColumnId =>
        val col = makeReturnCol(new persistence.ReturnColumnId()).asInstanceOf[persistence.ReturnColumnId]
        GraphQueryPattern.ReturnColumn.Id(
          GraphQueryPattern.NodePatternId(col.node.id),
          col.formatAsString,
          Symbol(col.aliasedAs)
        )

      case persistence.ReturnColumn.ReturnColumnProperty =>
        val col = makeReturnCol(new persistence.ReturnColumnProperty()).asInstanceOf[persistence.ReturnColumnProperty]
        GraphQueryPattern.ReturnColumn.Property(
          GraphQueryPattern.NodePatternId(col.node.id),
          Symbol(col.propertyKey),
          Symbol(col.aliasedAs)
        )

      case persistence.ReturnColumn.ReturnColumnAllProperties =>
        val col = makeReturnCol(new ReturnColumnAllProperties()).asInstanceOf[persistence.ReturnColumnAllProperties]
        GraphQueryPattern.ReturnColumn.AllProperties(
          GraphQueryPattern.NodePatternId(col.node.id),
          Symbol(col.aliasedAs)
        )

      case other =>
        throw new InvalidUnionType(other, persistence.ReturnColumn.names)
    }

  private[this] def writeNodePatternPropertyValuePattern(
    builder: FlatBufferBuilder,
    pattern: GraphQueryPattern.PropertyValuePattern
  ): TypeAndOffset =
    pattern match {
      case GraphQueryPattern.PropertyValuePattern.Value(value) =>
        val compareToOff = writeQuineValue(builder, value)
        val off = persistence.NodePatternPropertyValue.createNodePatternPropertyValue(
          builder,
          compareToOff
        )
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyValue, off)

      case GraphQueryPattern.PropertyValuePattern.AnyValueExcept(value) =>
        val compareToOff = writeQuineValue(builder, value)
        val off = persistence.NodePatternPropertyAnyValueExcept.createNodePatternPropertyAnyValueExcept(
          builder,
          compareToOff
        )
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValueExcept, off)

      case GraphQueryPattern.PropertyValuePattern.AnyValue =>
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValue, emptyTable(builder))

      case GraphQueryPattern.PropertyValuePattern.NoValue =>
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyNoValue, emptyTable(builder))

      case GraphQueryPattern.PropertyValuePattern.RegexMatch(pattern) =>
        val patternOff = builder.createString(pattern.pattern)
        val off = persistence.NodePatternPropertyRegexMatch.createNodePatternPropertyRegexMatch(
          builder,
          patternOff
        )
        TypeAndOffset(persistence.NodePatternPropertyValuePattern.NodePatternPropertyRegexMatch, off)
    }

  private[this] def readNodePatternPropertyValuePattern(
    typ: Byte,
    makeValueConstraint: Table => Table
  ): GraphQueryPattern.PropertyValuePattern =
    typ match {
      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyValue =>
        val cons = makeValueConstraint(new persistence.NodePatternPropertyValue())
          .asInstanceOf[persistence.NodePatternPropertyValue]
        val value = readQuineValue(cons.compareTo)
        GraphQueryPattern.PropertyValuePattern.Value(value)

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValueExcept =>
        val cons = makeValueConstraint(new persistence.NodePatternPropertyAnyValueExcept())
          .asInstanceOf[persistence.NodePatternPropertyAnyValueExcept]
        val value = readQuineValue(cons.compareTo)
        GraphQueryPattern.PropertyValuePattern.AnyValueExcept(value)

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyAnyValue =>
        GraphQueryPattern.PropertyValuePattern.AnyValue

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyNoValue =>
        GraphQueryPattern.PropertyValuePattern.NoValue

      case persistence.NodePatternPropertyValuePattern.NodePatternPropertyRegexMatch =>
        val cons = makeValueConstraint(new persistence.NodePatternPropertyRegexMatch())
          .asInstanceOf[persistence.NodePatternPropertyRegexMatch]
        val pattern = cons.pattern
        GraphQueryPattern.PropertyValuePattern.RegexMatch(Pattern.compile(pattern))

      case other =>
        throw new InvalidUnionType(other, persistence.NodePatternPropertyValuePattern.names)
    }

  private[this] def writeNodePattern(
    builder: FlatBufferBuilder,
    nodePattern: GraphQueryPattern.NodePattern
  ): Offset = {
    val labelsOff: Offset = {
      val labelOffs: Array[Offset] = new Array[Offset](nodePattern.labels.size)
      for ((label, i) <- nodePattern.labels.zipWithIndex)
        labelOffs(i) = builder.createString(label.name)
      persistence.NodePattern.createLabelsVector(builder, labelOffs)
    }
    val quineIdOff: Offset = nodePattern.qidOpt match {
      case None => NoOffset
      case Some(qid) => writeQuineId(builder, qid)
    }
    val propertiesOff: Offset = {
      val propertyOffs: Array[Offset] = new Array[Offset](nodePattern.properties.size)
      for (((propKey, propPat), i) <- nodePattern.properties.zipWithIndex) {
        val keyOff: Offset = builder.createString(propKey.name)
        val TypeAndOffset(patTyp, patOff) = writeNodePatternPropertyValuePattern(builder, propPat)
        propertyOffs(i) = persistence.NodePatternProperty.createNodePatternProperty(builder, keyOff, patTyp, patOff)
      }
      persistence.NodePattern.createPropertiesVector(builder, propertyOffs)
    }
    persistence.NodePattern.startNodePattern(builder)
    val patternIdOff: Offset = persistence.NodePatternId.createNodePatternId(
      builder,
      nodePattern.id.id
    )
    persistence.NodePattern.addPatternId(builder, patternIdOff)
    persistence.NodePattern.addLabels(builder, labelsOff)
    persistence.NodePattern.addQuineId(builder, quineIdOff)
    persistence.NodePattern.addProperties(builder, propertiesOff)
    persistence.NodePattern.endNodePattern(builder)
  }

  private[this] def readNodePattern(
    nodePattern: persistence.NodePattern
  ): GraphQueryPattern.NodePattern = {
    val labels: Set[Symbol] = {
      val builder = Set.newBuilder[Symbol]
      var i = 0
      val labelsLength = nodePattern.labelsLength
      while (i < labelsLength) {
        builder += Symbol(nodePattern.labels(i))
        i += 1
      }
      builder.result()
    }
    val quineIdOpt: Option[QuineId] = Option(nodePattern.quineId).map(readQuineId)
    val properties: Map[Symbol, GraphQueryPattern.PropertyValuePattern] = {
      val builder = Map.newBuilder[Symbol, GraphQueryPattern.PropertyValuePattern]
      var i = 0
      val propertiesLength = nodePattern.propertiesLength
      while (i < propertiesLength) {
        val property: persistence.NodePatternProperty = nodePattern.properties(i)
        val pattern = readNodePatternPropertyValuePattern(property.patternType, property.pattern)
        builder += Symbol(property.key) -> pattern
        i += 1
      }
      builder.result()
    }
    GraphQueryPattern.NodePattern(
      GraphQueryPattern.NodePatternId(nodePattern.patternId.id),
      labels,
      quineIdOpt,
      properties
    )
  }

  private[this] def writeEdgePattern(
    builder: FlatBufferBuilder,
    edgePattern: GraphQueryPattern.EdgePattern
  ): Offset = {
    val labelOff = builder.createString(edgePattern.label.name)
    persistence.EdgePattern.startEdgePattern(builder)
    val fromIdOff: Offset = persistence.NodePatternId.createNodePatternId(
      builder,
      edgePattern.from.id
    )
    persistence.EdgePattern.addFrom(builder, fromIdOff)
    val toIdOff: Offset = persistence.NodePatternId.createNodePatternId(
      builder,
      edgePattern.to.id
    )
    persistence.EdgePattern.addTo(builder, toIdOff)
    persistence.EdgePattern.addIsDirected(builder, edgePattern.isDirected)
    persistence.EdgePattern.addLabel(builder, labelOff)
    persistence.EdgePattern.endEdgePattern(builder)
  }

  private[this] def readEdgePattern(
    edgePattern: persistence.EdgePattern
  ): GraphQueryPattern.EdgePattern =
    GraphQueryPattern.EdgePattern(
      GraphQueryPattern.NodePatternId(edgePattern.from.id),
      GraphQueryPattern.NodePatternId(edgePattern.to.id),
      edgePattern.isDirected,
      Symbol(edgePattern.label)
    )

  private[this] def writeGraphQueryPattern(
    builder: FlatBufferBuilder,
    pattern: GraphQueryPattern
  ): Offset = {
    val nodesOff: Offset = {
      val nodeOffs: Array[Offset] = new Array(pattern.nodes.length)
      for ((node, i) <- pattern.nodes.zipWithIndex.toList)
        nodeOffs(i) = writeNodePattern(builder, node)
      persistence.GraphQueryPattern.createNodesVector(builder, nodeOffs)
    }
    val edgesOff: Offset = {
      val edgeOffs: Array[Offset] = new Array(pattern.edges.length)
      for ((node, i) <- pattern.edges.zipWithIndex)
        edgeOffs(i) = writeEdgePattern(builder, node)
      persistence.GraphQueryPattern.createEdgesVector(builder, edgeOffs)
    }
    val (toExtractTypsOff, toExtractsOff) = {
      val toExtractTypOffs: Array[Byte] = new Array(pattern.toExtract.length)
      val toExtractOffs: Array[Offset] = new Array(pattern.toExtract.length)
      for ((col, i) <- pattern.toExtract.zipWithIndex) {
        val TypeAndOffset(colTyp, colOff) = writeReturnColumn(builder, col)
        toExtractTypOffs(i) = colTyp
        toExtractOffs(i) = colOff
      }
      val typs = persistence.GraphQueryPattern.createToExtractTypeVector(builder, toExtractTypOffs)
      val offs = persistence.GraphQueryPattern.createToExtractVector(builder, toExtractOffs)
      typs -> offs
    }
    val TypeAndOffset(filterCondTyp, filterCondOff) = pattern.filterCond match {
      case None => TypeAndOffset(persistence.CypherExpr.NONE, NoOffset)
      case Some(exp) => writeCypherExpr(builder, exp)
    }
    val toReturnsOff: Offset = {
      val toReturnOffs: Array[Offset] = new Array(pattern.toReturn.length)
      for (((returnAs, toReturn), i) <- pattern.toReturn.zipWithIndex) {
        val keyOff: Offset = builder.createString(returnAs.name)
        val TypeAndOffset(valueTyp, valueOff) = writeCypherExpr(builder, toReturn)
        toReturnOffs(i) = persistence.CypherMapExprEntry.createCypherMapExprEntry(
          builder,
          keyOff,
          valueTyp,
          valueOff
        )
      }
      persistence.GraphQueryPattern.createToReturnVector(builder, toReturnOffs)
    }
    persistence.GraphQueryPattern.startGraphQueryPattern(builder)
    persistence.GraphQueryPattern.addNodes(builder, nodesOff)
    persistence.GraphQueryPattern.addEdges(builder, edgesOff)
    val startingPointOffset: Offset = persistence.NodePatternId.createNodePatternId(builder, pattern.startingPoint.id)
    persistence.GraphQueryPattern.addStartingPoint(builder, startingPointOffset)
    persistence.GraphQueryPattern.addToExtractType(builder, toExtractTypsOff)
    persistence.GraphQueryPattern.addToExtract(builder, toExtractsOff)
    persistence.GraphQueryPattern.addFilterCondType(builder, filterCondTyp)
    persistence.GraphQueryPattern.addFilterCond(builder, filterCondOff)
    persistence.GraphQueryPattern.addToReturn(builder, toReturnsOff)
    persistence.GraphQueryPattern.addDistinct(builder, pattern.distinct)

    persistence.GraphQueryPattern.endGraphQueryPattern(builder)
  }

  private[this] def readGraphQueryPattern(
    pattern: persistence.GraphQueryPattern
  ): GraphQueryPattern = {
    val nodes: NonEmptyList[GraphQueryPattern.NodePattern] =
      // Throwing an exception here if nodes is empty - which would indicate a serialization error
      NonEmptyList.fromListUnsafe(List.tabulate(pattern.nodesLength) { i =>
        readNodePattern(pattern.nodes(i))
      })
    val edges: Seq[GraphQueryPattern.EdgePattern] = Seq.tabulate(pattern.edgesLength) { i =>
      readEdgePattern(pattern.edges(i))
    }
    val startingPoint: GraphQueryPattern.NodePatternId = GraphQueryPattern.NodePatternId(pattern.startingPoint.id)
    val toExtract: Seq[GraphQueryPattern.ReturnColumn] = Seq.tabulate(pattern.toExtractLength) { i =>
      readReturnColumn(pattern.toExtractType(i), pattern.toExtract(_, i))
    }
    val filterCond: Option[cypher.Expr] = pattern.filterCondType match {
      case persistence.CypherExpr.NONE => None
      case typ => Some(readCypherExpr(typ, pattern.filterCond))
    }
    val toReturn: Seq[(Symbol, cypher.Expr)] = Seq.tabulate(pattern.toReturnLength) { i =>
      val prop: persistence.CypherMapExprEntry = pattern.toReturn(i)
      val returnExp = readCypherExpr(prop.valueType, prop.value)
      Symbol(prop.key) -> returnExp
    }

    val distinct: Boolean = pattern.distinct
    GraphQueryPattern(nodes, edges, startingPoint, toExtract, filterCond, toReturn, distinct)
  }

  private[this] def writeGraphPatternOrigin(
    builder: FlatBufferBuilder,
    graphPat: PatternOrigin.GraphPattern
  ): Offset = {
    val graphOff: Offset = writeGraphQueryPattern(builder, graphPat.pattern)
    val cypherOrigOff: Offset = graphPat.cypherOriginal match {
      case None => NoOffset
      case Some(original) => builder.createString(original)
    }
    persistence.GraphPatternOrigin.createGraphPatternOrigin(
      builder,
      graphOff,
      cypherOrigOff
    )
  }

  private[this] def readGraphPatternOrigin(
    graphPatOrigin: persistence.GraphPatternOrigin
  ): PatternOrigin.GraphPattern = {
    val graphPat = readGraphQueryPattern(graphPatOrigin.pattern)
    val originalCypher: Option[String] = Option(graphPatOrigin.cypherOriginal)
    PatternOrigin.GraphPattern(graphPat, originalCypher)
  }

  private[this] def writeBranchOrigin(
    builder: FlatBufferBuilder,
    origin: PatternOrigin.DgbOrigin
  ): TypeAndOffset =
    origin match {
      case PatternOrigin.DirectDgb =>
        TypeAndOffset(persistence.BranchOrigin.DirectDgb, NoOffset)

      case graphPat: PatternOrigin.GraphPattern =>
        val originOff: Offset = writeGraphPatternOrigin(builder, graphPat)
        TypeAndOffset(persistence.BranchOrigin.GraphPatternOrigin, originOff)
    }

  private[this] def readBranchOrigin(
    branchOriginTyp: Byte,
    makeBranchOrigin: Table => Table
  ): PatternOrigin.DgbOrigin =
    branchOriginTyp match {
      case persistence.BranchOrigin.DirectDgb =>
        PatternOrigin.DirectDgb

      case persistence.BranchOrigin.GraphPatternOrigin =>
        val graphPatOrigin = makeBranchOrigin(new persistence.GraphPatternOrigin())
          .asInstanceOf[persistence.GraphPatternOrigin]
        val readOrigin = readGraphPatternOrigin(graphPatOrigin)

        // DistinctId queries must include a `DISTINCT` keyword
        if (!readOrigin.pattern.distinct) {
          throw new InvalidPersistedQuineData(
            s"Detected an invalid DistinctId query pattern during deserialization. DistinctId queries must return a single `DISTINCT` value. Detected pattern was: `${readOrigin.cypherOriginal
              .getOrElse(readOrigin)}`"
          )
        }
        readOrigin

      case other =>
        throw new InvalidUnionType(other, persistence.BranchOrigin.names)
    }

  private[this] def writeSqV4Origin(
    builder: FlatBufferBuilder,
    origin: PatternOrigin.SqV4Origin
  ): TypeAndOffset =
    origin match {
      case PatternOrigin.DirectSqV4 =>
        TypeAndOffset(persistence.SqV4Origin.DirectSqV4, NoOffset)

      case graphPat: PatternOrigin.GraphPattern =>
        val originOff: Offset = writeGraphPatternOrigin(builder, graphPat)
        TypeAndOffset(persistence.SqV4Origin.GraphPatternOrigin, originOff)
    }

  private[this] def readSqV4Origin(
    branchOriginTyp: Byte,
    makeBranchOrigin: Table => Table
  ): PatternOrigin.SqV4Origin =
    branchOriginTyp match {
      case persistence.SqV4Origin.DirectSqV4 =>
        PatternOrigin.DirectSqV4

      case persistence.SqV4Origin.GraphPatternOrigin =>
        val graphPatOrigin = makeBranchOrigin(new persistence.GraphPatternOrigin())
          .asInstanceOf[persistence.GraphPatternOrigin]
        val readOrigin = readGraphPatternOrigin(graphPatOrigin)
        // MultipleValues queries do not yet support distinct (QU-568) so warn if that is requested
        if (readOrigin.pattern.distinct) {
          logger.warn(
            readOrigin.cypherOriginal match {
              case Some(cypherQuery) =>
                s"Read a GraphPattern for a MultipleValues query with a DISTINCT clause. This is not yet supported. Query was: '$cypherQuery'"
              case None =>
                s"Read a GraphPattern for a MultipleValues query that specifies `distinct`. This is not yet supported. Query pattern was: $readOrigin"
            }
          )
        }
        readOrigin

      case other =>
        throw new InvalidUnionType(other, persistence.SqV4Origin.names)
    }

  private[this] def writeSqV4StandingQuery(
    builder: FlatBufferBuilder,
    cypherQuery: StandingQueryPattern.MultipleValuesQueryPattern
  ): Offset = {
    val TypeAndOffset(queryTyp, queryOff) = writeMultipleValuesStandingQuery(builder, cypherQuery.compiledQuery)
    val TypeAndOffset(originTyp, originOff) = writeSqV4Origin(builder, cypherQuery.origin)
    persistence.SqV4Query.createSqV4Query(
      builder,
      queryTyp,
      queryOff,
      cypherQuery.includeCancellation,
      originTyp,
      originOff
    )
  }

  private[this] def readSqV4StandingQuery(
    cypherQuery: persistence.SqV4Query
  ): StandingQueryPattern.MultipleValuesQueryPattern = {
    val query: MultipleValuesStandingQuery = readMultipleValuesStandingQuery(cypherQuery.queryType, cypherQuery.query)
    val origin: PatternOrigin.SqV4Origin = readSqV4Origin(cypherQuery.originType, cypherQuery.origin)
    StandingQueryPattern.MultipleValuesQueryPattern(query, cypherQuery.includeCancellation, origin)
  }

  private[this] def writeDomainGraphNodeStandingQueryPattern(
    builder: FlatBufferBuilder,
    dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern
  ): Offset = {
    val aliasReturnAsOff: Offset = builder.createString(dgnPattern.aliasReturnAs.name)
    val TypeAndOffset(originTyp, originOff) = writeBranchOrigin(builder, dgnPattern.origin)
    persistence.BranchQuery.createBranchQuery(
      builder,
      dgnPattern.dgnId,
      dgnPattern.formatReturnAsStr,
      aliasReturnAsOff,
      dgnPattern.includeCancellation,
      originTyp,
      originOff
    )
  }

  private[this] def readDomainGraphNodeStandingQueryPattern(
    branchQuery: persistence.BranchQuery
  ): StandingQueryPattern.DomainGraphNodeStandingQueryPattern = {
    val origin = readBranchOrigin(branchQuery.originType, branchQuery.origin)
    StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
      branchQuery.dgnId,
      branchQuery.formatReturnAsString,
      Symbol(branchQuery.aliasReturnAs),
      branchQuery.includeCancellation,
      origin
    )
  }

  private[this] def writeStandingQueryPattern(
    builder: FlatBufferBuilder,
    sqPat: StandingQueryPattern
  ): TypeAndOffset =
    sqPat match {
      case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
        val offset: Offset = writeDomainGraphNodeStandingQueryPattern(builder, dgnPattern)
        TypeAndOffset(persistence.StandingQueryPattern.BranchQuery, offset)

      case cypher: StandingQueryPattern.MultipleValuesQueryPattern =>
        val offset: Offset = writeSqV4StandingQuery(builder, cypher)
        TypeAndOffset(persistence.StandingQueryPattern.SqV4Query, offset)
    }

  private[this] def readStandingQueryPattern(
    typ: Byte,
    makeSQP: Table => Table
  ): StandingQueryPattern =
    typ match {
      case persistence.StandingQueryPattern.BranchQuery =>
        val branch = makeSQP(new persistence.BranchQuery()).asInstanceOf[persistence.BranchQuery]
        readDomainGraphNodeStandingQueryPattern(branch)

      case persistence.StandingQueryPattern.SqV4Query =>
        val sqv4 = makeSQP(new persistence.SqV4Query()).asInstanceOf[persistence.SqV4Query]
        readSqV4StandingQuery(sqv4)

      case other =>
        throw new InvalidUnionType(other, persistence.StandingQueryPattern.names)
    }

  private[this] def writeStandingQuery(
    builder: FlatBufferBuilder,
    standingQuery: StandingQuery
  ): Offset = {
    val nameOff: Offset = builder.createString(standingQuery.name)
    val idOff: Offset = writeStandingQueryId(builder, standingQuery.id)
    val TypeAndOffset(queryTyp, queryOff) = writeStandingQueryPattern(builder, standingQuery.query)
    persistence.StandingQuery.createStandingQuery(
      builder,
      nameOff,
      idOff,
      queryTyp,
      queryOff,
      standingQuery.queueBackpressureThreshold,
      standingQuery.queueMaxSize
    )
  }

  private[this] def readStandingQuery(
    standingQuery: persistence.StandingQuery
  ): StandingQuery = {
    val id: StandingQueryId = readStandingQueryId(standingQuery.id)
    val query: StandingQueryPattern = readStandingQueryPattern(standingQuery.queryType, standingQuery.query)
    StandingQuery(
      standingQuery.name,
      id,
      query,
      standingQuery.queueBackpressureThreshold,
      standingQuery.queueMaxSize,
      // do not support hash code on restored standing query,
      // because the hash code is calculated per-host and is not stored
      shouldCalculateResultHashCode = false
    )
  }

  val format: BinaryFormat[StandingQuery] = new PackedFlatBufferBinaryFormat[StandingQuery] {
    def writeToBuffer(builder: FlatBufferBuilder, sq: StandingQuery): Offset =
      writeStandingQuery(builder, sq)

    def readFromBuffer(buffer: ByteBuffer): StandingQuery =
      readStandingQuery(persistence.StandingQuery.getRootAsStandingQuery(buffer))
  }
}

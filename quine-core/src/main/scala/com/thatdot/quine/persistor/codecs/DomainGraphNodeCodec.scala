package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.{FlatBufferBuilder, Table}

import com.thatdot.quine.graph.ByteBufferOps
import com.thatdot.quine.model.DomainGraphNode.{DomainGraphNodeEdge, DomainGraphNodeId}
import com.thatdot.quine.model.{
  CircularEdge,
  DependencyDirection,
  DependsUpon,
  DomainGraphNode,
  DomainNodeEquiv,
  EdgeMatchConstraints,
  FetchConstraint,
  GenericEdge,
  Incidental,
  IsDependedUpon,
  MandatoryConstraint,
  MuVariableName,
  NodeLocalComparisonFunc,
  NodeLocalComparisonFunctions,
  PropertyComparisonFunc,
  PropertyComparisonFunctions,
  PropertyValue,
  QuineValue
}
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.{NoOffset, Offset, TypeAndOffset, emptyTable}
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object DomainGraphNodeCodec extends PersistenceCodec[DomainGraphNode] {

  private[this] def writeGenericEdge(builder: FlatBufferBuilder, edge: GenericEdge): Offset =
    persistence.GenericEdge.createGenericEdge(
      builder,
      builder.createString(edge.edgeType.name),
      edgeDirection2Byte(edge.direction)
    )

  private[this] def readGenericEdge(edge: persistence.GenericEdge): GenericEdge =
    GenericEdge(
      Symbol(edge.edgeType),
      byte2EdgeDirection(edge.direction)
    )

  private[this] def writePropertyComparisonFunction(
    builder: FlatBufferBuilder,
    func: PropertyComparisonFunc
  ): TypeAndOffset =
    func match {
      case PropertyComparisonFunctions.Identicality =>
        TypeAndOffset(
          persistence.PropertyComparisonFunction.PropertyComparisonFunctionIdenticality,
          emptyTable(builder)
        )
      case PropertyComparisonFunctions.Wildcard =>
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionWildcard, emptyTable(builder))
      case PropertyComparisonFunctions.NoValue =>
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionNone, emptyTable(builder))
      case PropertyComparisonFunctions.NonIdenticality =>
        TypeAndOffset(
          persistence.PropertyComparisonFunction.PropertyComparisonFunctionNonIdenticality,
          emptyTable(builder)
        )
      case PropertyComparisonFunctions.RegexMatch(pattern) =>
        val patternOff: Offset = builder.createString(pattern)
        val offset: Offset =
          persistence.PropertyComparisonFunctionRegexMatch.createPropertyComparisonFunctionRegexMatch(
            builder,
            patternOff
          )
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionRegexMatch, offset)
      case PropertyComparisonFunctions.ListContains(values) =>
        val valuesOffs: Array[Offset] = new Array[Offset](values.size)
        for ((value, i) <- values.zipWithIndex)
          valuesOffs(i) = writeQuineValue(builder, value)
        val off = persistence.PropertyComparisonFunctionListContains.createPropertyComparisonFunctionListContains(
          builder,
          persistence.PropertyComparisonFunctionListContains.createValuesVector(builder, valuesOffs)
        )
        TypeAndOffset(persistence.PropertyComparisonFunction.PropertyComparisonFunctionListContains, off)
    }

  private[this] def readPropertyComparisonFunction(
    typ: Byte,
    makeFunc: Table => Table
  ): PropertyComparisonFunc =
    typ match {
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionIdenticality =>
        PropertyComparisonFunctions.Identicality
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionWildcard =>
        PropertyComparisonFunctions.Wildcard
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionNone =>
        PropertyComparisonFunctions.NoValue
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionNonIdenticality =>
        PropertyComparisonFunctions.NonIdenticality
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionRegexMatch =>
        val regexMatch = makeFunc(new persistence.PropertyComparisonFunctionRegexMatch())
          .asInstanceOf[persistence.PropertyComparisonFunctionRegexMatch]
        PropertyComparisonFunctions.RegexMatch(regexMatch.pattern)
      case persistence.PropertyComparisonFunction.PropertyComparisonFunctionListContains =>
        val cons = makeFunc(new persistence.PropertyComparisonFunctionListContains())
          .asInstanceOf[persistence.PropertyComparisonFunctionListContains]
        val values: Set[QuineValue] = {
          val builder = Set.newBuilder[QuineValue]
          var i = 0
          val valuesLength = cons.valuesLength
          while (i < valuesLength) {
            builder += readQuineValue(cons.values(i))
            i += 1
          }
          builder.result()
        }
        PropertyComparisonFunctions.ListContains(values)

      case other =>
        throw new InvalidUnionType(other, persistence.PropertyComparisonFunction.names)
    }

  private[this] def writeDomainNodeEquiv(builder: FlatBufferBuilder, dne: DomainNodeEquiv): Offset = {
    val classNameOff: Offset = dne.className match {
      case None => NoOffset
      case Some(name) => builder.createString(name)
    }

    val localPropsOff: Offset = {
      val localPropertiesOffs: Array[Offset] = new Array[Offset](dne.localProps.size)
      for (((propKey, (compFunction, propValueOpt)), i) <- dne.localProps.zipWithIndex) {
        val propKeyOff: Offset = builder.createString(propKey.name)
        val TypeAndOffset(compFuncTyp, compFuncOff) = writePropertyComparisonFunction(builder, compFunction)
        val propValueOff: Offset = persistence.LocalProperty.createValueVector(
          builder,
          propValueOpt match {
            case None => Array.emptyByteArray
            case Some(propVal) => propVal.serialized
          }
        )
        val localProp =
          persistence.LocalProperty.createLocalProperty(builder, propKeyOff, compFuncTyp, compFuncOff, propValueOff)
        localPropertiesOffs(i) = localProp
      }
      persistence.DomainNodeEquiv.createLocalPropertiesVector(builder, localPropertiesOffs)
    }

    val circularEdgesOff: Offset = {
      val circularEdgesOffs: Array[Offset] = new Array[Offset](dne.circularEdges.size)
      for (((edgeType, isDirected), i) <- dne.circularEdges.zipWithIndex)
        circularEdgesOffs(i) = persistence.CircularEdge.createCircularEdge(
          builder,
          builder.createString(edgeType.name),
          isDirected
        )
      persistence.DomainNodeEquiv.createCircularEdgesVector(builder, circularEdgesOffs)
    }

    persistence.DomainNodeEquiv.createDomainNodeEquiv(
      builder,
      classNameOff,
      localPropsOff,
      circularEdgesOff
    )
  }

  private[this] def readDomainNodeEquiv(dne: persistence.DomainNodeEquiv): DomainNodeEquiv = {
    val className: Option[String] = Option(dne.className)

    val localProps: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])] = {
      val builder = Map.newBuilder[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]
      var i: Int = 0
      val localPropertiesLength: Int = dne.localPropertiesLength
      while (i < localPropertiesLength) {
        val localProperty: persistence.LocalProperty = dne.localProperties(i)
        val propertyKey: Symbol = Symbol(localProperty.propertyKey)
        val compFunc: PropertyComparisonFunc = readPropertyComparisonFunction(
          localProperty.comparisonFunctionType,
          localProperty.comparisonFunction(_)
        )
        val propertyValueBytes: Option[PropertyValue] = {
          val bytes = localProperty.valueAsByteBuffer.remainingBytes
          if (bytes.length == 0) None
          else Some(PropertyValue.fromBytes(bytes))
        }
        builder += propertyKey -> (compFunc -> propertyValueBytes)
        i += 1
      }
      builder.result()
    }

    val circularEdges: Set[CircularEdge] = {
      val builder = Set.newBuilder[CircularEdge]
      var i: Int = 0
      val circularEdgesLength = dne.circularEdgesLength
      while (i < circularEdgesLength) {
        val circularEdge: persistence.CircularEdge = dne.circularEdges(i)
        builder += Symbol(circularEdge.edgeType) -> circularEdge.isDirected
        i += 1
      }
      builder.result()
    }

    DomainNodeEquiv(className, localProps, circularEdges)
  }

  private[this] def writeDomainEdge(
    builder: FlatBufferBuilder,
    de: DomainGraphNodeEdge
  ): Offset = {

    val depDirection: Byte = de.depDirection match {
      case DependsUpon => persistence.DependencyDirection.DependsUpon
      case IsDependedUpon => persistence.DependencyDirection.IsDependedUpon
      case Incidental => persistence.DependencyDirection.Incidental
    }

    val constraints: TypeAndOffset = de.constraints match {
      case MandatoryConstraint =>
        TypeAndOffset(persistence.EdgeMatchConstraints.MandatoryConstraint, emptyTable(builder))

      case FetchConstraint(min, max) =>
        TypeAndOffset(
          persistence.EdgeMatchConstraints.FetchConstraint,
          persistence.FetchConstraint.createFetchConstraint(builder, min, max.isDefined, max.getOrElse(0))
        )
    }

    persistence.DomainEdge.createDomainEdge(
      builder,
      writeGenericEdge(builder, de.edge),
      depDirection,
      de.dgnId,
      de.circularMatchAllowed,
      constraints.typ,
      constraints.offset
    )
  }

  private[this] def readDomainEdge(de: persistence.DomainEdge): DomainGraphNodeEdge = {

    val depDirection: DependencyDirection = de.dependency match {
      case persistence.DependencyDirection.DependsUpon => DependsUpon
      case persistence.DependencyDirection.IsDependedUpon => IsDependedUpon
      case persistence.DependencyDirection.Incidental => Incidental
      case other => throw new InvalidUnionType(other, persistence.DependencyDirection.names)
    }

    val constraints: EdgeMatchConstraints = de.constraintsType match {
      case persistence.EdgeMatchConstraints.MandatoryConstraint =>
        MandatoryConstraint
      case persistence.EdgeMatchConstraints.FetchConstraint =>
        val fetch = de.constraints(new persistence.FetchConstraint()).asInstanceOf[persistence.FetchConstraint]
        FetchConstraint(fetch.min, if (fetch.hasMax) Some(fetch.max) else None)
      case other =>
        throw new InvalidUnionType(other, persistence.EdgeMatchConstraints.names)
    }

    DomainGraphNodeEdge(
      readGenericEdge(de.edge),
      depDirection,
      de.dgnId,
      de.circularMatchAllowed,
      constraints
    )
  }

  private[this] def writeDomainGraphNode(
    builder: FlatBufferBuilder,
    dgn: DomainGraphNode
  ): TypeAndOffset =
    dgn match {
      case DomainGraphNode.Single(dne, identification, nextNodes, compFunc) =>
        val identificationOff: Offset = identification match {
          case None => NoOffset
          case Some(id) =>
            persistence.Identification.createIdentification(
              builder,
              writeQuineId(builder, id)
            )
        }

        val nextNodesOff: Offset = {
          val nextNodesOffs: Array[Offset] = new Array[Offset](nextNodes.size)
          var i = 0
          for (nextNode <- nextNodes) {
            nextNodesOffs(i) = writeDomainEdge(builder, nextNode)
            i += 1
          }
          persistence.SingleNode.createNextNodesVector(builder, nextNodesOffs)
        }

        val comparisonFunction: Byte = compFunc match {
          case NodeLocalComparisonFunctions.Identicality =>
            persistence.NodeLocalComparisonFunction.Identicality
          case NodeLocalComparisonFunctions.EqualSubset =>
            persistence.NodeLocalComparisonFunction.EqualSubset
          case NodeLocalComparisonFunctions.Wildcard =>
            persistence.NodeLocalComparisonFunction.Wildcard
        }

        val offset: Offset = persistence.SingleNode.createSingleNode(
          builder,
          writeDomainNodeEquiv(builder, dne),
          identificationOff,
          nextNodesOff,
          comparisonFunction
        )
        TypeAndOffset(persistence.DomainGraphNode.SingleNode, offset)

      case DomainGraphNode.Or(disjuncts) =>
        val offset: Offset = persistence.OrNode.createOrNode(
          builder,
          persistence.OrNode.createDisjunctsDgnIdsVector(builder, disjuncts.toArray)
        )
        TypeAndOffset(persistence.DomainGraphNode.OrNode, offset)

      case DomainGraphNode.And(conjuncts) =>
        val offset = persistence.AndNode.createAndNode(
          builder,
          persistence.AndNode.createConjunctsDgnIdsVector(builder, conjuncts.toArray)
        )
        TypeAndOffset(persistence.DomainGraphNode.AndNode, offset)

      case DomainGraphNode.Not(negated) =>
        TypeAndOffset(
          persistence.DomainGraphNode.NotNode,
          persistence.NotNode.createNotNode(builder, negated)
        )

      case DomainGraphNode.Mu(muVar, dgnId) =>
        val offset: Offset = persistence.MuNode.createMuNode(
          builder,
          builder.createString(muVar.str),
          dgnId
        )
        TypeAndOffset(persistence.DomainGraphNode.MuNode, offset)

      case DomainGraphNode.MuVar(muVar) =>
        TypeAndOffset(
          persistence.DomainGraphNode.MuVarNode,
          persistence.MuVarNode.createMuVarNode(builder, builder.createString(muVar.str))
        )
    }

  private[this] def readDomainGraphNode(
    typ: Byte,
    makeNode: Table => Table
  ): DomainGraphNode =
    typ match {
      case persistence.DomainGraphNode.SingleNode =>
        val single = makeNode(new persistence.SingleNode()).asInstanceOf[persistence.SingleNode]
        val domainNodeEquiv: DomainNodeEquiv = readDomainNodeEquiv(single.domainNodeEquiv)
        val identification = Option(single.identification).map(ident => readQuineId(ident.id))
        val nextNodes = {
          val builder = Seq.newBuilder[DomainGraphNodeEdge]
          var i: Int = 0
          val nextNodesLength = single.nextNodesLength
          while (i < nextNodesLength) {
            builder += readDomainEdge(single.nextNodes(i))
            i += 1
          }
          builder.result()
        }
        val comparisonFunc: NodeLocalComparisonFunc = single.comparisonFunction match {
          case persistence.NodeLocalComparisonFunction.Identicality =>
            NodeLocalComparisonFunctions.Identicality
          case persistence.NodeLocalComparisonFunction.EqualSubset =>
            NodeLocalComparisonFunctions.EqualSubset
          case persistence.NodeLocalComparisonFunction.Wildcard =>
            NodeLocalComparisonFunctions.Wildcard
          case other =>
            throw new InvalidUnionType(other, persistence.NodeLocalComparisonFunction.names)
        }

        DomainGraphNode.Single(domainNodeEquiv, identification, nextNodes, comparisonFunc)

      case persistence.DomainGraphNode.OrNode =>
        val or = makeNode(new persistence.OrNode()).asInstanceOf[persistence.OrNode]
        val disjuncts = Seq.newBuilder[DomainGraphNodeId]
        var i: Int = 0
        while (i < or.disjunctsDgnIdsLength()) {
          disjuncts += or.disjunctsDgnIds(i)
          i += 1
        }
        DomainGraphNode.Or(disjuncts.result())

      case persistence.DomainGraphNode.AndNode =>
        val and = makeNode(new persistence.AndNode()).asInstanceOf[persistence.AndNode]
        val conjuncts = Seq.newBuilder[DomainGraphNodeId]
        var i: Int = 0
        while (i < and.conjunctsDgnIdsLength()) {
          conjuncts += and.conjunctsDgnIds(i)
          i += 1
        }
        DomainGraphNode.And(conjuncts.result())

      case persistence.DomainGraphNode.NotNode =>
        val not = makeNode(new persistence.NotNode()).asInstanceOf[persistence.NotNode]
        DomainGraphNode.Not(not.negatedDgnId)

      case persistence.DomainGraphNode.MuNode =>
        val mu = makeNode(new persistence.MuNode()).asInstanceOf[persistence.MuNode]
        DomainGraphNode.Mu(MuVariableName(mu.variable), mu.dgnId)

      case persistence.DomainGraphNode.MuVarNode =>
        val muVar = makeNode(new persistence.MuVarNode()).asInstanceOf[persistence.MuVarNode]
        DomainGraphNode.MuVar(MuVariableName(muVar.variable))

      case other =>
        throw new InvalidUnionType(other, persistence.DomainGraphNode.names)
    }

  private[this] def writeBoxedDomainGraphNode(builder: FlatBufferBuilder, dgn: DomainGraphNode): Offset = {
    val TypeAndOffset(nodeTyp, nodeOff) = writeDomainGraphNode(builder, dgn)
    persistence.BoxedDomainGraphNode.createBoxedDomainGraphNode(builder, nodeTyp, nodeOff)
  }

  private[this] def readBoxedDomainGraphNode(branch: persistence.BoxedDomainGraphNode): DomainGraphNode =
    readDomainGraphNode(branch.nodeType, branch.node(_))

  val format: BinaryFormat[DomainGraphNode] = new PackedFlatBufferBinaryFormat[DomainGraphNode] {
    def writeToBuffer(builder: FlatBufferBuilder, dgn: DomainGraphNode): Offset =
      writeBoxedDomainGraphNode(builder, dgn)

    def readFromBuffer(buffer: ByteBuffer): DomainGraphNode =
      readBoxedDomainGraphNode(persistence.BoxedDomainGraphNode.getRootAsBoxedDomainGraphNode(buffer))
  }
}

package com.thatdot.quine.model

import scala.jdk.CollectionConverters._

import com.google.common.hash.Hashing.{combineOrdered, combineUnordered}
import com.google.common.hash.{HashCode, Hasher, Hashing}

import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId

/** Persistent data model for [[DomainGraphBranch]].
  * The difference between [[DomainGraphBranch]] and [[DomainGraphNode]] is:
  * [[DomainGraphBranch]] children are modeled by-value (as JVM references to more [[DomainGraphBranch]] instances),
  * and [[DomainGraphNode]] children are modeled by-identity (as JVM ints aliased as [[DomainGraphNodeId]]).
  */
sealed abstract class DomainGraphNode(val children: Seq[DomainGraphNodeId])

/** Contains a [[DomainGraphNode]] and the [[DomainGraphNodeId]] that identifies it. */
final case class IdentifiedDomainGraphNode(dgnId: DomainGraphNodeId, domainGraphNode: DomainGraphNode)

object DomainGraphNode {
  type DomainGraphNodeId = Long

  final case class Single(
    domainNodeEquiv: DomainNodeEquiv,
    identification: Option[QuineId] = None,
    nextNodes: Seq[DomainGraphEdge],
    comparisonFunc: NodeLocalComparisonFunc = NodeLocalComparisonFunctions.EqualSubset
  ) extends DomainGraphNode(nextNodes.map(_.dgnId))

  final case class Or(disjuncts: Seq[DomainGraphNodeId]) extends DomainGraphNode(disjuncts)

  final case class And(conjuncts: Seq[DomainGraphNodeId]) extends DomainGraphNode(conjuncts)

  final case class Not(negated: DomainGraphNodeId) extends DomainGraphNode(Seq(negated))

  final case class Mu(
    variable: MuVariableName,
    dgnId: DomainGraphNodeId // scope of the variable
  ) extends DomainGraphNode(Seq(dgnId))

  final case class MuVar(variable: MuVariableName) extends DomainGraphNode(Seq.empty)

  final case class DomainGraphEdge(
    edge: GenericEdge,
    depDirection: DependencyDirection,
    dgnId: DomainGraphNodeId,
    circularMatchAllowed: Boolean,
    constraints: EdgeMatchConstraints
  )

  /** Computes a [[DomainGraphNodeId]] from a [[DomainGraphNode]]
    * by deterministically combining every field in [[DomainGraphNode]] into a hash code.
    */
  def id(domainGraphNode: DomainGraphNode): DomainGraphNodeId = DGNHash(domainGraphNode).asLong
}

object DGNHash {
  import DomainGraphNode._

  def apply(domainGraphNode: DomainGraphNode): HashCode =
    putDomainGraphNode(domainGraphNode, newHasher).hash

  private def putOrdered[T](seq: Seq[T], into: Hasher, putElement: T => HashCode): Hasher = {
    val size = seq.size
    into.putInt(size)
    if (size > 0) into.putBytes(combineOrdered(seq.map(putElement).asJava).asBytes)
    into
  }

  private def putUnordered[T](iter: Iterable[T], into: Hasher, putElement: T => HashCode): Hasher = {
    val seq = iter.toList
    val size = seq.size
    into.putInt(size)
    if (size > 0) into.putBytes(combineUnordered(seq.map(putElement).asJava).asBytes)
    into
  }

  private def putOption[T](opt: Option[T], into: Hasher, putElement: T => HashCode): Hasher =
    putOrdered(opt.toList, into, putElement)

  // hash function implementing the 128-bit murmur3 algorithm
  private def newHasher = Hashing.murmur3_128.newHasher

  private def putQuineValueMapKeyValue(keyValue: (String, QuineValue), into: Hasher): Hasher = {
    val (key, value) = keyValue
    into.putUnencodedChars(key)
    putQuineValue(value, into)
  }

  private def putQuineValue(from: QuineValue, into: Hasher): Hasher =
    from match {
      case QuineValue.Str(string) =>
        into.putByte(0)
        into.putUnencodedChars(string)
      case QuineValue.Integer(long) =>
        into.putByte(1)
        into.putLong(long)
      case QuineValue.Floating(double) =>
        into.putByte(2)
        into.putDouble(double)
      case QuineValue.True =>
        into.putByte(3)
        into.putBoolean(true)
      case QuineValue.False =>
        into.putByte(4)
        into.putBoolean(false)
      case QuineValue.Null =>
        into.putByte(5)
      case QuineValue.Bytes(bytes) =>
        into.putByte(6)
        into.putBytes(bytes)
      case QuineValue.List(list) =>
        into.putByte(7)
        putOrdered[QuineValue](
          list,
          into,
          putQuineValue(_, newHasher).hash
        )
      case QuineValue.Map(map) =>
        into.putByte(8)
        putUnordered[(String, QuineValue)](
          map,
          into,
          putQuineValueMapKeyValue(_, newHasher).hash
        )
      case QuineValue.DateTime(time) =>
        into.putByte(9)
        into.putLong(time.getEpochSecond)
        into.putInt(time.getNano)
      case QuineValue.Id(id) =>
        into.putByte(10)
        into.putBytes(id.array)
      case QuineValue.Duration(d) =>
        into.putByte(11)
        into.putLong(d.getSeconds)
        into.putInt(d.getNano)
    }

  private def putLocalProp(key: Symbol, fn: PropertyComparisonFunc, v: Option[PropertyValue], h: Hasher): Hasher = {
    h.putUnencodedChars(key.name)
    fn match {
      case PropertyComparisonFunctions.Identicality =>
        h.putByte(0)
      case PropertyComparisonFunctions.Wildcard =>
        h.putByte(1)
      case PropertyComparisonFunctions.NoValue =>
        h.putByte(2)
      case PropertyComparisonFunctions.NonIdenticality =>
        h.putByte(3)
      case PropertyComparisonFunctions.RegexMatch(pattern) =>
        h.putByte(4)
        h.putUnencodedChars(pattern)
      case PropertyComparisonFunctions.ListContains(mustContain) =>
        h.putByte(5)
        putUnordered[QuineValue](
          mustContain,
          h,
          putQuineValue(_, newHasher).hash
        )
    }
    putOption[Array[Byte]](
      v map (_.serialized),
      h,
      newHasher.putBytes(_).hash
    )
  }

  private def putDomainNodeEquiv(from: DomainNodeEquiv, into: Hasher): Hasher = {
    val DomainNodeEquiv(className, localProps, circularEdges) = from
    putOption[String](
      className,
      into,
      newHasher.putUnencodedChars(_).hash
    )
    putUnordered[(Symbol, (PropertyComparisonFunc, Option[PropertyValue]))](
      localProps,
      into,
      { case (key, (fn, v)) =>
        putLocalProp(key, fn, v, newHasher).hash
      }
    )
    putUnordered[(Symbol, IsDirected)](
      circularEdges,
      into,
      { case (symbol, directed) =>
        newHasher
          .putUnencodedChars(symbol.name)
          .putBoolean(directed)
          .hash
      }
    )
  }

  private def putDomainGraphNodeEdge(edge: DomainGraphEdge, into: Hasher): Hasher = {
    val DomainGraphEdge(
      GenericEdge(edgeType, edgeDirection),
      depDirection,
      dgnId,
      circularMatchAllowed,
      constraints
    ) = edge
    into
      .putUnencodedChars(edgeType.name)
      .putByte(edgeDirection match {
        case EdgeDirection.Outgoing => 0
        case EdgeDirection.Incoming => 1
        case EdgeDirection.Undirected => 2
      })
      .putByte(depDirection match {
        case DependsUpon => 0
        case IsDependedUpon => 1
        case Incidental => 2
      })
      .putLong(dgnId)
      .putBoolean(circularMatchAllowed)
    constraints match {
      case FetchConstraint(min, maxMatch) =>
        into
          .putByte(0)
          .putInt(min)
        maxMatch match {
          case Some(value) => into.putInt(value)
          case None => into.putByte(0)
        }
      case MandatoryConstraint =>
        into.putByte(1)
    }
  }

  private def putDomainGraphNode(from: DomainGraphNode, into: Hasher): Hasher =
    from match {
      case Single(domainNodeEquiv, identification, nextNodes, comparisonFunc) =>
        into.putByte(0)
        putDomainNodeEquiv(domainNodeEquiv, into)
        putOption[Array[Byte]](
          identification.map(_.array),
          into,
          newHasher.putBytes(_).hash
        )
        putOrdered[DomainGraphEdge](
          nextNodes,
          into,
          putDomainGraphNodeEdge(_, newHasher).hash
        )
        comparisonFunc match {
          case NodeLocalComparisonFunctions.Identicality =>
            into.putByte(0)
          case NodeLocalComparisonFunctions.EqualSubset =>
            into.putByte(1)
          case NodeLocalComparisonFunctions.Wildcard =>
            into.putByte(2)
        }
      case Or(disjuncts) =>
        into.putByte(1)
        putOrdered[Long](
          disjuncts,
          into,
          newHasher.putLong(_).hash
        )
      case And(conjuncts) =>
        into.putByte(2)
        putOrdered[Long](
          conjuncts,
          into,
          newHasher.putLong(_).hash
        )
      case Not(negated) =>
        into.putByte(3)
        into.putLong(negated)
      case Mu(MuVariableName(str), dgnId) =>
        into.putByte(4)
        into.putUnencodedChars(str)
        into.putLong(dgnId)
      case MuVar(MuVariableName(str)) =>
        into.putByte(5)
        into.putUnencodedChars(str)
    }
}

/** A collection of related [[DomainGraphNode]]s. [[population]] contains the entire set of [[DomainGraphNode]]s
  * referenced either directly or indirectly by the node identified by [[dgnId]], and does also include the node
  * identified by [[dgnId]].
  */
final case class DomainGraphNodePackage(dgnId: DomainGraphNodeId, population: Map[DomainGraphNodeId, DomainGraphNode])

object DomainGraphNodePackage {

  /** Recursively traverse the children of this [[DomainGraphNode]]
    * producing the map of all [[DomainGraphNodeId]] and [[DomainGraphNode]] in the tree.
    * Returned collection does include the [[DomainGraphNodeId]] at the root of the tree
    * (which is the passed-in value).
    *
    * @param dgnId root of the tree
    * @param getDomainGraphNode function that produces a [[DomainGraphNode]] from [[DomainGraphNodeId]]
    */
  def apply(
    dgnId: DomainGraphNodeId,
    getDomainGraphNode: DomainGraphNodeId => Option[DomainGraphNode]
  ): DomainGraphNodePackage = {
    def f(dgnId: DomainGraphNodeId): Map[DomainGraphNodeId, DomainGraphNode] = getDomainGraphNode(dgnId) match {
      case Some(dgn) =>
        (dgn match {
          case DomainGraphNode.Single(_, _, nextNodes, _) =>
            nextNodes.flatMap(nextNode => f(nextNode.dgnId)).toMap
          case DomainGraphNode.Or(disjuncts) => disjuncts.flatMap(f).toMap
          case DomainGraphNode.And(conjuncts) => conjuncts.flatMap(f).toMap
          case DomainGraphNode.Not(negated) => f(negated)
          case DomainGraphNode.Mu(_, dgnId) => f(dgnId)
          case DomainGraphNode.MuVar(_) => Map.empty[DomainGraphNodeId, DomainGraphNode]
        }) + (dgnId -> dgn)
      case None =>
        Map.empty[DomainGraphNodeId, DomainGraphNode]
    }
    // Keyword "new" is REQUIRED to call the case class constructor
    // and avoid recursively calling this apply function!
    new DomainGraphNodePackage(dgnId, f(dgnId))
  }
}

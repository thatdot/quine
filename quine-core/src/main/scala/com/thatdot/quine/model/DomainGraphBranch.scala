package com.thatdot.quine.model

import java.util.regex.Pattern

import scala.collection.compat._

import com.google.common.cache.{Cache, CacheBuilder}

import com.thatdot.quine.model
import com.thatdot.quine.model.DomainGraphNode.{DomainGraphEdge, DomainGraphNodeId}

/** A generic query instruction
  *
  * Historical note: there was a time when DomainGraphBranch was the 'only' way to interact
  * with the datastore.
  */
sealed abstract class DomainGraphBranch {

  /** Total depth of the [[DomainGraphBranch]] */
  def length: Int

  /** Total nodes in the [[DomainGraphBranch]] */
  def size: Int

  /** Format this DGB as a "pretty" (multiline, indented) string
    * @param indentCount the base number of indents to add to the serialized string
    * @return the pretty representation
    */
  def pretty(indentCount: Int = 0): String

  /** Direct children of this DGB -- that is, branches to which this branch may subscribe
    *
    * INV: elements are distinct (ie, children.toSet has the same elements as children)
    */
  def children: Iterable[DomainGraphBranch]

  /** Recursively traverses the [[DomainGraphBranch]], producing a [[DomainGraphNode]] and [[DomainGraphNodeId]]
    * for every node in the [[DomainGraphBranch]] tree, returned in a [[DomainGraphNodePackage]].
    */
  def toDomainGraphNodePackage: DomainGraphNodePackage
}

object DomainGraphBranch {

  /** A query which creates nothing and succeeds everywhere (with one empty
    * result)
    */
  def empty: SingleBranch = SingleBranch.empty

  def wildcard: SingleBranch = SingleBranch(
    DomainNodeEquiv.empty,
    None,
    Nil,
    NodeLocalComparisonFunctions.Wildcard
  )

  /** Cache domainGraphBranch values for re-use. */
  private val domainGraphBranchCache: Cache[DomainGraphNodeId, Option[DomainGraphBranch]] = CacheBuilder
    .newBuilder()
    .asInstanceOf[CacheBuilder[Any, Any]]
    .weakKeys()
    .build[DomainGraphNodeId, Option[DomainGraphBranch]]()

  /** Produces a [[DomainGraphBranch]] from the provided [[DomainGraphNodeId]]. */
  def fromDomainGraphNodeId(
    dgnId: DomainGraphNodeId,
    getDomainGraphNode: DomainGraphNodeId => Option[DomainGraphNode]
  ): Option[DomainGraphBranch] = {

    def retrieveValue(
      dgnId: DomainGraphNodeId,
      getDomainGraphNode: DomainGraphNodeId => Option[DomainGraphNode]
    ): Option[DomainGraphBranch] = {
      val f = fromDomainGraphNodeId(_, getDomainGraphNode)
      getDomainGraphNode(dgnId) flatMap {
        case DomainGraphNode.Single(domainNodeEquiv, identification, nextNodes, comparisonFunc) =>
          val edges = nextNodes.toList.flatMap {
            case DomainGraphEdge(edge, depDirection, edgeDgnId, circularMatchAllowed, constraints) =>
              f(edgeDgnId).map {
                model.DomainEdge(edge, depDirection, _, circularMatchAllowed, constraints)
              }
          }
          Option.when(nextNodes.size == edges.size)(
            model.SingleBranch(domainNodeEquiv, identification, edges, comparisonFunc)
          )
        case DomainGraphNode.Or(disjuncts) =>
          val disjunctDgbs = disjuncts.flatMap(f(_))
          Option.when(disjunctDgbs.size == disjuncts.size)(model.Or(disjunctDgbs.toList))
        case DomainGraphNode.And(conjuncts) =>
          val conjunctsDgbs = conjuncts.flatMap(f(_))
          Option.when(conjunctsDgbs.size == conjuncts.size)(model.And(conjunctsDgbs.toList))
        case DomainGraphNode.Not(negated) =>
          f(negated) map model.Not
        case DomainGraphNode.Mu(variable, dgnId) =>
          f(dgnId) map (model.Mu(variable, _))
        case DomainGraphNode.MuVar(variable) =>
          Some(model.MuVar(variable))
      }
    }

    domainGraphBranchCache.getIfPresent(dgnId) match {
      case b @ Some(_) => b
      case _ =>
        val branch = retrieveValue(dgnId, getDomainGraphNode)
        domainGraphBranchCache.put(dgnId, branch)
        branch
    }
  }
}

/** Query for creating, fetching, or testing for a particular structure rooted at a node
  *
  * @param domainNodeEquiv a specification of the node's local properties and circular edges
  * @param identification a particular node ID to require
  * @param nextBranches recursive subqueries (on nodes found by following edges)
  * @param comparisonFunc how to evaluate the domainNodeEquiv against a potentially-matching node
  */
final case class SingleBranch(
  domainNodeEquiv: DomainNodeEquiv,
  identification: Option[QuineId] = None,
  nextBranches: List[DomainEdge],
  comparisonFunc: NodeLocalComparisonFunc = NodeLocalComparisonFunctions.EqualSubset
) extends DomainGraphBranch {

  def length: Int = 1 + nextBranches.foldLeft(0)((acc, e) => Math.max(acc, e.branch.length))
  def size: Int = 1 + nextBranches.foldLeft(0)((acc, e) => acc + e.branch.size)

  def identifyRoot(id: QuineId): SingleBranch = this.copy(identification = Some(id))

  override def pretty(indentCount: Int = 0): String = {
    def indents(count: Int = indentCount) = "\t" * count
    s"""${indents()}SingleBranch(
       |${indents(indentCount + 1)}$domainNodeEquiv,
       |${indents(indentCount + 1)}$identification,
       |${indents(indentCount + 1)}$comparisonFunc,
       |${indents(indentCount + 1)}Set(${if (nextBranches.nonEmpty) "\n" else ""}${nextBranches
      .map(c => c.toString(indentCount + 2))
      .mkString(",\n")}${if (nextBranches.nonEmpty) {
      "\n" + indents(indentCount + 1) + ")"
    } else ")"}
       |${indents(indentCount)})""".stripMargin
  }

  val children: Iterable[DomainGraphBranch] =
    nextBranches.collect { case DomainEdge(_, _, child, _, _) =>
      child
    }.toSet

  def toDomainGraphNodePackage: DomainGraphNodePackage = {
    val edges = nextBranches map { case DomainEdge(e, depDirection, branch, circularMatchAllowed, constraints) =>
      val DomainGraphNodePackage(childDgnId, population) = branch.toDomainGraphNodePackage
      val edge = DomainGraphEdge(e, depDirection, childDgnId, circularMatchAllowed, constraints)
      (edge, population)
    }
    val dgn = DomainGraphNode.Single(domainNodeEquiv, identification, edges.map(_._1), comparisonFunc)
    val id = DomainGraphNode.id(dgn)
    DomainGraphNodePackage(id, Map(id -> dgn) ++ edges.flatMap(_._2))
  }
}
object SingleBranch {
  // a branch that is consistent with being rooted at any arbitrary node.
  def empty: SingleBranch = SingleBranch(
    DomainNodeEquiv.empty,
    None,
    List.empty,
    NodeLocalComparisonFunctions.EqualSubset
  )
}

/** Query for fetching/testing several queries and concatenating all of their
  * results.
  *
  * @param disjuncts queries whose results we concatenate
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class Or(disjuncts: List[DomainGraphBranch]) extends DomainGraphBranch {

  override def length: Int = 1 + disjuncts.foldLeft(0)((acc, y) => Math.max(acc, y.length))
  override def size: Int = 1 + disjuncts.foldLeft(0)((acc, y) => acc + y.size)

  def pretty(indentCount: Int = 0): String = {
    val indents = "\t" * indentCount
    if (disjuncts.isEmpty) {
      s"${indents}Or()"
    } else {
      s"""${indents}Or(
         |$indents${disjuncts.map(_.pretty(indentCount + 1)).mkString(",\n")}
         |$indents)""".stripMargin
    }
  }

  val children: Iterable[DomainGraphBranch] = disjuncts.toSet

  def toDomainGraphNodePackage: DomainGraphNodePackage = {
    val recursiveResult = disjuncts.map(_.toDomainGraphNodePackage)
    val dgn = DomainGraphNode.Or(recursiveResult.map(_.dgnId))
    val id = DomainGraphNode.id(dgn)
    DomainGraphNodePackage(id, Map(id -> dgn) ++ recursiveResult.flatMap(_.population))
  }
}

/** Query for fetching/testing several results and taking the cross-product of
  * all of their results.
  *
  * @param conjuncts queries whose results we combine
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class And(conjuncts: List[DomainGraphBranch]) extends DomainGraphBranch {

  override def length: Int = 1 + conjuncts.foldLeft(0)((acc, y) => Math.max(acc, y.length))
  override def size: Int = 1 + conjuncts.foldLeft(0)((acc, y) => acc + y.size)

  def pretty(indentCount: Int = 0): String = {
    val indents = "\t" * indentCount
    if (conjuncts.isEmpty) {
      s"${indents}And()"
    } else {
      s"""${indents}And(
         |${conjuncts.map(_.pretty(indentCount + 1)).mkString(",\n")}
         |$indents)""".stripMargin
    }
  }

  val children: Iterable[DomainGraphBranch] = conjuncts.toSet

  def toDomainGraphNodePackage: DomainGraphNodePackage = {
    val recursiveResult = conjuncts.map(_.toDomainGraphNodePackage)
    val dgn = DomainGraphNode.And(recursiveResult.map(_.dgnId))
    val id = DomainGraphNode.id(dgn)
    DomainGraphNodePackage(id, Map(id -> dgn) ++ recursiveResult.flatMap(_.population))
  }
}

/** Query for checking that a certain subquery is 'not' matched.
  *
  * NB: in the fetching case, [[Not]] always returns one empty node component
  * since a successful match requires the negated query to produce nothing.
  *
  * @param negated query we are testing
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class Not(negated: DomainGraphBranch) extends DomainGraphBranch {

  override def length: Int = 1 + negated.length
  override def size: Int = 1 + negated.size

  def pretty(indentCount: Int = 0): String = {
    val indents = "\t" * indentCount
    s"""${indents}Not(
       |${negated.pretty(indentCount + 1)}
       |$indents)""".stripMargin
  }

  val children: Iterable[DomainGraphBranch] = Set(negated)

  def toDomainGraphNodePackage: DomainGraphNodePackage = {
    val recursiveResult = negated.toDomainGraphNodePackage
    val dgn = DomainGraphNode.Not(recursiveResult.dgnId)
    val id = DomainGraphNode.id(dgn)
    DomainGraphNodePackage(id, Map(id -> dgn) ++ recursiveResult.population)
  }
}

/** See [[Mu]] and [[MuVar]] */
final case class MuVariableName(str: String) extends AnyVal

/** Potentially recursive query
  *
  * This lets us handle recursive queries, without bounding the depth of the
  * recursion (aka. unfolding the recursion to a fixed depth). The idea behind
  * this is inspired by type-checking of [iso-recursive types][0].
  *
  * Mutual recursion (and other more complex forms of recursion) can be handled
  * with interwoven [[Mu]]'s.
  *
  * [0]: https://en.wikipedia.org/wiki/Recursive_data_type#Isorecursive_types
  *
  * @param variable the variable which stands for the recursive query
  * @param branch the recursive query, almost always using `variable`
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class Mu(
  variable: MuVariableName,
  branch: DomainGraphBranch // scope of the variable
) extends DomainGraphBranch {

  def unfold: DomainGraphBranch = Substitution.substitute(branch, variable, this)

  override def length: Int = 1 + branch.length
  override def size: Int = 1 + branch.size

  def pretty(indentCount: Int = 0): String = {
    def indents(i: Int = indentCount) = "\t" * i
    s"""${indents()}Mu(
       |${indents(indentCount + 1)}${variable.str},
       |${branch.pretty(indentCount + 1)}
       |${indents()})""".stripMargin
  }

  val children: Iterable[DomainGraphBranch] = Set(branch)

  def toDomainGraphNodePackage: DomainGraphNodePackage = {
    val recursiveResult = branch.toDomainGraphNodePackage
    val dgn = DomainGraphNode.Mu(variable, recursiveResult.dgnId)
    val id = DomainGraphNode.id(dgn)
    DomainGraphNodePackage(id, Map(id -> dgn) ++ recursiveResult.population)
  }
}

/** Variable standing in for a recursive query
  *
  * INVARIANT: a [[MuVar]] should always be nested (possibly far inside) some
  * enclosing [[Mu]], which has a matching `variable` field.
  *
  * @param variable the variable standing for a recursive query - bound in some
  *                 enclosing [[Mu]]
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class MuVar(variable: MuVariableName) extends DomainGraphBranch {

  override def length: Int = 1
  override def size: Int = 1

  def pretty(indentCount: Int = 0): String = {
    val indents = "\t" * indentCount
    s"${indents}MuVar(${variable.str})"
  }

  val children: Iterable[DomainGraphBranch] = Iterable.empty

  def toDomainGraphNodePackage: DomainGraphNodePackage = {
    val dgn = DomainGraphNode.MuVar(variable)
    val id = DomainGraphNode.id(dgn)
    DomainGraphNodePackage(id, Map(id -> dgn))
  }
}

object Substitution {
  def substitute(
    substituteIn: DomainGraphBranch,
    variable: MuVariableName,
    branch: DomainGraphBranch
  ): DomainGraphBranch = substituteIn match {
    case SingleBranch(done, id, nextBranches, comparisonFunc) =>
      val nextBranchesSubstituted =
        nextBranches.map(nextBranch => nextBranch.copy(branch = substitute(nextBranch.branch, variable, branch)))
      SingleBranch(done, id, nextBranchesSubstituted, comparisonFunc)

    case And(conjuncts) =>
      And(conjuncts.map(conjunct => substitute(conjunct, variable, branch)))

    case Or(disjuncts) =>
      And(disjuncts.map(disjunct => substitute(disjunct, variable, branch)))

    case Not(negated) =>
      Not(substitute(negated, variable, branch))

    case Mu(variableMu, branchMu) =>
      assert(
        variableMu != variable
      ) // This should not be possible - fresh vars must be used for every [[Mu]]
      Mu(variableMu, substitute(branchMu, variable, branch))

    case v @ MuVar(variable2) => if (variable == variable2) branch else v
  }
}

sealed abstract class NodeLocalComparisonFunc {
  def apply(qn: QueryNode, fn: FoundNode): Boolean
}
object NodeLocalComparisonFunctions {
  import DGBOps.DNMapOps

  case object Identicality extends NodeLocalComparisonFunc with Serializable {
    def apply(qn: QueryNode, fn: FoundNode): Boolean = qn == fn
  }

  case object EqualSubset extends NodeLocalComparisonFunc with Serializable {
    def apply(qn: QueryNode, fn: FoundNode): Boolean =
      (qn.className.isEmpty || qn.className == fn.className) &&
      fn.localProps.containsByBothConditions(qn.localProps)
  }

  case object Wildcard extends NodeLocalComparisonFunc with Serializable {
    def apply(qn: QueryNode, fn: FoundNode) = true
  }
}

sealed abstract class PropertyComparisonFunc {

  /** * Apply the comparison function, with `qp` as the LHS of the operator and `fp` as the RHS
    */
  def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean
}

case object PropertyComparisonFunctions {

  /** A value exists and matches the specified value exactly. */
  case object Identicality extends PropertyComparisonFunc {
    def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean = qp == fp
  }

  /** Any value matches as long as one exists. */
  case object Wildcard extends PropertyComparisonFunc {
    def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean = fp.nonEmpty
  }

  /** Value must not exist. */
  case object NoValue extends PropertyComparisonFunc {
    def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean = fp.isEmpty
  }

  /** A value either does exist or is any other value or type other than the specified value/type. */
  case object NonIdenticality extends PropertyComparisonFunc {
    def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean = (qp, fp) match {
      case (q: Some[PropertyValue], f) => q != f
      // Like comparing to null, if the test condition is None, then everything is NonIdentical, even None:
      case (None, _) => true
    }
  }

  /** Compare a property against a cached regex pattern
    *
    * @param pattern regex to match
    */
  final case class RegexMatch(pattern: String) extends PropertyComparisonFunc {

    val compiled: Pattern = Pattern.compile(pattern)

    /** Returns true if the found property is a string that matches the regex */
    def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean =
      fp match {
        case Some(PropertyValue(QuineValue.Str(testing))) =>
          compiled.matcher(testing).matches
        case _ =>
          false
      }
  }

  final case class ListContains(mustContain: Set[QuineValue]) extends PropertyComparisonFunc {

    /** Returns true if the found property is a list that contains all the specified values */
    def apply(qp: Option[PropertyValue], fp: Option[PropertyValue]): Boolean =
      fp match {
        case Some(PropertyValue(QuineValue.List(values))) =>
          mustContain.subsetOf(values.toSet)
        case _ =>
          false
      }
  }
}

sealed abstract class DependencyDirection {
  val reverse: DependencyDirection
}
case object DependsUpon extends DependencyDirection { val reverse = IsDependedUpon }
case object IsDependedUpon extends DependencyDirection { val reverse = DependsUpon }
case object Incidental extends DependencyDirection { val reverse = Incidental }

final case class GenericEdge(edgeType: Symbol, direction: EdgeDirection) {
  def reverse: GenericEdge = this.copy(direction = direction.reverse)
  def toHalfEdge(other: QuineId): HalfEdge = HalfEdge(edgeType, direction, other)
  override def toString: String = s"GenericEdge(${edgeType.name},$direction)"
}

// TODO unify with [[DomainGraphEdge]], if possible: these differ only in that one uses a DGB and one uses a DGN ID
final case class DomainEdge(
  edge: GenericEdge,
  depDirection: DependencyDirection,
  branch: DomainGraphBranch,
  circularMatchAllowed: Boolean = false,
  constraints: EdgeMatchConstraints = MandatoryConstraint
) {

  def toString(indent: Int = 0): String = {
    def indents(count: Int) = "\t" * count
    s"""${indents(indent)}DomainEdge(
       |${indents(indent + 1)}$edge,
       |${indents(indent + 1)}$depDirection,
       |${indents(indent + 1)}circularMatchAllowed = $circularMatchAllowed,
       |${indents(indent + 1)}$constraints,
       |${branch.pretty(indent + 1)}
       |${indents(indent)})""".stripMargin
  }
}

sealed abstract class EdgeMatchConstraints {
  val min: Int

  // `maxMatch` should express whether a match fails in the runtime when found edges > max.
  // The possibility of building results as subsets of total matches (via maxConstruct size) is a model issue.
  val maxMatch: Option[Int]
}

final case class FetchConstraint(min: Int, maxMatch: Option[Int]) extends EdgeMatchConstraints
case object MandatoryConstraint extends EdgeMatchConstraints {
  val min = 1
  val maxMatch: Option[Int] = None
}

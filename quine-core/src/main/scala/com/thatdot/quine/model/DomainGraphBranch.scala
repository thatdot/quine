package com.thatdot.quine.model

import java.util.regex.Pattern

sealed abstract class ExecInstruction

sealed abstract class Create extends ExecInstruction

sealed abstract class Fetch extends Create

sealed abstract class Test extends Fetch

//sealed abstract class FetchOptional extends Fetch ???

/** A generic query instruction
  *
  * The phantom type parameter constrains the contexts in which a query works:
  *
  *   - [[Create]] query can be used to create, fetch, or test
  *   - [[Fetch]] query can be used to fetch, test
  *   - [[Test]] query can be used to test
  *
  * This ends up working even in the Scala type-system. Due to contravariance,
  * `DomainGraphBranch[Create] <: DomainGraphBranch[Fetch]` and
  * `DomainGraphBranch[Fetch]  <: DomainGraphBranch[Test]`.
  *
  * In practice, this means functions taking DomainGraphBranch arguments should use the "lowest" ExecInstruction they
  * can. For example, a function that only needs to test values, but should also support Create and Fetch DGBs would
  * have a signature like `def iOnlyTest(dgb: DomainGraphBranch[Test]): Boolean`. A function that should only support
  * Create DGBs, but not Fetch or Test, might have a signature like `def iCreateThings(dgb: DomainGraphBranch[Create])`
  *
  * Historical note: there was a time when DomainGraphBranch was the 'only' way to interact
  * with the datastore.
  */
sealed abstract class DomainGraphBranch[-T <: ExecInstruction] {

  /** Total depth of the [[DomainGraphBranch]] */
  def length: Int

  /** Total nodes in the [[DomainGraphBranch]] */
  def size: Int

  /** Safely cast a [[DomainGraphBranch]] into a super type */
  def upcast[U <: ExecInstruction](ev: U <:< T): DomainGraphBranch[U] = this.asInstanceOf

  /** Format this DGB as a "pretty" (multiline, indented) string
    * @param indentCount the base number of indents to add to the serialized string
    * @return the pretty representation
    */
  def pretty(indentCount: Int = 0): String

  /** Direct children of this DGB -- that is, branches to which this branch may subscribe
    *
    * INV: elements are distinct (ie, children.toSet has the same elements as children)
    */
  def children: Iterable[DomainGraphBranch[T]]
}

object DomainGraphBranch {

  /** A query which creates nothing and succeeds everywhere (with one empty
    * result)
    */
  def empty: SingleBranch[Create] = SingleBranch.empty

  def wildcard: SingleBranch[Test] = SingleBranch[Test](
    DomainNodeEquiv.empty,
    None,
    Nil,
    NodeLocalComparisonFunctions.Wildcard
  )

  /** Since there is only one class extending `DomainGraphBranch[Create]`, it is
    * safe to convert any such instance into `SingleBranch[Create]`
    *
    * The Scala compiler won't figure that out on its own though, hence the need
    * for this implicit conversion
    */
  implicit def asSingleBranch(dgb: DomainGraphBranch[Create]): SingleBranch[Create] = dgb match {
    case l: SingleBranch[Create] => l
    case _ =>
      throw new Exception(
        s"Only `$SingleBranch[Create]` can be a `$DomainGraphBranch[Create]`. You had: $dgb"
      )
  }
}

/** Query for creating, fetching, or testing for a particular structure rooted at a node
  *
  * @param domainNodeEquiv a specification of the node's local properties and circular edges
  * @param identification a particular node ID to require
  * @param nextBranches recursive subqueries (on nodes found by following edges)
  * @param comparisonFunc how to evaluate the domainNodeEquiv against a potentially-matching node
  */
final case class SingleBranch[-T <: ExecInstruction](
  domainNodeEquiv: DomainNodeEquiv,
  identification: Option[QuineId] = None,
  nextBranches: List[DomainEdge[T]],
  comparisonFunc: NodeLocalComparisonFunc = NodeLocalComparisonFunctions.EqualSubset
) extends DomainGraphBranch[T] {

  def length: Int = 1 + nextBranches.foldLeft(0)((acc, e) => Math.max(acc, e.branch.length))
  def size: Int = 1 + nextBranches.foldLeft(0)((acc, e) => acc + e.branch.size)

  def identifyRoot(id: QuineId): SingleBranch[T] = this.copy(identification = Some(id))

  /** Linearize a [[Create]] branch into a list of paths, and put sort these in dependency order.
    *
    * @param ev value-level proof that [[T]] is a super type of [[Create]]. Coupled with [[subtypeCreate]], this implies
    *           that [[T]] actually is equal to [[Create]]. We use [[<:<]] instead of [[=:=]] to satisfy constraints
    *           imposed by the fact that [[DomainGraphBranch]] is upcast.
    * @return a list of linear paths
    */
  def allLinearComponentsInDependencyOrder(implicit ev: Create <:< T): List[DomainGraphPath] = {
    // `Create <:< T` and `T <:< Create` ==> `T =:= Create` ==> `LocalProps[T] =:= LocalProps[Create]`
    val self = this.asInstanceOf[SingleBranch[Create]]
    DGBOps.linearComponentsInDependencyOrder(self)
  }

  // This only can be called when T = Create.
  def isSuperSetOf(other: DomainGraphBranch[Create])(implicit ev: Create <:< T): Boolean =
    // `Create <:< T` and `T <:< Create` ==> `T =:= Create` ==> `List[DomainEdge[T]] =:= List[DomainEdge[Create]]`
    NodeLocalComparisonFunctions.EqualSubset(domainNodeEquiv, other.domainNodeEquiv) &&
    (identification.isEmpty || other.identification.isEmpty || (identification == other.identification)) &&
    other.nextBranches.forall(c => nextBranches.exists(b => b.isSuperSetOf(c))) &&
    comparisonFunc == other.comparisonFunc

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

  val children: Iterable[DomainGraphBranch[T]] =
    nextBranches.collect { case DomainEdge(_, _, child, _, _) =>
      child
    }.toSet
}
object SingleBranch {
  // a branch that is consistent with being rooted at any arbitrary node.
  def empty: SingleBranch[Create] = SingleBranch[Create](
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
final case class Or[-T <: ExecInstruction](disjuncts: List[DomainGraphBranch[T]]) extends DomainGraphBranch[T] {

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

  val children: Iterable[DomainGraphBranch[T]] = disjuncts.toSet
}

/** Query for fetching/testing several results and taking the cross-product of
  * all of their results.
  *
  * @param conjuncts queries whose results we combine
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class And[-T <: ExecInstruction](conjuncts: List[DomainGraphBranch[T]]) extends DomainGraphBranch[T] {

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

  val children: Iterable[DomainGraphBranch[T]] = conjuncts.toSet
}

/** Query for checking that a certain subquery is 'not' matched.
  *
  * NB: in the fetching case, [[Not]] always returns one empty node component
  * since a successful match requires the negated query to produce nothing.
  *
  * @param negated query we are testing
  * INV: T is not Create -- in particular, there exists T <:< Fetch
  */
final case class Not[-T <: ExecInstruction](negated: DomainGraphBranch[T]) extends DomainGraphBranch[T] {

  override def length: Int = 1 + negated.length
  override def size: Int = 1 + negated.size

  def pretty(indentCount: Int = 0): String = {
    val indents = "\t" * indentCount
    s"""${indents}Not(
       |${negated.pretty(indentCount + 1)}
       |$indents)""".stripMargin
  }

  val children: Iterable[DomainGraphBranch[T]] = Set(negated)
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
final case class Mu[-T <: ExecInstruction](
  variable: MuVariableName,
  branch: DomainGraphBranch[T] // scope of the variable
) extends DomainGraphBranch[T] {

  def unfold: DomainGraphBranch[T] = Substitution.substitute(branch, variable, this)

  override def length: Int = 1 + branch.length
  override def size: Int = 1 + branch.size

  def pretty(indentCount: Int = 0): String = {
    def indents(i: Int = indentCount) = "\t" * i
    s"""${indents()}Mu(
       |${indents(indentCount + 1)}${variable.str},
       |${branch.pretty(indentCount + 1)}
       |${indents()})""".stripMargin
  }

  val children: Iterable[DomainGraphBranch[T]] = Set(branch)
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
final case class MuVar[-T <: ExecInstruction](variable: MuVariableName) extends DomainGraphBranch[T] {

  override def length: Int = 1
  override def size: Int = 1

  def pretty(indentCount: Int = 0): String = {
    val indents = "\t" * indentCount
    s"${indents}MuVar(${variable.str})"
  }

  val children: Iterable[DomainGraphBranch[T]] = Iterable.empty
}

object Substitution {
  def substitute[T <: ExecInstruction](
    subsituteIn: DomainGraphBranch[T],
    variable: MuVariableName,
    branch: DomainGraphBranch[T]
  ): DomainGraphBranch[T] = subsituteIn match {
    case SingleBranch(dne, id, nextBranches, comparisonFunc) =>
      val nextBranchesSubstituted =
        nextBranches.map(nextBranch => nextBranch.copy(branch = substitute[T](nextBranch.branch, variable, branch)))
      SingleBranch(dne, id, nextBranchesSubstituted, comparisonFunc)

    case And(conjuncts) =>
      And(conjuncts.map(conjunct => substitute[T](conjunct, variable, branch)))

    case Or(disjuncts) =>
      And(disjuncts.map(disjunct => substitute[T](disjunct, variable, branch)))

    case Not(negated) =>
      Not(substitute[T](negated, variable, branch))

    case Mu(variableMu, branchMu) =>
      assert(
        variableMu != variable
      ) // This should not be possible - fresh vars must be used for every [[Mu]]
      Mu(variableMu, substitute[T](branchMu, variable, branch))

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

final case class DomainEdge[-T <: ExecInstruction](
  edge: GenericEdge,
  depDirection: DependencyDirection,
  branch: DomainGraphBranch[T],
  circularMatchAllowed: Boolean = false,
  constraints: EdgeMatchConstraints[T] = MandatoryConstraint
) {

  // This only can be called when T = Create.
  def isSuperSetOf(other: DomainEdge[Create])(implicit ev: Create <:< T): Boolean =
    edge == other.edge &&
    branch.upcast(ev).isSuperSetOf(other.branch) &&
    circularMatchAllowed == other.circularMatchAllowed &&
    constraints >= other.constraints

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

sealed abstract class EdgeMatchConstraints[-T <: ExecInstruction] {
  val min: Int

  // `maxMatch` should express whether a match fails in the runtime when found edges > max.
  // The possibility of building results as subsets of total matches (via maxConstruct size) is a model issue.
  val maxMatch: Option[Int]

  /** Checks if this constraint is at least as restrictive as another constraint */
  def >=[U <: ExecInstruction](other: EdgeMatchConstraints[U]): Boolean =
    min >= other.min &&
    (other.maxMatch.isEmpty || maxMatch.fold(false)(_ <= other.maxMatch.get))
}

final case class FetchConstraint(min: Int, maxMatch: Option[Int]) extends EdgeMatchConstraints[Fetch]
case object MandatoryConstraint extends EdgeMatchConstraints[Create] {
  val min = 1
  val maxMatch: Option[Int] = None
}

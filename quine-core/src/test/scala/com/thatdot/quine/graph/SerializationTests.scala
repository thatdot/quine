package com.thatdot.quine.graph

import java.time.{
  Duration => JavaDuration,
  Instant,
  LocalDateTime => JavaLocalDateTime,
  ZonedDateTime => JavaZonedDateTime
}
import java.util.UUID
import java.util.regex.Pattern

import scala.collection.compat.immutable._
import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}
import scala.language.higherKinds

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.util.Buildable
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import shapeless.cachedImplicit

import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.SubscribersToThisNodeUtil
import com.thatdot.quine.graph.behavior.StandingQuerySubscribers
import com.thatdot.quine.graph.cypher.{
  Expr => CypherExpr,
  Func => CypherFunc,
  QueryContext,
  StandingQuery => CypherStandingQuery,
  StandingQueryState => CypherStandingQueryState,
  Value => CypherValue
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.{CypherSubscriber, ResultId}
import com.thatdot.quine.model
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.PersistenceCodecs

// WARNING: Using this can really make compile times explode!!!
// import org.scalacheck.ScalacheckShapeless._

/** The derived [[Arbitrary]] instances for some types get big fast. If the
  * serialization tests ever start being too slow, you can get scalacheck to
  * print out timing information for each property by adding the following to
  * `build.sbt`:
  *
  * {{{
  * testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3")
  * }}}
  */
trait ArbitraryInstances {

  /* Tweak the containers so that the generation size does _not_ get passed
   * through straight away. Instead, we pick a container size and then scale
   * the remaining size for the container values
   */
  implicit def arbContainer[C[_], T](implicit
    a: Arbitrary[T],
    b: Buildable[T, C[T]],
    t: C[T] => Iterable[T]
  ): Arbitrary[C[T]] = Arbitrary {
    Gen.sized(s =>
      Gen.choose(0, s).flatMap { s1 =>
        val s2 = s / Math.max(s1, 1)
        Gen.buildableOfN[C[T], T](s1, Gen.resize(s2, a.arbitrary))
      }
    )
  }
  implicit def arbContainer2[C[_, _], T, U](implicit
    a: Arbitrary[(T, U)],
    b: Buildable[(T, U), C[T, U]],
    t: C[T, U] => Iterable[(T, U)]
  ): Arbitrary[C[T, U]] = Arbitrary {
    Gen.sized(s =>
      Gen.choose(0, s).flatMap { s1 =>
        val s2 = s / Math.max(s1, 1)
        Gen.buildableOfN[C[T, U], (T, U)](s1, Gen.resize(s2, a.arbitrary))
      }
    )
  }

  /* This exposes a bunch of helpers that are similar to [[Gen.resultOf]] but which distribute the
   * generator size among the subterms (instead of passing it through). This is criticially
   * important to bound the total size of an AST. Just decrementing the size in the recursive case
   * it still not enough since you still get exponetial growth in tree size for linear growth in
   * ScalaCheck "size" parameter.
   */
  object GenApply {

    /** Split the current generator size into the specified number of sub-groups.
      *
      * The sum of the sizes should equal 1 less than the initial generator size. The length of the
      * list returned is equal to the requested number of groups.
      *
      * @param n how many sub-groups to split into?
      * @return size of sub-groups
      */
    private[this] def partitionSize(n: Int): Gen[Seq[Int]] =
      for {
        size <- Gen.size
        decrementedSize = size - 1
        if decrementedSize >= 0
        groupSize = decrementedSize / n
        remainder = decrementedSize % n
        groups = List.tabulate(n)(i => if (i < remainder) 1 + groupSize else groupSize)
        shuffledGroups <- Gen.pick(n, groups)
      } yield shuffledGroups.toList

    def resultOf[T1: Arbitrary, R](f: T1 => R): Gen[R] =
      for {
        Seq(s1) <- partitionSize(1)
        t1 <- Gen.resize(s1, arbitrary[T1])
      } yield f(t1)

    def resultOf[T1: Arbitrary, T2: Arbitrary, R](f: (T1, T2) => R): Gen[R] =
      for {
        Seq(s1, s2) <- partitionSize(2)
        t1 <- Gen.resize(s1, arbitrary[T1])
        t2 <- Gen.resize(s2, arbitrary[T2])
      } yield f(t1, t2)

    def resultOf[T1: Arbitrary, T2: Arbitrary, T3: Arbitrary, R](f: (T1, T2, T3) => R): Gen[R] =
      for {
        Seq(s1, s2, s3) <- partitionSize(3)
        t1 <- Gen.resize(s1, arbitrary[T1])
        t2 <- Gen.resize(s2, arbitrary[T2])
        t3 <- Gen.resize(s3, arbitrary[T3])
      } yield f(t1, t2, t3)

    def resultOf[T1: Arbitrary, T2: Arbitrary, T3: Arbitrary, T4: Arbitrary, R](f: (T1, T2, T3, T4) => R): Gen[R] =
      for {
        Seq(s1, s2, s3, s4) <- partitionSize(4)
        t1 <- Gen.resize(s1, arbitrary[T1])
        t2 <- Gen.resize(s2, arbitrary[T2])
        t3 <- Gen.resize(s3, arbitrary[T3])
        t4 <- Gen.resize(s4, arbitrary[T4])
      } yield f(t1, t2, t3, t4)

    def resultOf[T1: Arbitrary, T2: Arbitrary, T3: Arbitrary, T4: Arbitrary, T5: Arbitrary, R](
      f: (T1, T2, T3, T4, T5) => R
    ): Gen[R] =
      for {
        Seq(s1, s2, s3, s4, s5) <- partitionSize(5)
        t1 <- Gen.resize(s1, arbitrary[T1])
        t2 <- Gen.resize(s2, arbitrary[T2])
        t3 <- Gen.resize(s3, arbitrary[T3])
        t4 <- Gen.resize(s4, arbitrary[T4])
        t5 <- Gen.resize(s5, arbitrary[T5])
      } yield f(t1, t2, t3, t4, t5)

    def resultOf[T1: Arbitrary, T2: Arbitrary, T3: Arbitrary, T4: Arbitrary, T5: Arbitrary, T6: Arbitrary, R](
      f: (T1, T2, T3, T4, T5, T6) => R
    ): Gen[R] =
      for {
        Seq(s1, s2, s3, s4, s5, s6) <- partitionSize(6)
        t1 <- Gen.resize(s1, arbitrary[T1])
        t2 <- Gen.resize(s2, arbitrary[T2])
        t3 <- Gen.resize(s3, arbitrary[T3])
        t4 <- Gen.resize(s4, arbitrary[T4])
        t5 <- Gen.resize(s5, arbitrary[T5])
        t6 <- Gen.resize(s6, arbitrary[T6])
      } yield f(t1, t2, t3, t4, t5, t6)
  }

  /** This behaves like one big `oneOf`, except in the case that the size is 1 or smaller. In those
    * cases, only the "small" generators take part in the `oneOf`. This becomes useful in tree-like
    * structures because when reaching tree leaves, we want to avoid trying to generate non-leaf
    * nodes (since those will eventually just underflow the generator, making it fail and skip).
    *
    * This manifests as a test failure due to too many skipped test cases.
    *
    * @param small set of generators to try when size <= 1
    * @param other set of other generators to also try when size > 1
    * @return generator that tries both inputs sets of generators
    */
  def sizedOneOf[A](small: Seq[Gen[A]], other: Seq[Gen[A]]): Gen[A] = {
    val gens: IndexedSeq[Gen[A]] = (small ++ other).toIndexedSeq
    val smallGensMax: Int = math.max(0, small.length - 1)
    val allGensMax: Int = gens.length - 1
    if (allGensMax < 0) {
      Gen.fail[A]
    } else {
      for {
        size <- Gen.size
        genIdx <- Gen.choose(0, if (size <= 1) smallGensMax else allGensMax)
        a <- gens(genIdx)
      } yield a
    }
  }

  implicit val arbByteArray: Arbitrary[Array[Byte]] = cachedImplicit

  /** We want [[QuineId]] to be a nice mixture of mostly short arrays, with a
    * sprinkling of large ones. The generator for [[BigInt]] does exactly that!
    */
  implicit val arbQid: Arbitrary[QuineId] = Arbitrary {
    arbitrary[BigInt].map { (bi: BigInt) =>
      QuineId(bi.toByteArray)
    }
  }

  /** We want mostly short symbols, but on occasion some large ones too */
  implicit val arbSymbol: Arbitrary[Symbol] = Arbitrary {
    Gen
      .frequency(
        20 -> Gen.choose(0, 10),
        2 -> Gen.choose(10, 50),
        1 -> Gen.choose(50, 100)
      )
      .flatMap { n =>
        Gen
          .listOfN(n, Gen.alphaChar)
          .map { (l: List[Char]) =>
            Symbol(l.mkString)
          }
      }
  }

  implicit val arbDirection: Arbitrary[EdgeDirection] = Arbitrary {
    Gen.oneOf(
      EdgeDirection.Outgoing,
      EdgeDirection.Incoming,
      EdgeDirection.Undirected
    )
  }

  implicit val arbMilliseconds: Arbitrary[Milliseconds] = Arbitrary {
    arbitrary[Long].map(Milliseconds.apply)
  }

  implicit val arbEventTime: Arbitrary[EventTime] = Arbitrary {
    arbitrary[Long].map(EventTime.fromRaw)
  }

  implicit val arbQuineValue: Arbitrary[QuineValue] = Arbitrary {
    Gen.lzy(
      sizedOneOf(
        small = List(
          Gen.const[QuineValue](QuineValue.True),
          Gen.const[QuineValue](QuineValue.False),
          Gen.const[QuineValue](QuineValue.Null)
        ),
        other = List(
          Gen.resultOf[String, QuineValue](QuineValue.Str.apply),
          Gen.resultOf[Long, QuineValue](QuineValue.Integer.apply),
          Gen.resultOf[Double, QuineValue](QuineValue.Floating.apply),
          Gen.resultOf[Array[Byte], QuineValue](QuineValue.Bytes.apply),
          GenApply.resultOf[Vector[QuineValue], QuineValue](QuineValue.List.apply),
          GenApply.resultOf[Map[String, QuineValue], QuineValue](QuineValue.Map.apply),
          GenApply.resultOf[Instant, QuineValue](QuineValue.DateTime.apply),
          GenApply.resultOf[QuineId, QuineValue](QuineValue.Id.apply)
        )
      )
    )
  }

  implicit val arbNodeCypherValue: Arbitrary[CypherExpr.Node] = Arbitrary {
    GenApply.resultOf[QuineId, Set[Symbol], Map[Symbol, CypherValue], CypherExpr.Node](CypherExpr.Node.apply)
  }

  implicit val arbRelationshipCypherValue: Arbitrary[CypherExpr.Relationship] = Arbitrary {
    GenApply.resultOf[QuineId, Symbol, Map[Symbol, CypherValue], QuineId, CypherExpr.Relationship](
      CypherExpr.Relationship.apply
    )
  }

  implicit val arbCypherFunc: Arbitrary[CypherFunc] = Arbitrary {
    Gen.oneOf[CypherFunc](CypherFunc.builtinFunctions)
  }

  implicit val arbCypherValue: Arbitrary[CypherValue] = Arbitrary {
    Gen.lzy(
      sizedOneOf(
        small = List(
          Gen.const[CypherValue](CypherExpr.True),
          Gen.const[CypherValue](CypherExpr.False),
          Gen.const[CypherValue](CypherExpr.Null)
        ),
        other = List(
          Gen.resultOf[String, CypherValue](CypherExpr.Str.apply),
          Gen.resultOf[Long, CypherValue](CypherExpr.Integer.apply),
          Gen.resultOf[Double, CypherValue](CypherExpr.Floating.apply),
          Gen.resultOf[Array[Byte], CypherValue](CypherExpr.Bytes.apply),
          arbNodeCypherValue.arbitrary,
          arbRelationshipCypherValue.arbitrary,
          GenApply.resultOf[Vector[CypherValue], CypherValue](CypherExpr.List.apply),
          GenApply.resultOf[Map[String, CypherValue], CypherValue](CypherExpr.Map.apply),
          GenApply.resultOf[CypherExpr.Node, Vector[(CypherExpr.Relationship, CypherExpr.Node)], CypherValue](
            CypherExpr.Path.apply
          ),
          Gen.resultOf[JavaLocalDateTime, CypherValue](CypherExpr.LocalDateTime.apply),
          Gen.resultOf[JavaZonedDateTime, CypherValue](CypherExpr.DateTime.apply),
          Gen.resultOf[JavaDuration, CypherValue](CypherExpr.Duration.apply)
        )
      )
    )
  }

  implicit val arbCypherExpr: Arbitrary[CypherExpr] = Arbitrary {
    Gen.lzy(
      sizedOneOf(
        small = List(
          Gen.const[CypherExpr](CypherExpr.FreshNodeId)
        ),
        other = List(
          arbCypherValue.arbitrary.map(x => x),
          Gen.resultOf[Symbol, CypherExpr](CypherExpr.Variable.apply),
          GenApply.resultOf[CypherExpr, Symbol, CypherExpr](CypherExpr.Property.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.DynamicProperty.apply),
          GenApply.resultOf[CypherExpr, Option[CypherExpr], Option[CypherExpr], CypherExpr](CypherExpr.ListSlice.apply),
          GenApply.resultOf[Int, CypherExpr](CypherExpr.Parameter.apply),
          GenApply.resultOf[Vector[CypherExpr], CypherExpr](CypherExpr.ListLiteral.apply),
          GenApply.resultOf[Map[String, CypherExpr], CypherExpr](CypherExpr.MapLiteral.apply),
          GenApply.resultOf[CypherExpr, Seq[(String, CypherExpr)], Boolean, CypherExpr](CypherExpr.MapProjection.apply),
          GenApply.resultOf[Vector[CypherExpr], CypherExpr](CypherExpr.PathExpression.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.RelationshipStart.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.RelationshipEnd.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.UnaryAdd.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.UnarySubtract.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.IsNotNull.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.IsNull.apply),
          GenApply.resultOf[CypherExpr, CypherExpr](CypherExpr.Not.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Equal.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Subtract.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Multiply.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Divide.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Modulo.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Exponentiate.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Add.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.GreaterEqual.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.LessEqual.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Greater.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Less.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.InList.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.StartsWith.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.EndsWith.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Contains.apply),
          GenApply.resultOf[CypherExpr, CypherExpr, CypherExpr](CypherExpr.Regex.apply),
          GenApply.resultOf[Vector[CypherExpr], CypherExpr](CypherExpr.And.apply),
          GenApply.resultOf[Vector[CypherExpr], CypherExpr](CypherExpr.Or.apply),
          GenApply.resultOf[Option[CypherExpr], Vector[(CypherExpr, CypherExpr)], Option[CypherExpr], CypherExpr](
            CypherExpr.Case.apply
          ),
          GenApply.resultOf[CypherFunc, Vector[CypherExpr], CypherExpr](CypherExpr.Function.apply),
          GenApply.resultOf[Symbol, CypherExpr, CypherExpr, CypherExpr, CypherExpr](CypherExpr.ListComprehension.apply),
          GenApply.resultOf[Symbol, CypherExpr, CypherExpr, CypherExpr](CypherExpr.AllInList.apply),
          GenApply.resultOf[Symbol, CypherExpr, CypherExpr, CypherExpr](CypherExpr.AnyInList.apply),
          GenApply.resultOf[Symbol, CypherExpr, CypherExpr, CypherExpr](CypherExpr.SingleInList.apply),
          GenApply.resultOf[Symbol, CypherExpr, Symbol, CypherExpr, CypherExpr, CypherExpr](CypherExpr.ReduceList.apply)
        )
      )
    )
  }

  implicit val arbPropertyValue: Arbitrary[PropertyValue] = Arbitrary {
    arbitrary[QuineValue].map(PropertyValue.apply)
  }

  implicit val arbHalfEdge: Arbitrary[HalfEdge] = Arbitrary {
    Gen.resultOf[Symbol, EdgeDirection, QuineId, HalfEdge](HalfEdge.apply)
  }

  implicit val arbNodeChangeEvent: Arbitrary[NodeChangeEvent] = Arbitrary {
    import NodeChangeEvent._
    Gen.oneOf(
      Gen.resultOf[HalfEdge, NodeChangeEvent](EdgeAdded.apply),
      Gen.resultOf[HalfEdge, NodeChangeEvent](EdgeRemoved.apply),
      Gen.resultOf[Symbol, PropertyValue, NodeChangeEvent](PropertySet.apply),
      Gen.resultOf[Symbol, PropertyValue, NodeChangeEvent](PropertyRemoved.apply),
      Gen.resultOf[QuineId, NodeChangeEvent](MergedIntoOther.apply),
      Gen.resultOf[QuineId, NodeChangeEvent](MergedHere.apply)
    )
  }

  implicit val arbNodeChangeEventWithTime: Arbitrary[NodeChangeEvent.WithTime] = Arbitrary {
    Gen.resultOf[NodeChangeEvent, EventTime, NodeChangeEvent.WithTime](NodeChangeEvent.WithTime.apply)
  }

  implicit val arbPropCompF: Arbitrary[PropertyComparisonFunc] = Arbitrary {
    Gen.oneOf[PropertyComparisonFunc](
      PropertyComparisonFunctions.Identicality,
      PropertyComparisonFunctions.NonIdenticality,
      PropertyComparisonFunctions.NoValue,
      PropertyComparisonFunctions.Wildcard,
      PropertyComparisonFunctions.RegexMatch("[a-z].*"),
      PropertyComparisonFunctions.ListContains(Set[QuineValue](QuineValue.Str("KNOWS")))
    )
  }

  implicit val arbNodeCompF: Arbitrary[NodeLocalComparisonFunc] = Arbitrary {
    Gen.oneOf[NodeLocalComparisonFunc](
      NodeLocalComparisonFunctions.Identicality,
      NodeLocalComparisonFunctions.EqualSubset,
      NodeLocalComparisonFunctions.Wildcard
    )
  }

  implicit val arbGenericEdge: Arbitrary[GenericEdge] = Arbitrary {
    Gen.resultOf[Symbol, EdgeDirection, GenericEdge](GenericEdge.apply)
  }

  implicit val arbDependencyDir: Arbitrary[DependencyDirection] = Arbitrary {
    Gen.oneOf(DependsUpon, IsDependedUpon, Incidental)
  }

  implicit val arbStandingQueryId: Arbitrary[StandingQueryId] = Arbitrary {
    arbitrary[UUID].map(StandingQueryId(_))
  }
  implicit val arbStandingQueryPartId: Arbitrary[StandingQueryPartId] = Arbitrary {
    arbitrary[UUID].map(StandingQueryPartId(_))
  }
  implicit val arbResultId: Arbitrary[ResultId] = Arbitrary {
    arbitrary[UUID].map(ResultId(_))
  }

  implicit val arbDomainNodeEquiv: Arbitrary[DomainNodeEquiv] = Arbitrary {
    Gen.resultOf[
      Option[String],
      Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])],
      Set[CircularEdge],
      DomainNodeEquiv
    ](DomainNodeEquiv.apply)
  }

  implicit val arbConstraintsCreate: Arbitrary[EdgeMatchConstraints[Create]] =
    Arbitrary(MandatoryConstraint)

  implicit val arbDgbCreate: Arbitrary[DomainGraphBranch[Create]] = Arbitrary {
    Gen.lzy(
      GenApply.resultOf(
        SingleBranch[Create](
          _: DomainNodeEquiv,
          _: Option[QuineId],
          _: List[DomainEdge[Create]],
          _: NodeLocalComparisonFunc
        )
      )
    )
  }

  implicit val arbDomainEdgeCreate: Arbitrary[DomainEdge[Create]] = Arbitrary {
    GenApply.resultOf[
      GenericEdge,
      DependencyDirection,
      DomainGraphBranch[Create],
      Boolean,
      EdgeMatchConstraints[Create],
      DomainEdge[Create]
    ](DomainEdge.apply)
  }

  implicit val arbConstraintsFetch: Arbitrary[EdgeMatchConstraints[model.Test]] = Arbitrary {
    Gen.oneOf(
      Gen.const(MandatoryConstraint),
      GenApply.resultOf[Int, Option[Int], EdgeMatchConstraints[model.Test]](FetchConstraint.apply)
    )
  }

  implicit val arbMuVariableName: Arbitrary[MuVariableName] = Arbitrary {
    Gen.resultOf[String, MuVariableName](MuVariableName.apply)
  }

  implicit val arbDgbTest: Arbitrary[DomainGraphBranch[model.Test]] = Arbitrary {
    Gen.lzy(
      sizedOneOf(
        small = List(Gen.resultOf(MuVar[model.Test](_: MuVariableName))),
        other = List(
          GenApply.resultOf(
            SingleBranch[model.Test](
              _: DomainNodeEquiv,
              _: Option[QuineId],
              _: List[DomainEdge[model.Test]],
              _: NodeLocalComparisonFunc
            )
          ),
          GenApply.resultOf(And[model.Test](_: List[DomainGraphBranch[model.Test]])),
          GenApply.resultOf(Or[model.Test](_: List[DomainGraphBranch[model.Test]])),
          GenApply.resultOf(Not[model.Test](_: DomainGraphBranch[model.Test])),
          GenApply.resultOf(Mu[model.Test](_: MuVariableName, _: DomainGraphBranch[model.Test]))
        )
      )
    )
  }

  implicit val arbDomainEdgeTest: Arbitrary[DomainEdge[model.Test]] = Arbitrary {
    GenApply.resultOf[
      GenericEdge,
      DependencyDirection,
      DomainGraphBranch[model.Test],
      Boolean,
      EdgeMatchConstraints[model.Test],
      DomainEdge[model.Test]
    ](DomainEdge.apply)
  }

  implicit val arbEdgeCollection: Arbitrary[Iterator[HalfEdge]] = Arbitrary(
    Gen.resultOf[Seq[HalfEdge], Iterator[HalfEdge]](_.iterator)
  )
  /*
    arbitrary[List[HalfEdge]].map { (hes: List[HalfEdge]) =>
      val col = new ReverseOrderedEdgeCollection
      hes.foreach(col +=)
      col
    }
  }
   */

  implicit val arbProperties: Arbitrary[Properties] = cachedImplicit

  implicit val arbSubscription: Arbitrary[SubscribersToThisNodeUtil.Subscription] = Arbitrary {
    Gen.resultOf[
      Set[Notifiable],
      LastNotification,
      Set[StandingQueryId],
      SubscribersToThisNodeUtil.Subscription
    ](SubscribersToThisNodeUtil.Subscription.apply)
  }

  type IndexSubscribers = MutableMap[
    (DomainGraphBranch[Test], AssumedDomainEdge),
    SubscribersToThisNodeUtil.Subscription
  ]
  implicit val arbIndexSubscribers: Arbitrary[IndexSubscribers] = cachedImplicit

  type DomainNodeIndex = MutableMap[
    QuineId,
    MutableMap[
      (DomainGraphBranch[Test], AssumedDomainEdge),
      Option[IsDirected]
    ]
  ]
  implicit val arbDomainNodeIndex: Arbitrary[DomainNodeIndex] = cachedImplicit

  implicit val arbValueConstraint: Arbitrary[CypherStandingQuery.LocalProperty.ValueConstraint] = Arbitrary {
    import CypherStandingQuery.LocalProperty._
    Gen.oneOf(
      Gen.resultOf[CypherValue, ValueConstraint](Equal.apply),
      Gen.resultOf[CypherValue, ValueConstraint](NotEqual.apply),
      Gen.const[ValueConstraint](Any),
      Gen.const[ValueConstraint](None),
      Gen.const[ValueConstraint](Regex("[a-z].*")),
      Gen.resultOf[Set[CypherValue], ValueConstraint](ListContains.apply)
    )
  }

  implicit val arbCypherStandingQuery: Arbitrary[CypherStandingQuery] = Arbitrary {
    Gen.lzy(
      sizedOneOf(
        small = List(
          Gen.resultOf[Unit, CypherStandingQuery](_ => CypherStandingQuery.UnitSq())
        ),
        other = List(
          GenApply.resultOf[ArraySeq[CypherStandingQuery], Boolean, CypherStandingQuery](
            CypherStandingQuery.Cross(_, _)
          ),
          GenApply
            .resultOf[Symbol, CypherStandingQuery.LocalProperty.ValueConstraint, Option[Symbol], CypherStandingQuery](
              CypherStandingQuery.LocalProperty(_, _, _)
            ),
          GenApply.resultOf[Symbol, Boolean, CypherStandingQuery](CypherStandingQuery.LocalId(_, _)),
          GenApply.resultOf[Option[Symbol], Option[EdgeDirection], CypherStandingQuery, CypherStandingQuery](
            CypherStandingQuery.SubscribeAcrossEdge(_, _, _)
          ),
          GenApply.resultOf[HalfEdge, StandingQueryPartId, CypherStandingQuery](
            CypherStandingQuery.EdgeSubscriptionReciprocal(_, _)
          ),
          GenApply.resultOf[Option[CypherExpr], CypherStandingQuery, Boolean, List[
            (Symbol, CypherExpr)
          ], CypherStandingQuery](CypherStandingQuery.FilterMap(_, _, _, _))
        )
      )
    )
  }

  implicit val arbNodePatternId: Arbitrary[GraphQueryPattern.NodePatternId] = Arbitrary {
    Gen.resultOf(GraphQueryPattern.NodePatternId(_))
  }

  implicit val arbPropertyValuePattern: Arbitrary[GraphQueryPattern.PropertyValuePattern] = Arbitrary {
    import GraphQueryPattern.PropertyValuePattern
    import GraphQueryPattern.PropertyValuePattern._
    Gen.oneOf(
      Gen.resultOf[QuineValue, PropertyValuePattern](Value.apply),
      Gen.resultOf[QuineValue, PropertyValuePattern](AnyValueExcept.apply),
      Gen.const[PropertyValuePattern](AnyValue),
      Gen.const[PropertyValuePattern](NoValue),
      Gen.const[PropertyValuePattern](RegexMatch(Pattern.compile("[a-z].*")))
    )
  }

  implicit val arbNodePattern: Arbitrary[GraphQueryPattern.NodePattern] = Arbitrary {
    Gen.resultOf[GraphQueryPattern.NodePatternId, Set[Symbol], Option[QuineId], Map[
      Symbol,
      GraphQueryPattern.PropertyValuePattern
    ], GraphQueryPattern.NodePattern](GraphQueryPattern.NodePattern.apply)
  }

  implicit val arbEdgePattern: Arbitrary[GraphQueryPattern.EdgePattern] = Arbitrary {
    Gen.resultOf[
      GraphQueryPattern.NodePatternId,
      GraphQueryPattern.NodePatternId,
      Boolean,
      Symbol,
      GraphQueryPattern.EdgePattern
    ](GraphQueryPattern.EdgePattern.apply)
  }

  implicit val arbReturnColumn: Arbitrary[GraphQueryPattern.ReturnColumn] = Arbitrary {
    import GraphQueryPattern.ReturnColumn
    Gen.oneOf(
      Gen.resultOf[GraphQueryPattern.NodePatternId, Boolean, Symbol, ReturnColumn](ReturnColumn.Id.apply),
      Gen.resultOf[GraphQueryPattern.NodePatternId, Symbol, Symbol, ReturnColumn](ReturnColumn.Property.apply)
    )
  }

  val arbDistinctGraphPattern: Arbitrary[GraphQueryPattern] = Arbitrary {
    Gen.resultOf[
      Seq[GraphQueryPattern.NodePattern],
      Seq[GraphQueryPattern.EdgePattern],
      GraphQueryPattern.NodePatternId,
      Seq[GraphQueryPattern.ReturnColumn],
      Option[cypher.Expr],
      Seq[(Symbol, cypher.Expr)],
      Boolean,
      GraphQueryPattern
    ](GraphQueryPattern.apply)(
      Arbitrary(arbitrary[Seq[GraphQueryPattern.NodePattern]].filter(_.nonEmpty)),
      implicitly,
      implicitly,
      implicitly,
      implicitly,
      implicitly,
      Arbitrary(Gen.const(true))
    )
  }
  val arbNonDistinctGraphPattern: Arbitrary[GraphQueryPattern] = Arbitrary {
    arbDistinctGraphPattern.arbitrary.map(_.copy(distinct = false))
  }

  implicit val arbDgbOrigin: Arbitrary[PatternOrigin.DgbOrigin] = Arbitrary {
    implicit val distinctGraphPattern = arbDistinctGraphPattern
    Gen.oneOf(
      Gen.const[PatternOrigin.DgbOrigin](PatternOrigin.DirectDgb),
      Gen.resultOf[GraphQueryPattern, Option[String], PatternOrigin.DgbOrigin](PatternOrigin.GraphPattern.apply)
    )
  }

  implicit val arbSqv4Origin: Arbitrary[PatternOrigin.SqV4Origin] = Arbitrary {
    implicit val distinctGraphPattern = arbNonDistinctGraphPattern
    Gen.oneOf(
      Gen.const[PatternOrigin.SqV4Origin](PatternOrigin.DirectSqV4),
      Gen.resultOf[GraphQueryPattern, Option[String], PatternOrigin.SqV4Origin](PatternOrigin.GraphPattern.apply)
    )
  }

  implicit val arbStandingQueryPattern: Arbitrary[StandingQueryPattern] = Arbitrary {
    Gen.oneOf[StandingQueryPattern](
      Gen.resultOf[
        DomainGraphBranch[Create],
        Boolean,
        Symbol,
        Boolean,
        PatternOrigin.DgbOrigin,
        StandingQueryPattern
      ](StandingQueryPattern.Branch.apply),
      Gen.resultOf[
        CypherStandingQuery,
        Boolean,
        PatternOrigin.SqV4Origin,
        StandingQueryPattern
      ](StandingQueryPattern.SqV4.apply)
    )
  }

  implicit val arbStandingQuery: Arbitrary[StandingQuery] = Arbitrary {
    Gen.resultOf[String, StandingQueryId, StandingQueryPattern, Int, Int, StandingQuery](StandingQuery.apply)
  }

  implicit val arbQueryContext: Arbitrary[QueryContext] = Arbitrary {
    Gen.resultOf[Map[Symbol, CypherValue], QueryContext](QueryContext.apply)
  }

  implicit val arbStandingQueryState: Arbitrary[CypherStandingQueryState] = Arbitrary {
    import com.thatdot.quine.graph.cypher._
    Gen.oneOf(
      Gen.resultOf[StandingQueryPartId, Option[ResultId], CypherStandingQueryState](UnitState.apply),
      Gen.resultOf[StandingQueryPartId, Int, ArraySeq[MutableMap[ResultId, QueryContext]], MutableMap[ResultId, List[
        ResultId
      ]], CypherStandingQueryState](CrossState.apply),
      Gen.resultOf[StandingQueryPartId, Option[ResultId], CypherStandingQueryState](LocalPropertyState.apply),
      Gen.resultOf[StandingQueryPartId, Option[ResultId], CypherStandingQueryState](LocalIdState.apply),
      Gen.resultOf[StandingQueryPartId, MutableMap[
        HalfEdge,
        (StandingQueryPartId, MutableMap[ResultId, (ResultId, QueryContext)])
      ], CypherStandingQueryState](SubscribeAcrossEdgeState.apply),
      Gen.resultOf[StandingQueryPartId, HalfEdge, Boolean, MutableMap[
        ResultId,
        (ResultId, QueryContext)
      ], StandingQueryPartId, CypherStandingQueryState](EdgeSubscriptionReciprocalState.apply),
      Gen.resultOf[StandingQueryPartId, MutableMap[ResultId, (ResultId, QueryContext)], CypherStandingQueryState](
        FilterMapState.apply
      )
    )
  }

  implicit val arbCypherSubscriber: Arbitrary[CypherSubscriber] = Arbitrary {
    Gen.oneOf[CypherSubscriber](
      Gen.resultOf[QuineId, StandingQueryId, StandingQueryPartId, CypherSubscriber](
        CypherSubscriber.QuerySubscriber.apply
      ),
      Gen.resultOf[StandingQueryId, CypherSubscriber](CypherSubscriber.GlobalSubscriber.apply)
    )
  }

  implicit val arbStandingQuerySubscribers: Arbitrary[StandingQuerySubscribers] = Arbitrary {
    Gen.resultOf[StandingQueryPartId, StandingQueryId, MutableSet[
      CypherSubscriber
    ], StandingQuerySubscribers](
      StandingQuerySubscribers
        .apply(_: StandingQueryPartId, _: StandingQueryId, _: MutableSet[CypherSubscriber])
    )
  }
  implicit val arbNodeSnapshot: Arbitrary[NodeSnapshot] = Arbitrary {
    Gen.resultOf[
      Map[Symbol, PropertyValue], // properties
      Iterable[HalfEdge], // edges
      Option[QuineId], // forwardTo
      Set[QuineId], // mergedIntoHere
      IndexSubscribers, // subscribersToThisNode
      DomainNodeIndex, // domainNodeIndex
      NodeSnapshot
    ](NodeSnapshot.apply)
  }

}

class SerializationTests extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  // This doubles the default size and minimum successful tests
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 200)

  import PersistenceCodecs._

  "Binary serialization" should "roundtrip QuineValue" in {
    forAll { (v: QuineValue) =>
      val bytes = QuineValue.writeMsgPack(v)
      QuineValue.readMsgPack(bytes) == v && QuineValue.readMsgPackType(bytes) == v.quineType
    }
  }

  it should "roundtrip NodeChangeEvent.WithTime" in {
    forAll { (event: NodeChangeEvent.WithTime) =>
      eventWithTimeFormat.read(eventWithTimeFormat.write(event)).get == event
    }
  }

  it should "roundtrip NodeChangeEvent" in {
    forAll { (event: NodeChangeEvent) =>
      eventFormat.read(eventFormat.write(event)).get == event
    }
  }

  it should "roundtrip cypher.Expr" in {
    forAll { (e: CypherExpr) =>
      cypherExprFormat.read(cypherExprFormat.write(e)).get == e
    }
  }

  it should "roundtrip DomainGraphBranch" in {
    forAll { (dgb: DomainGraphBranch[model.Test]) =>
      dgbFormat.read(dgbFormat.write(dgb)).get == dgb
    }
  }

  it should "roundtrip NodeSnapshot" in {
    forAll { (snapshot: NodeSnapshot) =>
      nodeSnapshotFormat.read(nodeSnapshotFormat.write(snapshot)).get == snapshot
    }
  }

  it should "roundtrip StandingQuery" in {
    forAll { (sq: StandingQuery) =>
      standingQueryFormat.read(standingQueryFormat.write(sq)).get == sq
    }
  }

  it should "roundtrip StandingQueryState" in {
    forAll { (subs: StandingQuerySubscribers, sq: CypherStandingQueryState) =>
      standingQueryStateFormat.read(standingQueryStateFormat.write(subs -> sq)).get == subs -> sq
    }
  }
}

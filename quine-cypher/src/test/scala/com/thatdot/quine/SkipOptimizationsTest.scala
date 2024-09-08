package com.thatdot.quine;

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.extended.ask
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

import com.thatdot.quine.compiler.cypher.CypherHarness
import com.thatdot.quine.graph.SkipOptimizerKey
import com.thatdot.quine.graph.cypher.Query.Return
import com.thatdot.quine.graph.cypher.SkipOptimizingActor.{ResumeQuery, SkipOptimizationError}
import com.thatdot.quine.graph.cypher.{Columns, Expr, Func, Location, Parameters, Query, QueryContext, Value}
import com.thatdot.quine.model.Milliseconds

class SkipOptimizationsTest
    extends CypherHarness("skip-optimizations")
    with EitherValues
    with OptionValues
    with Matchers {
  val XSym: Symbol = Symbol("x")
  // UNWIND range(0, 999) AS x
  val queryFamily: Query.Unwind[Location.Anywhere] = Query.Unwind(
    Expr.Function(
      Func.Range,
      Vector(
        Expr.Integer(0),
        Expr.Integer(999),
      ),
    ),
    XSym,
    Query.Unit(Columns.Omitted),
    Columns.Specified(Vector(XSym)),
  )
  val atTime: Some[Milliseconds] = Some(Milliseconds(1586631600L))

  def freshSkipActor(): ActorRef = {
    graph.cypherOps.skipOptimizerCache.refresh(SkipOptimizerKey(queryFamily, cypherHarnessNamespace, atTime))
    graph.cypherOps.skipOptimizerCache.get(SkipOptimizerKey(queryFamily, cypherHarnessNamespace, atTime))
  }
  def actorIsPresentInCache: Boolean =
    graph.cypherOps.skipOptimizerCache.getIfPresent(
      SkipOptimizerKey(queryFamily, cypherHarnessNamespace, atTime),
    ) != null

  /** Executes a query in [[queryFamily]] according to the provided projection rules
    */
  def queryFamilyViaActor(
    actor: ActorRef,
    dropRule: Option[Query.Skip.Drop],
    takeRule: Option[Query.Limit.Take],
    orderBy: Option[Query.Sort.SortBy] = None,
    distinctBy: Option[Query.Distinct.DistinctBy] = None,
    restartIfAppropriate: Boolean = false,
  ): Future[Seq[Value]] =
    (actor ? (
      ResumeQuery(
        Return(
          queryFamily,
          orderBy,
          distinctBy,
          dropRule,
          takeRule,
          columns = Columns.Specified(Vector(XSym)),
        ),
        QueryContext.empty,
        Parameters.empty,
        restartIfAppropriate,
        _,
      ),
    )).mapTo[Either[SkipOptimizationError, Source[QueryContext, NotUsed]]]
      .flatMap { e =>
        val resultStream = e.value
        resultStream.runWith(Sink.seq)
      }
      .map(results => results.map(_.get(XSym).value))

  def queryActorExpectingError(
    actor: ActorRef,
    dropRule: Option[Query.Skip.Drop],
    takeRule: Option[Query.Limit.Take],
    orderBy: Option[Query.Sort.SortBy] = None,
    distinctBy: Option[Query.Distinct.DistinctBy] = None,
    innerQuery: Query[Location.Anywhere] = queryFamily,
  ): Future[SkipOptimizationError] =
    (actor ? (
      ResumeQuery(
        Return(
          innerQuery,
          orderBy,
          distinctBy,
          dropRule,
          takeRule,
          columns = Columns.Specified(Vector(XSym)),
        ),
        QueryContext.empty,
        Parameters.empty,
        restartIfAppropriate = false,
        _,
      ),
    )).mapTo[Either[SkipOptimizationError, Source[QueryContext, NotUsed]]]
      .map { response =>
        assert(actorIsPresentInCache, "rejected ResumeQuery requests should not terminate the actor")
        response.left.value
      }

  describe("basic correctness of SKIP optimizations") {
    it("should return a complete, correct, collection of results") {
      val skipActor: ActorRef = freshSkipActor()
      queryFamilyViaActor(skipActor, dropRule = Some(Expr.Integer(0)), takeRule = None)
        .map { results =>
          val expectedResultValues = (0 until 1000).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(!actorIsPresentInCache, "completing the family's stream should remove the actor from the cache"),
        )
    }
    it("should return a partial collection of results given a LIMIT") {
      val skipActor: ActorRef = freshSkipActor()
      queryFamilyViaActor(skipActor, dropRule = None, takeRule = Some(Expr.Integer(200)))
        .map { results =>
          val expectedResultValues = (0 until 200).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
    }
    it("should return sequential pages") {
      val skipActor: ActorRef = freshSkipActor()
      // page 1: 0-99
      queryFamilyViaActor(skipActor, dropRule = None, takeRule = Some(Expr.Integer(100)))
        .map { results =>
          val expectedResultValues = (0 until 100).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
        // page 2: 100-199
        .flatMap(_ =>
          queryFamilyViaActor(skipActor, dropRule = Some(Expr.Integer(100)), takeRule = Some(Expr.Integer(100))),
        )
        .map { results =>
          val expectedResultValues = (100 until 200).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
    }
    it("should return nonsequential increasing pages") {
      val skipActor: ActorRef = freshSkipActor()
      // page 1: 0-99
      queryFamilyViaActor(skipActor, dropRule = None, takeRule = Some(Expr.Integer(100)))
        .map { results =>
          val expectedResultValues = (0 until 100).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
        // page 2: 200-499
        .flatMap(_ =>
          queryFamilyViaActor(skipActor, dropRule = Some(Expr.Integer(200)), takeRule = Some(Expr.Integer(300))),
        )
        .map { results =>
          val expectedResultValues = (200 until 500).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
    }
    it("should return a final unbounded page") {
      val skipActor: ActorRef = freshSkipActor()
      // page 1: 50-199
      queryFamilyViaActor(skipActor, dropRule = Some(Expr.Integer(50)), takeRule = Some(Expr.Integer(150)))
        .map { results =>
          val expectedResultValues = (50 until 200).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
        // page 2: 210-??? (ie, 999)
        .flatMap(_ => queryFamilyViaActor(skipActor, dropRule = Some(Expr.Integer(210)), takeRule = None))
        .map { results =>
          val expectedResultValues = (210 until 1000).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            !actorIsPresentInCache,
            "completing the family's stream should remove the actor from the cache",
          ),
        )
    }
    it("should allow querying of out-of-order pages when restartIfAppropriate = true") {
      val skipActor: ActorRef = freshSkipActor()
      // page 1: 50-199
      queryFamilyViaActor(skipActor, dropRule = Some(Expr.Integer(50)), takeRule = Some(Expr.Integer(150)))
        .map { results =>
          val expectedResultValues = (50 until 200).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
        // page 2: 50-54
        .flatMap(_ =>
          queryFamilyViaActor(
            skipActor,
            dropRule = Some(Expr.Integer(50)),
            takeRule = Some(Expr.Integer(5)),
            restartIfAppropriate = true,
          ),
        )
        .map { results =>
          val expectedResultValues = (50 until 55).map(i => Expr.Integer(i.toLong))
          results should contain theSameElementsInOrderAs expectedResultValues
        }
        .map(_ =>
          assert(
            actorIsPresentInCache,
            "completing only some streams in a family should not remove the actor from the cache",
          ),
        )
    }
  }

  describe("basic error handling / query analysis") {
    it("should reject queries specifying no SKIP or LIMIT") {
      val skipActor: ActorRef = freshSkipActor()
      queryActorExpectingError(skipActor, dropRule = None, takeRule = None).map(err =>
        assert(err.isInstanceOf[SkipOptimizationError.UnsupportedProjection]),
      )
    }
    it("should reject queries specifying a DISTINCT") {
      val skipActor: ActorRef = freshSkipActor()
      queryActorExpectingError(
        skipActor,
        dropRule = Some(Expr.Integer(100)),
        takeRule = None,
        distinctBy = Some(Seq(Expr.Variable(XSym))),
      ).map(err => assert(err.isInstanceOf[SkipOptimizationError.UnsupportedProjection]))
    }
    it("should reject queries specifying an ORDER BY") {
      val skipActor: ActorRef = freshSkipActor()
      queryActorExpectingError(
        skipActor,
        dropRule = Some(Expr.Integer(100)),
        takeRule = None,
        orderBy = Some(Seq(Expr.Variable(XSym) -> true)),
      ).map(err => assert(err.isInstanceOf[SkipOptimizationError.UnsupportedProjection]))
    }
    it("should reject queries specifying an ill-typed SKIP") {
      val skipActor: ActorRef = freshSkipActor()
      queryActorExpectingError(
        skipActor,
        dropRule = Some(Expr.Null),
        takeRule = None,
      ).map(err => assert(err.isInstanceOf[SkipOptimizationError.InvalidSkipLimit]))
    }
    it("should reject queries specifying an ill-typed LIMIT") {
      val skipActor: ActorRef = freshSkipActor()
      queryActorExpectingError(
        skipActor,
        dropRule = None,
        takeRule = Some(Expr.Str("the number 5")),
      ).map(err => assert(err.isInstanceOf[SkipOptimizationError.InvalidSkipLimit]))
    }
    it("should reject queries for the wrong family") {
      val skipActor: ActorRef = freshSkipActor()
      queryActorExpectingError(
        skipActor,
        dropRule = Some(Expr.Integer(100)),
        takeRule = None,
        innerQuery = queryFamily.copy(as =
          Symbol("y"),
        ), // "UNWIND range(0, 999) AS y" -- which does not match "UNWIND range(0, 999) AS x
      ).map(err => assert(err == SkipOptimizationError.QueryMismatch))
    }
  }
}

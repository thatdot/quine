package com.thatdot.quine.graph.cypher

import cats.data.{NonEmptyList, StateT}
import cats.implicits._
import com.quine.language.ast.{Expression, Operator, Predicate}
import com.quine.language.{ast => QueryAST}

import com.thatdot.quine.graph.GraphQueryPattern.{NodePattern, NodePatternId, ReturnColumn}
import com.thatdot.quine.graph.InvalidQueryPattern.HasACycle
import com.thatdot.quine.graph.{GraphQueryPattern, InvalidQueryPattern}

sealed trait Projection

object Projection {
  case class Property(of: Symbol, name: Symbol, as: Symbol) extends Projection
  case class Calculation(expr: Expr, as: Symbol) extends Projection
  case class Serial(subs: NonEmptyList[Projection]) extends Projection
  case class Parallel(subs: NonEmptyList[Projection]) extends Projection
}

sealed trait BinOp

object BinOp {
  case object Merge extends BinOp
  case object Append extends BinOp
}

sealed trait QuinePattern

object QuinePattern {
  case object QuineUnit extends QuinePattern
  case class Node(binding: Symbol) extends QuinePattern
  case class Edge(edgeLabel: Symbol, remotePattern: QuinePattern) extends QuinePattern

  case class Fold(init: QuinePattern, over: List[QuinePattern], f: BinOp, projection: Option[Projection])
      extends QuinePattern
}

object Compiler {
  class StateHelper[S, E] {
    type Program[A] = StateT[Either[E, *], S, A]

    def pure[A](a: A): Program[A] = StateT.pure(a)
    def error(e: E): Program[Unit] = StateT.liftF(Left(e))
    def get: Program[S] = StateT.get
    def mod(update: S => S): Program[Unit] = StateT.modify(update)
    def updateAndGet(update: S => S): Program[S] = mod(update) *> get
    def inspect[A](view: S => A): Program[A] = StateT.inspect(view)
    def failIf(test: => Boolean, e: E): Program[Unit] = if (test) {
      error(e)
    } else {
      pure(())
    }
  }

  case class CompilerState(
    gqp: GraphQueryPattern,
    nodesById: Map[NodePatternId, NodePattern],
    visited: Set[NodePatternId]
  )

  val helper = new StateHelper[CompilerState, InvalidQueryPattern]

  import helper._

  def compileNodePattern(nodePattern: NodePattern): Program[QuinePattern] = for {
    visited <- inspect(_.visited)
    _ <- mod(st => st.copy(visited = st.visited + nodePattern.id))
    edges <- inspect(_.gqp.edges)
    unhandledEdges = edges.toList.filterNot(edge => visited.contains(edge.to) || visited.contains(edge.from))
    interestingEdges = unhandledEdges.filter(edge => edge.to == nodePattern.id || edge.from == nodePattern.id)
    edgeQueries <- interestingEdges.traverse { edge =>
      val interestingId = if (nodePattern.id == edge.from) edge.to else edge.from
      val subPattern = compileFromStartingPoint(interestingId)
      subPattern.map(pattern => QuinePattern.Edge(edge.label, pattern))
    }
    extractions <- inspect(_.gqp.toExtract)
    interestingExtractions = extractions.toList.filter {
      case ReturnColumn.Id(node, _, _) => node == nodePattern.id
      case ReturnColumn.Property(node, _, _) => node == nodePattern.id
      case ReturnColumn.AllProperties(_, _) => ???
    }.toNel
    properties = interestingExtractions.flatMap { extracts =>
      val maybeProjections = extracts.toList
        .map {
          case ReturnColumn.Id(_, _, _) => None
          case ReturnColumn.Property(node, propertyKey, aliasedAs) =>
            Some(Projection.Property(Symbol(node.id.toString), propertyKey, aliasedAs))
          case ReturnColumn.AllProperties(_, _) => ???
        }
        .filter(_.isDefined)
      if (maybeProjections.isEmpty) {
        None
      } else {
        Some(Projection.Parallel(NonEmptyList.fromListUnsafe(maybeProjections.sequence.get)))
      }
    }
    returns <- inspect(_.gqp.toReturn)
  } yield {
    val nodePat = QuinePattern.Node(Symbol(nodePattern.id.id.toString))
    val withProperties = properties match {
      case Some(p) =>
        QuinePattern.Fold(
          init = QuinePattern.QuineUnit,
          over = List(nodePat),
          f = BinOp.Append,
          projection = Some(p)
        )
      case None => nodePat
    }
    val withEdges = if (edgeQueries.isEmpty) {
      withProperties
    } else {
      QuinePattern.Fold(
        init = QuinePattern.QuineUnit,
        over = withProperties :: edgeQueries,
        f = BinOp.Merge,
        projection = None
      )
    }
    if (returns.isEmpty) {
      withEdges
    } else {
      QuinePattern.Fold(
        init = QuinePattern.QuineUnit,
        over = withEdges :: Nil,
        f = BinOp.Merge,
        projection = Some(
          Projection.Parallel(NonEmptyList.fromListUnsafe(returns.map(p => Projection.Calculation(p._2, p._1)).toList))
        )
      )
    }
  }

  def compileFromStartingPoint(pid: NodePatternId): Program[QuinePattern] = for {
    alreadySeen <- inspect(_.visited.contains(pid))
    _ <- failIf(alreadySeen, HasACycle)
    node <- inspect(_.nodesById(pid))
    qp <- compileNodePattern(node)
  } yield qp

  def compileFromQueryPattern(gqp: GraphQueryPattern): QuinePattern = {
    val nodesById: Map[NodePatternId, NodePattern] = gqp.nodes.map(pattern => pattern.id -> pattern).toList.toMap
    println(s"Edges ${gqp.edges}")
    println(s"ToExtract ${gqp.toExtract}")
    println(s"Filter ${gqp.filterCond}")
    println(s"Return ${gqp.toReturn}")
    compileFromStartingPoint(gqp.startingPoint).runA(CompilerState(gqp, nodesById, Set.empty[NodePatternId])) match {
      case Left(err) => throw err
      case Right(quinePattern) => quinePattern
    }
  }

  def compileFromPredicate(predicate: QueryAST.Predicate): QuinePattern = {
    def findNodes(p: Predicate): List[Symbol] = p match {
      case Predicate.And(lhs, rhs) => findNodes(lhs) ++ findNodes(rhs)
      case Predicate.Or(_, _) => Nil
      case Predicate.ExistsNode(binding, _) => binding :: Nil
      case Predicate.ExistsPath(_, _, _) => Nil
      case Predicate.ExistsEdge(_, _, _, _) => Nil
      case Predicate.Satisfies(_) => Nil
      case Predicate.True => Nil
      case Predicate.False => Nil
    }

    val nodes = findNodes(predicate)

    def findEdges(p: Predicate): (Map[Symbol, Symbol], Map[Symbol, Symbol]) = p match {
      case Predicate.And(lhs, rhs) =>
        val (ls, ld) = findEdges(lhs)
        val (rs, rd) = findEdges(rhs)
        (ls ++ rs, ld ++ rd)
      case Predicate.Or(_, _) => (Map(), Map())
      case Predicate.ExistsNode(_, _) => (Map(), Map())
      case Predicate.ExistsPath(_, _, _) => (Map(), Map())
      case Predicate.ExistsEdge(edge, _, src, dest) => (Map(edge -> src), Map(edge -> dest))
      case Predicate.Satisfies(_) => (Map(), Map())
      case Predicate.True => (Map(), Map())
      case Predicate.False => (Map(), Map())
    }

    val edges = findEdges(predicate)

    println(edges)

    val startingPoint = nodes.find(node => !edges._2.values.toSet.contains(node)).get

    def buildExecutionPlan(from: Symbol): QuinePattern = {
      val interestingEdges = edges._1.toList.filter(p => p._2 == from).map(p => p._1 -> edges._2(p._1))
      val nodePat = QuinePattern.Node(from)
      val edgePats = interestingEdges.map(e => QuinePattern.Edge(Symbol("edge"), buildExecutionPlan(e._2)))
      QuinePattern.Fold(
        init = QuinePattern.QuineUnit,
        over = nodePat :: edgePats,
        f = BinOp.Merge,
        projection = None
      )
    }

    buildExecutionPlan(startingPoint)
  }

  def expressionFromDesc(exp: QueryAST.Expression): Expr = exp match {
    case Expression.Ident(name) => Expr.Variable(name)
    case Expression.Apply(_, _) => ???
    case Expression.Literal(_) => ???
    case Expression.Parameter(_) => ???
    case Expression.UnaryOp(_, _) => ???
    case Expression.BinOp(op, lhs, rhs) =>
      op match {
        case Operator.Plus => Expr.Add(expressionFromDesc(lhs), expressionFromDesc(rhs))
        case Operator.Dot => Expr.Property(expressionFromDesc(lhs), rhs.asInstanceOf[Expression.Ident].name)
        case Operator.And => ???
        case Operator.Or =>
          println(exp)
          ???
        case Operator.LessThan => ???
        case Operator.Equals => ???
        case Operator.GreaterThan => ???
        case Operator.GreaterThanEqual => ???
        case Operator.Minus => ???
        case Operator.Not => ???
        case Operator.XOr => ???
      }
  }

  def projectionFromDesc(projection: QueryAST.Projection): Projection = projection match {
    case QueryAST.Projection.Parallel(projections) =>
      Projection.Parallel(NonEmptyList.fromListUnsafe(projections.map(projectionFromDesc)))
    case QueryAST.Projection.Calculation(expression, as) => Projection.Calculation(expressionFromDesc(expression), as)
  }

  def compileFromAST(ast: QueryAST.Query): QuinePattern = ast match {
    case QueryAST.Query.Union(lhs, rhs) =>
      QuinePattern.Fold(
        init = QuinePattern.QuineUnit,
        over = List(compileFromAST(lhs), compileFromAST(rhs)),
        f = BinOp.Append,
        projection = None
      )
    case QueryAST.Query.Single(predicate, _, projection) =>
      QuinePattern.Fold(
        init = QuinePattern.QuineUnit,
        over = List(compileFromPredicate(predicate)),
        f = BinOp.Merge,
        projection = projection.map(p => projectionFromDesc(p))
      )
    case QueryAST.Query.Empty => QuinePattern.QuineUnit
  }
}

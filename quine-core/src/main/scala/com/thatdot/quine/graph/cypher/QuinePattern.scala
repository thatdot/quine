package com.thatdot.quine.graph.cypher

import java.time.format.DateTimeFormatter
import java.time.{ZonedDateTime => JavaZonedDateTime}
import java.util.Locale

import scala.collection.immutable.SortedMap

import cats.data.State
import cats.implicits._

import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.ast._
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.model.{EdgeDirection, PropertyValue, QuineId, QuineIdProvider, QuineValue}

class QuinePatternUnimplementedException(msg: String) extends RuntimeException(msg)

object CypherAndQuineHelpers {
  def patternValueToCypherValue(value: Value): cypher.Value =
    value match {
      case Value.Null => Expr.Null
      case Value.True => Expr.True
      case Value.False => Expr.False
      case Value.Integer(n) => Expr.Integer(n)
      case Value.Real(d) => Expr.Floating(d)
      case Value.Text(str) => Expr.Str(str)
      case Value.DateTime(zdt) => Expr.DateTime(zdt)
      case Value.List(values) => Expr.List(values.toVector.map(patternValueToCypherValue))
      case Value.Map(values) => Expr.Map(values.map(p => p._1.name -> patternValueToCypherValue(p._2)))
      case Value.NodeId(qid) => Expr.Bytes(qid)
      case Value.Node(id, labels, props) =>
        Expr.Node(id, labels, props.values.map(p => p._1 -> patternValueToCypherValue(p._2)))
    }

  def cypherValueToPatternValue(value: cypher.Value): Value = value match {
    case value: Expr.PropertyValue =>
      value match {
        case Expr.Str(string) => Value.Text(string)
        case Expr.Integer(long) => Value.Integer(long)
        case Expr.Floating(double) => Value.Real(double)
        case Expr.True => Value.True
        case Expr.False => Value.False
        case Expr.Bytes(b, representsId) => ???
        case Expr.List(list) => Value.List(list.toList.map(cypherValueToPatternValue))
        case Expr.Map(map) => Value.Map(map.map(p => Symbol(p._1) -> cypherValueToPatternValue(p._2)))
        case Expr.LocalDateTime(localDateTime) => ???
        case Expr.Date(date) => ???
        case Expr.Time(time) => ???
        case Expr.LocalTime(localTime) => ???
        case Expr.DateTime(zonedDateTime) => ???
        case Expr.Duration(duration) => ???
      }
    case n: Expr.Number =>
      n match {
        case Expr.Integer(long) => Value.Integer(long)
        case Expr.Floating(double) => Value.Real(double)
        case Expr.Null => Value.Null
      }
    case _: Expr.Bool => ???
    case Expr.Node(id, labels, properties) =>
      Value.Node(id, labels, Value.Map(SortedMap.from(properties.map(p => p._1 -> cypherValueToPatternValue(p._2)))))
    case Expr.Relationship(start, name, properties, end) => ???
    case Expr.Path(head, tails) => ???
  }

  def patternValueToPropertyValue(value: Value): Option[PropertyValue] =
    value match {
      case Value.Null => None
      case Value.True => ???
      case Value.False => ???
      case Value.Integer(n) => Some(PropertyValue.apply(n))
      case Value.Real(d) => ???
      case Value.Text(str) => Some(PropertyValue.apply(str))
      case Value.DateTime(zdt) => Some(PropertyValue(QuineValue.DateTime(zdt.toOffsetDateTime)))
      case Value.List(values) => ???
      case Value.Map(values) => ???
      case Value.NodeId(_) => ???
      case _: Value.Node => ???
    }

  def quineValueToPatternValue(value: QuineValue): Value = value match {
    case QuineValue.Str(string) => Value.Text(string)
    case QuineValue.Integer(long) => Value.Integer(long)
    case QuineValue.Floating(double) => ???
    case QuineValue.True => ???
    case QuineValue.False => ???
    case QuineValue.Null => ???
    case QuineValue.Bytes(bytes) => ???
    case QuineValue.List(list) => Value.List(list.toList.map(quineValueToPatternValue))
    case QuineValue.Map(map) => ???
    case QuineValue.DateTime(instant) => ???
    case QuineValue.Duration(duration) => ???
    case QuineValue.Date(date) => ???
    case QuineValue.LocalTime(time) => ???
    case QuineValue.Time(time) => ???
    case QuineValue.LocalDateTime(localDateTime) => ???
    case QuineValue.Id(id) => ???
  }

  def propertyValueToPatternValue(value: PropertyValue): Value = PropertyValue.unapply(value) match {
    case Some(qv) => quineValueToPatternValue(qv)
    case None => ???
  }

  def maybeGetByIndex[A](xs: List[A], index: Int): Option[A] = index match {
    case n if n < 0 => None
    case 0 => xs.headOption
    case _ => if (xs.isEmpty) None else maybeGetByIndex(xs.tail, index - 1)
  }

  def getTextValue(value: Value): Option[String] = value match {
    case Value.Text(str) => Some(str)
    case Value.Null => None
    case _ => throw new Exception("This should be either a text value or null.")
  }

  def getIdentifier(exp: Expression): Expression.Ident = exp match {
    case ident: Expression.Ident => ident
    case _ => ???
  }

  def getNode(value: Value): Value.Node = value match {
    case node: Value.Node => node
    case _ => ???
  }

  def getNodeIdFromIdent(identifier: Identifier, queryContext: QueryContext): QuineId =
    queryContext(identifier.name) match {
      case node: Expr.Node => node.id
      case _ => ???
    }

  def invert(direction: EdgeDirection): EdgeDirection = direction match {
    case EdgeDirection.Outgoing => EdgeDirection.Incoming
    case EdgeDirection.Incoming => EdgeDirection.Outgoing
    case EdgeDirection.Undirected => EdgeDirection.Undirected
  }
}

object ExpressionInterpreter {

  def eval(
    exp: Expression,
    idProvider: QuineIdProvider,
    qc: QueryContext,
    parameters: Map[Symbol, cypher.Value],
  ): Value =
    exp match {
      case Expression.IdLookup(_, nodeIdentifier, _) =>
        Value.NodeId(
          CypherAndQuineHelpers.getNode(CypherAndQuineHelpers.cypherValueToPatternValue(qc(nodeIdentifier.name))).id,
        )
      case Expression.SynthesizeId(_, args, _) =>
        val evaledArgs = args.map(arg => eval(arg, idProvider, qc, parameters))
        val cypherIdValues = evaledArgs.map(CypherAndQuineHelpers.patternValueToCypherValue)
        val id = com.thatdot.quine.graph.idFrom(cypherIdValues: _*)(idProvider)
        Value.NodeId(id)
      case Expression.AtomicLiteral(_, value, _) => value
      case Expression.ListLiteral(source, value, ty) => ???
      case Expression.MapLiteral(source, value, ty) => ???
      case Expression.Ident(_, identifier, _) =>
        CypherAndQuineHelpers.cypherValueToPatternValue(qc(identifier.name))
      case Expression.Parameter(_, name, _) =>
        val trimName = Symbol(name.name.substring(1))
        CypherAndQuineHelpers.cypherValueToPatternValue(parameters(trimName))
      case Expression.Apply(_, name, args, _) =>
        val evaledArgs = args.map(arg => eval(arg, idProvider, qc, parameters))
        name.name match {
          case "idFrom" =>
            val cypherIdValues = evaledArgs.map(CypherAndQuineHelpers.patternValueToCypherValue)
            val id = com.thatdot.quine.graph.idFrom(cypherIdValues: _*)(idProvider)
            Value.NodeId(id)
          case "datetime" =>
            (for {
              toConvert <- CypherAndQuineHelpers.getTextValue(evaledArgs.head)
              timeFormat <- CypherAndQuineHelpers.getTextValue(evaledArgs.tail.head)
            } yield {
              val formatter = DateTimeFormatter.ofPattern(timeFormat, Locale.US)
              val dateTime = JavaZonedDateTime.parse(toConvert, formatter)
              Value.DateTime(dateTime)
            }).getOrElse(Value.Null)
          case "text.regexFirstMatch" =>
            val toMatch = evaledArgs(0).asInstanceOf[Value.Text].str
            val patternStr = evaledArgs(1).asInstanceOf[Value.Text].str
            val pattern = patternStr.r
            val regexMatch = pattern.findFirstMatchIn(toMatch).toList
            Value.List(
              for {
                m <- regexMatch
                i <- 0 to m.groupCount
              } yield Value.Text(m.group(i)),
            )
        }
      case Expression.UnaryOp(source, op, exp, ty) => ???
      case Expression.BinOp(_, op, lhs, rhs, _) =>
        op match {
          case Operator.Plus =>
            val l = eval(lhs, idProvider, qc, parameters)
            val r = eval(rhs, idProvider, qc, parameters)
            (l, r) match {
              case (Value.Null, _) => Value.Null
              case (_, Value.Null) => Value.Null
              case (Value.Text(lstr), Value.Text(rstr)) => Value.Text(lstr + rstr)
              case (Value.Integer(a), Value.Integer(b)) => Value.Integer(a + b)
              case (Value.Real(a), Value.Real(b)) => Value.Real(a + b)
              case (Value.Text(str), Value.Integer(n)) => Value.Text(str + n)
              case (_, _) =>
                println(s"??? $l and $r")
                ???
            }
          case Operator.Minus => ???
          case Operator.Asterisk => ???
          case Operator.Slash => ???
          case Operator.Percent => ???
          case Operator.Carat => ???
          case Operator.Equals => ???
          case Operator.NotEquals => ???
          case Operator.LessThan => ???
          case Operator.LessThanEqual => ???
          case Operator.GreaterThan => ???
          case Operator.GreaterThanEqual => ???
          case Operator.And => ???
          case Operator.Or => ???
          case Operator.Xor => ???
          case Operator.Not => ???
        }
      case Expression.FieldAccess(_, of, fieldName, _) =>
        val thing = eval(of, idProvider, qc, parameters)
        thing match {
          case Value.Map(values) => values(fieldName.name)
          case _ => ???
        }
      case Expression.IndexIntoArray(_, of, indexExp, _) =>
        val list = eval(of, idProvider, qc, parameters).asInstanceOf[Value.List]
        val index = eval(indexExp, idProvider, qc, parameters).asInstanceOf[Value.Integer]
        val intIndex = index.n.toInt
        CypherAndQuineHelpers.maybeGetByIndex(list.values, intIndex).getOrElse(Value.Null)
    }
}

sealed trait QueryPlan

object QueryPlan {
  case class AllNodeScan(nodeInstructions: List[QueryPlan]) extends QueryPlan
  case class SpecificId(idExp: Expression, nodeInstructions: List[QueryPlan]) extends QueryPlan
  case class Product(of: List[QueryPlan]) extends QueryPlan
  case class LoadNode(binding: Identifier) extends QueryPlan
  case class Filter(filterExpression: Expression) extends QueryPlan
  case class Effect(effect: Cypher.Effect) extends QueryPlan
  case class Project(projection: Expression, as: Symbol) extends QueryPlan
  case class CreateHalfEdge(from: Expression, to: Expression, direction: EdgeDirection, label: Symbol) extends QueryPlan
  case object UnitPlan extends QueryPlan

  def unit: QueryPlan = UnitPlan
}

object Planner {

  type EdgeData = (Identifier, Identifier, EdgeDirection, Symbol)

  case class QueryPlanState(
    seen: Set[Identifier],
    remainingFilters: List[Expression],
    remainingProjections: List[Cypher.Projection],
    remainingEffects: List[Cypher.Effect],
    remainingEdges: List[EdgeData],
  )

  type PlanningProgram[A] = State[QueryPlanState, A]

  def pure[A](a: A): PlanningProgram[A] = State.pure(a)

  def mod(update: QueryPlanState => QueryPlanState): PlanningProgram[Unit] = State.modify(update)

  def inspect[A](view: QueryPlanState => A): PlanningProgram[A] = State.inspect(view)

  def splitConjucts(filterExpression: Expression): List[Expression] =
    filterExpression match {
      case Expression.BinOp(_, Operator.And, lhs, rhs, _) =>
        splitConjucts(lhs) ::: splitConjucts(rhs)
      case _ => List(filterExpression)
    }

  def isIdPredicate(exp: Expression): Boolean = exp match {
    case Expression.BinOp(_, Operator.Equals, _: Expression.IdLookup, _, _) => true
    case Expression.BinOp(_, Operator.Equals, _, _: Expression.IdLookup, _) => true
    case _ => false
  }

  def findIdPatternFor(identifier: Identifier): PlanningProgram[Option[Expression]] = for {
    filters <- inspect(s => s.remainingFilters)
    maybeFilterExp = filters.find {
      case Expression.BinOp(_, Operator.Equals, Expression.IdLookup(_, nodeBinding, _), _, _) =>
        nodeBinding == identifier
      case Expression.BinOp(_, Operator.Equals, _, Expression.IdLookup(_, nodeBinding, _), _) =>
        nodeBinding == identifier
      case _ => false
    }
  } yield maybeFilterExp.map {
    case Expression.BinOp(_, Operator.Equals, _: Expression.IdLookup, idExp, _) => idExp
    case Expression.BinOp(_, Operator.Equals, idExp, _: Expression.IdLookup, _) => idExp
    case _ => ??? //This case is impossible due to filtering above
  }

  def getAllIdsIn(expression: Expression): Set[Identifier] = expression match {
    case Expression.IdLookup(_, id, _) => Set(id)
    case Expression.SynthesizeId(_, exps, _) =>
      exps.foldLeft(Set.empty[Identifier])((idSet, exp) => idSet.union(getAllIdsIn(exp)))
    case _: Expression.AtomicLiteral => Set.empty[Identifier] // No identifiers in a literal value
    case Expression.ListLiteral(_, values, _) =>
      values.foldLeft(Set.empty[Identifier])((resultSet, exp) => resultSet.union(getAllIdsIn(exp)))
    case Expression.MapLiteral(_, value, _) =>
      value.values.foldLeft(Set.empty[Identifier])((resultSet, exp) => resultSet.union(getAllIdsIn(exp)))
    case Expression.Ident(_, identifier, _) => Set(identifier)
    case _: Expression.Parameter => Set.empty[Identifier] // Probably need to think on this more.
    case Expression.Apply(_, _, args, _) =>
      args.foldLeft(Set.empty[Identifier])((resultSet, exp) => resultSet.union(getAllIdsIn(exp)))
    case Expression.UnaryOp(_, _, exp, _) => getAllIdsIn(exp)
    case Expression.BinOp(_, _, lhs, rhs, _) => getAllIdsIn(lhs).union(getAllIdsIn(rhs))
    case Expression.FieldAccess(_, of, _, _) => getAllIdsIn(of)
    case Expression.IndexIntoArray(_, of, _, _) => getAllIdsIn(of)
  }

  def getSatisfiedFilters(expressions: List[Expression], seen: Set[Identifier]): List[Expression] = expressions.filter {
    exp =>
      val idsInFilterExpression = getAllIdsIn(exp)
      seen.intersect(idsInFilterExpression) == idsInFilterExpression
  }

  def findFiltersFor(identifier: Identifier): PlanningProgram[List[Expression]] = for {
    _ <- mod(s => s.copy(seen = s.seen + identifier))
    seen <- inspect(_.seen)
    filters <- inspect(_.remainingFilters)
    applicableFilters = getSatisfiedFilters(filters.filterNot(isIdPredicate), seen)
    removed = filters.filterNot(applicableFilters.contains)
    _ <- mod(s => s.copy(remainingFilters = removed))
  } yield applicableFilters

  def getSatisfiedProjections(projections: List[Cypher.Projection], seen: Set[Identifier]): List[Cypher.Projection] =
    projections.filter { p =>
      val idsInProjectionExpression = getAllIdsIn(p.expression)
      seen.intersect(idsInProjectionExpression) == idsInProjectionExpression
    }

  def getRootId(expression: Expression): Identifier = expression match {
    case Expression.FieldAccess(_, of, _, _) => getRootId(of)
    case Expression.Ident(_, id, _) => id
    case _ => throw new QuinePatternUnimplementedException(s"Unexpected expression: $expression")
  }

  def findEffectsFor(id: Identifier): PlanningProgram[List[Cypher.Effect]] = for {
    effects <- inspect(_.remainingEffects)
    applicableEffects = effects.filter {
      case Cypher.Effect.SetProperty(_, property, _) => getRootId(property) == id
      case Cypher.Effect.SetProperties(_, of, _) => of == id
      case Cypher.Effect.SetLabel(_, on, _) => on == id
      case _: Cypher.Effect.Create => false
    }
    removed = effects.filterNot(applicableEffects.contains)
    _ <- mod(s => s.copy(remainingEffects = removed))
  } yield applicableEffects

  def findProjectionsFor(identifier: Identifier): PlanningProgram[List[Cypher.Projection]] = for {
    _ <- mod(s => s.copy(seen = s.seen + identifier))
    seen <- inspect(_.seen)
    projections <- inspect(_.remainingProjections)
    applicableProjections = getSatisfiedProjections(projections, seen)
    aliases = applicableProjections.map(_.as).toSet
    newSeen = seen.union(aliases)
    _ <- mod(s => s.copy(seen = newSeen))
    removed = projections.filterNot(applicableProjections.contains)
    _ <- mod(s => s.copy(remainingProjections = removed))
  } yield applicableProjections

  def planProjections(projections: List[Cypher.Projection]): List[QueryPlan] =
    projections.map(p => QueryPlan.Project(p.expression, p.as.name))

  def planFilters(expressions: List[Expression]): List[QueryPlan] =
    expressions.map(e => QueryPlan.Filter(e))

  def planEffects(effects: List[Cypher.Effect]): List[QueryPlan] =
    effects.map(QueryPlan.Effect)

  def planConnections(connections: List[Cypher.Connection]): List[QueryPlan] = List(QueryPlan.unit)

  def getLocalHalfEdges(identifier: Identifier, idExp: Expression): PlanningProgram[List[QueryPlan.CreateHalfEdge]] =
    for {
      halfEdges <- inspect(_.remainingEdges)
      localHalfEdges = halfEdges.filter { case (from, _, _, _) =>
        from == identifier
      }
      edgeCreation <- localHalfEdges.traverse { case (_, to, dir, label) =>
        findIdPatternFor(to).map { otherId =>
          QueryPlan.CreateHalfEdge(idExp, otherId.get, dir, label)
        }
      }
      removedEdges = halfEdges.filterNot(localHalfEdges.contains)
      _ <- mod(s => s.copy(remainingEdges = removedEdges))
    } yield edgeCreation

  def planGraphPattern(pattern: Cypher.GraphPattern): PlanningProgram[QueryPlan] = pattern.initial.maybeBinding match {
    case None => pure(QueryPlan.AllNodeScan(planConnections(pattern.path)))
    case Some(nodeBinding) =>
      for {
        maybeIdAnchor <- findIdPatternFor(nodeBinding)
        filters <- findFiltersFor(nodeBinding)
        projections <- findProjectionsFor(nodeBinding)
        planForProjections = planProjections(projections)
        planForFilters = planFilters(filters)
        effects <- findEffectsFor(nodeBinding)
        planForEffects = planEffects(effects)
        edgePlans <- getLocalHalfEdges(nodeBinding, maybeIdAnchor.get)
        planForConnections = planConnections(pattern.path)
      } yield {
        val load: QueryPlan = QueryPlan.LoadNode(nodeBinding)
        maybeIdAnchor match {
          case None =>
            QueryPlan.AllNodeScan(
              load :: planForFilters ::: planForProjections ::: planForEffects ::: edgePlans ::: planForConnections,
            )
          case Some(idExp) =>
            QueryPlan.SpecificId(
              idExp,
              load :: planForFilters ::: planForProjections ::: planForEffects ::: edgePlans ::: planForConnections,
            )
        }
      }
  }

  def planPatterns(
    patterns: List[Cypher.GraphPattern],
    maybeExpression: Option[Expression],
    effects: List[Cypher.Effect],
    halfEdges: List[EdgeData],
  ): PlanningProgram[List[QueryPlan]] = {
    val filterExpressions = maybeExpression.map(splitConjucts).getOrElse(Nil)
    for {
      _ <- mod(s =>
        s.copy(remainingFilters = filterExpressions, remainingEffects = effects, remainingEdges = halfEdges),
      )
      patternPlans <- patterns.traverse(planGraphPattern)
    } yield patternPlans
  }

  def planReadingClause(
    clause: Cypher.ReadingClause,
    localEffects: List[Cypher.Effect],
    halfEdges: List[EdgeData],
  ): PlanningProgram[List[QueryPlan]] =
    clause match {
      case Cypher.ReadingClause.FromPatterns(_, patterns, maybePredicate) =>
        planPatterns(patterns, maybePredicate, localEffects, halfEdges)
      case Cypher.ReadingClause.FromUnwind(source, list, as) => ???
      case Cypher.ReadingClause.FromProcedure(source, name, args, yields) => ???
      case Cypher.ReadingClause.FromSubquery(source, bindings, subquery) => ???
    }

  def planPart(localEffects: List[Cypher.Effect], halfEdges: List[EdgeData])(
    queryPart: Cypher.QueryPart,
  ): PlanningProgram[List[QueryPlan]] =
    queryPart match {
      case Cypher.QueryPart.ReadingClausePart(readingClause) =>
        planReadingClause(readingClause, localEffects, halfEdges)
      case Cypher.QueryPart.WithClausePart(withClause) => ???
      case Cypher.QueryPart.EffectPart(effect) => pure(List(QueryPlan.Effect(effect)))
    }

  def isLocalEffect(effect: Cypher.Effect): Boolean = effect match {
    case Cypher.Effect.SetProperty(_, _, _) | Cypher.Effect.SetProperties(_, _, _) | Cypher.Effect.SetLabel(_, _, _) =>
      true
    case _ => false
  }

  def filterAndRemoveLocalEffects(parts: List[Cypher.QueryPart]): (List[Cypher.Effect], List[Cypher.QueryPart]) = {
    val localEffects = parts.collect {
      case Cypher.QueryPart.EffectPart(effect) if isLocalEffect(effect) => effect
    }

    val nonLocalParts = parts.filter {
      case Cypher.QueryPart.EffectPart(effect) => !isLocalEffect(effect)
      case _ => true
    }

    (localEffects, nonLocalParts)
  }

  def filterAndRemoveCreateEffects(
    parts: List[Cypher.QueryPart],
  ): (List[Cypher.Effect.Create], List[Cypher.QueryPart]) = {
    val createEffects = parts.collect { case Cypher.QueryPart.EffectPart(effect: Cypher.Effect.Create) =>
      effect
    }

    val nonCreateParts = parts.filter {
      case Cypher.QueryPart.EffectPart(_: Cypher.Effect.Create) => false
      case _ => true
    }

    (createEffects, nonCreateParts)
  }

  def directionToEdgeDirection(direction: Direction): EdgeDirection = direction match {
    case Direction.Left => EdgeDirection.Outgoing
    case Direction.Right => EdgeDirection.Incoming
  }

  def halfEdgesFromGraphPattern(pattern: Cypher.GraphPattern): List[EdgeData] =
    pattern.path
      .foldLeft(List.empty[EdgeData] -> pattern.initial) { (result, connection) =>
        val source = result._2.maybeBinding.get
        val dest = connection.dest.maybeBinding.get
        val dir = directionToEdgeDirection(connection.edge.direction)
        val edgeType = connection.edge.labels.head
        val startHalfEdge = (source, dest, dir, edgeType)
        val endHalfEdge = (dest, source, dir.reverse, edgeType)
        (startHalfEdge :: endHalfEdge :: result._1, connection.dest)
      }
      ._1

  def planCreateHalfEdges(createEffects: List[Cypher.Effect.Create]): List[EdgeData] =
    createEffects.flatMap(e => e.patterns.flatMap(halfEdgesFromGraphPattern))

  def planForSinglepartQuery(query: Cypher.Query.SingleQuery.SinglepartQuery): PlanningProgram[QueryPlan] = for {
    _ <- mod(s => s.copy(remainingProjections = query.bindings))
    result = filterAndRemoveLocalEffects(query.queryParts)
    result1 = filterAndRemoveCreateEffects(result._2)
    halfEdges = planCreateHalfEdges(result1._1)
    plansForParts <- result1._2.flatTraverse(planPart(result._1, halfEdges))
  } yield QueryPlan.Product(plansForParts)

  def generatePlanFromDescription(queryDescription: Cypher.Query): QueryPlan = queryDescription match {
    case query: Cypher.Query.SingleQuery =>
      query match {
        case single: Cypher.Query.SingleQuery.SinglepartQuery =>
          planForSinglepartQuery(single).runA(QueryPlanState(Set(), Nil, Nil, Nil, Nil)).value
        case _ => throw new QuinePatternUnimplementedException(s"$queryDescription is not currently handled.")
      }
    case _ => throw new QuinePatternUnimplementedException(s"$queryDescription is not currently handled.")
  }
}

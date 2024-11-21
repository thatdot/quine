package com.thatdot.quine.graph.cypher

import java.time.format.DateTimeFormatter
import java.time.{ZonedDateTime => JavaZonedDateTime}
import java.util.Locale

import scala.collection.immutable.SortedMap
import scala.concurrent.Promise

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import com.google.common.collect.Interners

import com.thatdot.cypher.phases.SymbolAnalysisModule.SymbolTable
import com.thatdot.language.ast._
import com.thatdot.language.phases.DependencyGraph
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.{NamespaceId, QuinePatternOpsGraph, cypher}
import com.thatdot.quine.model.{EdgeDirection, PropertyValue, QuineId, QuineIdProvider, QuineValue}

sealed trait BinOp

object BinOp {
  case object Merge extends BinOp
  case object Append extends BinOp
}

class QuinePatternUnimplementedException(msg: String) extends RuntimeException(msg)

sealed trait QuinePattern

sealed trait Output

object QuinePattern {
  private val interner = Interners.newWeakInterner[QuinePattern]()

  case object QuineUnit extends QuinePattern

  case class Node(binding: Identifier) extends QuinePattern
  case class Edge(edgeLabel: Symbol, remotePattern: QuinePattern) extends QuinePattern

  case class Fold(init: QuinePattern, over: List[QuinePattern], f: BinOp, output: Output) extends QuinePattern

  def mkNode(binding: Identifier): QuinePattern =
    interner.intern(Node(binding))

  def mkEdge(edgeLabel: Symbol, remotePattern: QuinePattern): QuinePattern =
    interner.intern(Edge(edgeLabel, remotePattern))

  def mkFold(init: QuinePattern, over: List[QuinePattern], f: BinOp, output: Output): QuinePattern =
    interner.intern(Fold(interner.intern(init), over.map(interner.intern), f, output))
}

object CypherAndQuineHelpers {
  def patternValueToCypherValue(value: Value): cypher.Value =
    value match {
      case Value.Null => Expr.Null
      case Value.True => ???
      case Value.False => ???
      case Value.Integer(n) => Expr.Integer(n)
      case Value.Real(d) => ???
      case Value.Text(str) => Expr.Str(str)
      case Value.DateTime(zdt) => Expr.DateTime(zdt)
      case Value.List(values) => Expr.List(values.toVector.map(patternValueToCypherValue))
      case Value.Map(values) => Expr.Map(values.map(p => p._1.name -> patternValueToCypherValue(p._2)))
      case Value.NodeId(_) => ???
      case _: Value.Node => ???
    }

  def cypherValueToPatternValue(value: cypher.Value): Value = value match {
    case value: Expr.PropertyValue =>
      value match {
        case Expr.Str(string) => Value.Text(string)
        case Expr.Integer(long) => Value.Integer(long)
        case Expr.Floating(double) => ???
        case Expr.True => ???
        case Expr.False => ???
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
    case QuineValue.Str(string) => ???
    case QuineValue.Integer(long) => ???
    case QuineValue.Floating(double) => ???
    case QuineValue.True => ???
    case QuineValue.False => ???
    case QuineValue.Null => ???
    case QuineValue.Bytes(bytes) => ???
    case QuineValue.List(list) => ???
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

  def eval(exp: Expression, idProvider: QuineIdProvider, qc: QueryContext): Value =
    exp match {
      case Expression.AtomicLiteral(_, value, _) => value
      case Expression.ListLiteral(source, value, ty) => ???
      case Expression.MapLiteral(source, value, ty) => ???
      case Expression.Ident(_, identifier, _) =>
        CypherAndQuineHelpers.cypherValueToPatternValue(qc(identifier.name))
      case Expression.Parameter(_, name, _) =>
        val trimName = Symbol(name.name.substring(1))
        CypherAndQuineHelpers.cypherValueToPatternValue(qc(trimName))
      case Expression.Apply(_, name, args, _) =>
        val evaledArgs = args.map(arg => eval(arg, idProvider, qc))
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
            val l = eval(lhs, idProvider, qc)
            val r = eval(rhs, idProvider, qc)
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
        val thing = eval(of, idProvider, qc)
        thing match {
          case Value.Map(values) => values(fieldName.name)
          case _ => ???
        }
      case Expression.IndexIntoArray(_, of, indexExp, _) =>
        val list = eval(of, idProvider, qc).asInstanceOf[Value.List]
        val index = eval(indexExp, idProvider, qc).asInstanceOf[Value.Integer]
        val intIndex = index.n.toInt
        CypherAndQuineHelpers.maybeGetByIndex(list.values, intIndex).getOrElse(Value.Null)
    }
}

object Compiler {

  def compileDepGraphToStream(
    dependencyGraph: DependencyGraph,
    namespace: NamespaceId,
    stream: Source[QueryContext, NotUsed],
    graph: QuinePatternOpsGraph,
  ): Source[QueryContext, NotUsed] =
    dependencyGraph match {
      case DependencyGraph.Independent(steps) =>
        steps.foldLeft(stream)((res, g) => compileDepGraphToStream(g, namespace, res, graph))
      case DependencyGraph.Dependent(first, second) =>
        compileDepGraphToStream(
          second,
          namespace,
          compileDepGraphToStream(first, namespace, stream, graph),
          graph,
        )
      case DependencyGraph.Step(instruction) =>
        instruction match {
          case Instruction.NodeById(binding, idExp) =>
            stream.via(Flow[QueryContext].mapAsync(1) { qc =>
              val promise = Promise[QueryContext]()
              ExpressionInterpreter.eval(idExp, graph.idProvider, qc) match {
                case Value.NodeId(id) =>
                  val stqid = SpaceTimeQuineId(id, namespace, None)
                  graph.relayTell(stqid, QuinePatternCommand.LoadNode(binding, qc, promise))
                case _ =>
                  //TODO Should be able to handle certain kinds of values here, but how?
                  ???
              }
              promise.future
            })
          case i @ Instruction.Effect(le) =>
            le match {
              case LocalEffect.SetProperty(field, to) =>
                stream.via(Flow[QueryContext].mapAsync(1) { qc =>
                  val result = Promise[QueryContext]()
                  val idExp = CypherAndQuineHelpers.getIdentifier(field.of)
                  val node = CypherAndQuineHelpers.getNode(ExpressionInterpreter.eval(idExp, graph.idProvider, qc))
                  val stqid = SpaceTimeQuineId(node.id, namespace, None)
                  graph.relayTell(
                    stqid,
                    QuinePatternCommand.SetProperty(field.fieldName.name, to, idExp.identifier, qc, result),
                  )
                  result.future
                })
              case LocalEffect.SetLabels(on, labels) =>
                stream.via(Flow[QueryContext].mapAsync(1) { qc =>
                  val result = Promise[QueryContext]()
                  qc(on.name) match {
                    case node: Expr.Node =>
                      val stqid = SpaceTimeQuineId(node.id, namespace, None)
                      graph.relayTell(stqid, QuinePatternCommand.SetLabels(labels, on, qc, result))
                    case _ => ???
                  }
                  result.future
                })
              case LocalEffect.CreateNode(id, labels, maybeProperties) =>
                stream.via(Flow[QueryContext].mapAsync(1) { qc =>
                  val result = Promise[QueryContext]()
                  qc(id.name) match {
                    case node: Expr.Node =>
                      val stqid = SpaceTimeQuineId(node.id, namespace, None)
                      graph.relayTell(stqid, QuinePatternCommand.PopulateNode(labels, maybeProperties, id, qc, result))
                    case _ => ???
                  }
                  result.future
                })
              case LocalEffect.CreateEdge(labels, direction, leftIdent, rightIdent, _) =>
                val quineDirection = direction match {
                  case Direction.Left => EdgeDirection.Outgoing
                  case Direction.Right => EdgeDirection.Incoming
                }
                stream
                  .via(Flow[QueryContext].mapAsync(1) { qc =>
                    val result = Promise[QueryContext]()
                    val leftId = CypherAndQuineHelpers.getNodeIdFromIdent(leftIdent, qc)
                    val rightId = CypherAndQuineHelpers.getNodeIdFromIdent(rightIdent, qc)
                    val stqid = SpaceTimeQuineId(leftId, namespace, None)
                    graph
                      .relayTell(stqid, QuinePatternCommand.AddEdge(labels.head, quineDirection, rightId, qc, result))
                    result.future
                  })
                  .via(Flow[QueryContext].mapAsync(1) { qc =>
                    val result = Promise[QueryContext]()
                    val leftId = CypherAndQuineHelpers.getNodeIdFromIdent(leftIdent, qc)
                    val rightId = CypherAndQuineHelpers.getNodeIdFromIdent(rightIdent, qc)
                    val stqid = SpaceTimeQuineId(rightId, namespace, None)
                    graph.relayTell(
                      stqid,
                      QuinePatternCommand.AddEdge(labels.head, quineDirection.reverse, leftId, qc, result),
                    )
                    result.future
                  })
              case _ => throw new QuinePatternUnimplementedException(s"Unexpected instruction $i")
            }
          case Instruction.Proj(exp, as) =>
            stream.via(Flow[QueryContext].map { qc =>
              val evaled = ExpressionInterpreter.eval(exp, graph.idProvider, qc)
              val cypherValue = CypherAndQuineHelpers.patternValueToCypherValue(evaled)
              qc + (as.name -> cypherValue)
            })
          case _ =>
            println(s"Got an unexpected instruction $instruction")
            stream
        }
      case DependencyGraph.Empty => stream
    }

  def compile(program: List[Instruction], symbolTable: SymbolTable): DependencyGraph =
    program.foldLeft(DependencyGraph.Empty.asInstanceOf[DependencyGraph])((graph, nextInst) =>
      DependencyGraph.Dependent(graph, DependencyGraph.Step(nextInst)),
    )
}

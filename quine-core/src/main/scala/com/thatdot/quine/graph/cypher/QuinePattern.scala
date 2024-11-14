package com.thatdot.quine.graph.cypher

import java.time.format.DateTimeFormatter
import java.time.{ZonedDateTime => JavaZonedDateTime}
import java.util.Locale

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import com.google.common.collect.Interners

import com.thatdot.cypher.phases.SymbolAnalysisModule.SymbolTable
import com.thatdot.language.ast._
import com.thatdot.language.phases.DependencyGraph
import com.thatdot.quine.graph.messaging.{LiteralMessage, SpaceTimeQuineId, WrappedActorRef}
import com.thatdot.quine.graph.{NamespaceId, QuinePatternOpsGraph, cypher}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineId, QuineValue}

sealed trait BinOp

object BinOp {
  case object Merge extends BinOp
  case object Append extends BinOp
}

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
    case Expr.Node(id, labels, properties) => ???
    case Expr.Relationship(start, name, properties, end) => ???
    case Expr.Path(head, tails) => ???
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

  def getNodeIdFromQueryContext(
    identifier: Identifier,
    namespaceId: NamespaceId,
    queryContext: QueryContext,
  ): SpaceTimeQuineId = {
    val nodeBytes = queryContext(identifier.name) match {
      case Expr.Bytes(ba, true) => ba
      case _ =>
        println("Failed to get bytes?")
        ???
    }
    val quineId = QuineId(nodeBytes.clone())
    SpaceTimeQuineId(quineId, namespaceId, None)
  }

  def invert(direction: EdgeDirection): EdgeDirection = direction match {
    case EdgeDirection.Outgoing => EdgeDirection.Incoming
    case EdgeDirection.Incoming => EdgeDirection.Outgoing
    case EdgeDirection.Undirected => EdgeDirection.Undirected
  }
}

object ExpressionInterpreter {
  def eval(exp: Expression, qc: QueryContext): Value =
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
        val evaledArgs = args.map(arg => eval(arg, qc))
        name.name match {
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
            val l = eval(lhs, qc)
            val r = eval(rhs, qc)
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
        val thing = eval(of, qc)
        thing match {
          case Value.Map(values) => values(fieldName.name)
          case _ => ???
        }
      case Expression.IndexIntoArray(_, of, indexExp, _) =>
        val list = eval(of, qc).asInstanceOf[Value.List]
        val index = eval(indexExp, qc).asInstanceOf[Value.Integer]
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
    loader: ActorRef,
  ): Source[QueryContext, NotUsed] =
    dependencyGraph match {
      case DependencyGraph.Independent(steps) =>
        steps.foldLeft(stream)((res, g) => compileDepGraphToStream(g, namespace, res, graph, loader))
      case DependencyGraph.Dependent(first, second) =>
        compileDepGraphToStream(
          second,
          namespace,
          compileDepGraphToStream(first, namespace, stream, graph, loader),
          graph,
          loader,
        )
      case DependencyGraph.Step(instruction) =>
        instruction match {
          case Instruction.NodeById(binding, idParts) =>
            stream.via(Flow[QueryContext].map { qc =>
              val idCypherValues =
                idParts.map(p => ExpressionInterpreter.eval(p, qc)).map(CypherAndQuineHelpers.patternValueToCypherValue)
              val nodeId = com.thatdot.quine.graph.idFrom(idCypherValues: _*)(graph.idProvider)
              val nodeIdBytes = Expr.Bytes(nodeId)
              qc + (binding.name -> nodeIdBytes)
            })
          case Instruction.Effect(le) =>
            stream.via(Flow[QueryContext].map { qc =>
              le match {
                case LocalEffect.SetProperty(field, to) =>
                  field.of match {
                    case Expression.Ident(_, identifier, _) =>
                      val stqid = CypherAndQuineHelpers.getNodeIdFromQueryContext(identifier, namespace, qc)
                      CypherAndQuineHelpers.patternValueToPropertyValue(ExpressionInterpreter.eval(to, qc)) match {
                        case None =>
                          graph.relayTell(
                            stqid,
                            LiteralMessage.RemovePropertyCommand(field.fieldName.name, WrappedActorRef(loader)),
                          )
                        case Some(value) =>
                          graph.relayTell(
                            stqid,
                            LiteralMessage.SetPropertyCommand(field.fieldName.name, value, WrappedActorRef(loader)),
                          )
                      }
                    case _ => ???
                  }
                case LocalEffect.SetLabels(on, labels) =>
                  val stqid = CypherAndQuineHelpers.getNodeIdFromQueryContext(on, namespace, qc)
                  graph.relayTell(stqid, LiteralMessage.SetLabels(labels, WrappedActorRef(loader)))
                case LocalEffect.CreateNode(id, labels, maybeProperties) =>
                  qc.get(id.name) match {
                    case Some(_) => ()
                    case None =>
                      val freshId = graph.idProvider.newQid()
                      val stqid = SpaceTimeQuineId(freshId, namespace, None)
                      graph.relayTell(stqid, LiteralMessage.SetLabels(labels, WrappedActorRef(loader)))
                      maybeProperties match {
                        case None => ()
                        case Some(map) =>
                          map.value.foreach { p =>
                            val pname = p._1.name
                            val evaled = ExpressionInterpreter.eval(p._2, qc)
                            val pval = CypherAndQuineHelpers.patternValueToPropertyValue(evaled)
                            pval match {
                              case Some(v) =>
                                graph.relayTell(
                                  stqid,
                                  LiteralMessage.SetPropertyCommand(pname, v, WrappedActorRef(loader)),
                                )
                              case None =>
                                graph.relayTell(
                                  stqid,
                                  LiteralMessage.RemovePropertyCommand(pname, WrappedActorRef(loader)),
                                )
                            }
                          }
                      }
                  }
                case LocalEffect.CreateEdge(labels, direction, left, right, _) =>
                  val ed = direction match {
                    case Direction.Left => EdgeDirection.Outgoing
                    case Direction.Right => EdgeDirection.Incoming
                  }
                  val lid = CypherAndQuineHelpers.getNodeIdFromQueryContext(left, namespace, qc)
                  val rid = CypherAndQuineHelpers.getNodeIdFromQueryContext(right, namespace, qc)
                  graph.relayTell(
                    lid,
                    LiteralMessage.AddHalfEdgeCommand(HalfEdge(labels.head, ed, rid.id), WrappedActorRef(loader)),
                  )
                  graph.relayTell(
                    rid,
                    LiteralMessage.AddHalfEdgeCommand(
                      HalfEdge(labels.head, CypherAndQuineHelpers.invert(ed), lid.id),
                      WrappedActorRef(loader),
                    ),
                  )
                case _ =>
                  println("oops")
                  ???
              }
              qc
            })
          case Instruction.Proj(exp, as) =>
            stream.via(Flow[QueryContext].map { qc =>
              val evaled = ExpressionInterpreter.eval(exp, qc)
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

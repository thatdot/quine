package com.thatdot.quine.graph.cypher.quinepattern

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Status.Success
import org.apache.pekko.stream.CompletionStrategy
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.common.quineid.QuineId
import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.ast.{Value => PatternValue}
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.{Expr, QueryContext, Value}
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph

sealed trait QueryTarget

object QueryTarget {
  case class Node(qid: QuineId) extends QueryTarget
  case object All extends QueryTarget
  case object None extends QueryTarget
}

object QuinePatternInterpreter {

  def interpret(
    queryPlan: QueryPlan,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    queryPlan match {
      case allNodeScan: QueryPlan.AllNodeScan => interpretAllNodeScan(allNodeScan, target, parameters)
      case anchorById: QueryPlan.SpecificId => interpretById(anchorById, target, namespace, parameters, ctx)
      case _: QueryPlan.TraverseEdge => throw new QuinePatternUnimplementedException("traverseEdge")
      case product: QueryPlan.Product => interpretApply(product, target, namespace, parameters, ctx)
      case loadNode: QueryPlan.LoadNode => interpretLoad(loadNode, target, namespace, parameters, ctx)
      case unwind: QueryPlan.Unwind => interpretUnwind(unwind, target, namespace, parameters, ctx)
      case filter: QueryPlan.Filter => interpretFilter(filter, target, parameters, ctx)
      case effect: QueryPlan.Effect => interpretEffect(effect, target, namespace, parameters, ctx)
      case project: QueryPlan.Project => interpretProjection(project, target, parameters, ctx)
      case _: QueryPlan.CreateHalfEdge => throw new QuinePatternUnimplementedException("createEdge")
      case call: QueryPlan.ProcedureCall => interpretCall(call, namespace, parameters, ctx)
      case QueryPlan.UnitPlan => interpretUnit(ctx)
    }

  def interpretCall(
    call: QueryPlan.ProcedureCall,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    call.name.name match {
      case "incrementCounter" =>
        val evaledArgs =
          call.args.map(arg => QuinePatternExpressionInterpreter.eval(arg, graph.idProvider, ctx, parameters))
        val nodeId = evaledArgs.head match {
          case PatternValue.NodeId(id) => id
          case PatternValue.Node(id, _, _) => id
          case _ => throw new QuinePatternUnimplementedException("int.add")
        }
        val stqid = SpaceTimeQuineId(nodeId, namespace, None)
        val propName = evaledArgs.tail.head match {
          case PatternValue.Text(name) => name
          case _ => throw new QuinePatternUnimplementedException("int.add")
        }
        val amount = evaledArgs.tail.tail.head match {
          case PatternValue.Integer(n) => n
          case _ => throw new QuinePatternUnimplementedException("int.add")
        }
        QuinePatternHelpers.createNodeActionSource { ref =>
          graph.relayTell(
            stqid,
            QuinePatternCommand.AtomicIncrement(propName, amount.toInt, call.yields.head, ctx, ref),
          )
          NotUsed
        }
    }

  def interpretUnwind(
    unwind: QueryPlan.Unwind,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    QuinePatternExpressionInterpreter.eval(unwind.listExp, graph.idProvider, ctx, parameters) match {
      case PatternValue.List(values) =>
        values.foldLeft(Source.single(ctx)) { (src, value) =>
          src.flatMapConcat { ctx =>
            val newCtx = ctx + (unwind.alias.name -> QuinePatternHelpers.patternValueToCypherValue(value))
            interpret(unwind.over, target, namespace, parameters, newCtx)
          }
        }
      case other => throw new QuinePatternUnimplementedException(s"Unsupported value type for unwind: $other")
    }

  def interpretFilter(filter: QueryPlan.Filter, target: QueryTarget, parameters: Map[Symbol, Value], ctx: QueryContext)(
    implicit graph: QuinePatternOpsGraph,
  ): Source[QueryContext, NotUsed] =
    target match {
      case _: QueryTarget.Node => throw new QuinePatternUnimplementedException("Filtering on nodes")
      case QueryTarget.All => throw new QuinePatternUnimplementedException("Filtering on all nodes")
      case QueryTarget.None =>
        Source.single(ctx).filter { ctx =>
          QuinePatternExpressionInterpreter.eval(filter.filterExpression, graph.idProvider, ctx, parameters) match {
            case PatternValue.True => true
            case PatternValue.False => false
            case other => throw new QuinePatternUnimplementedException(s"Unsupported value type for filter: $other")
          }
        }
    }

  def interpretCreatePatternWithConnections(
    pattern: Cypher.NodePattern,
    connections: List[Cypher.Connection],
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    connections match {
      case h :: t =>
        val binding = pattern.maybeBinding.get
        val fromId = ctx(binding.name).asInstanceOf[Expr.Node].id
        val toId = ctx(h.dest.maybeBinding.get.name).asInstanceOf[Expr.Node].id
        val fromStqid = SpaceTimeQuineId(fromId, namespace, None)
        val toStqid = SpaceTimeQuineId(toId, namespace, None)
        val directionFrom = QuinePatternHelpers.directionToEdgeDirection(h.edge.direction)
        QuinePatternHelpers
          .createNodeActionSource { ref =>
            graph.relayTell(
              fromStqid,
              QuinePatternCommand.CreateEdge(toId, directionFrom, h.edge.labels.head, ctx, ref),
            )
            NotUsed
          }
          .flatMapConcat { nextCtx =>
            QuinePatternHelpers
              .createNodeActionSource { ref =>
                graph.relayTell(
                  toStqid,
                  QuinePatternCommand.CreateEdge(fromId, directionFrom.reverse, h.edge.labels.head, nextCtx, ref),
                )
                NotUsed
              }
              .flatMapConcat(nextCtx =>
                interpretCreatePatternWithConnections(h.dest, t, target, namespace, parameters, nextCtx),
              )
          }
      case Nil =>
        if (pattern.labels.nonEmpty || pattern.maybeProperties.nonEmpty) {
          val freshId = graph.idProvider.newQid()
          val stqid = SpaceTimeQuineId(freshId, namespace, None)
          QuinePatternHelpers
            .createNodeActionSource { ref =>
              graph.relayTell(stqid, QuinePatternCommand.SetLabels(pattern.labels, ctx, ref))
              NotUsed
            }
            .flatMapConcat { nextCtx =>
              QuinePatternHelpers
                .createNodeActionSource { ref =>
                  pattern.maybeProperties match {
                    case Some(properties) =>
                      val result = QuinePatternExpressionInterpreter.eval(
                        properties,
                        graph.idProvider,
                        nextCtx,
                        parameters,
                      ) match {
                        case PatternValue.Map(values) => values
                        case _ => ???
                      }
                      graph.relayTell(stqid, QuinePatternCommand.SetProperties(result, nextCtx, ref))
                    case None => ref ! nextCtx
                  }
                  NotUsed
                }
                .flatMapConcat { nextCtx =>
                  pattern.maybeBinding match {
                    case None => Source.single(nextCtx)
                    case Some(binding) =>
                      Source
                        .actorRefWithBackpressure[QueryContext](
                          ackMessage = QuinePatternCommand.QuinePatternAck,
                          completionMatcher = { case _: Success =>
                            CompletionStrategy.immediately
                          },
                          failureMatcher = PartialFunction.empty,
                        )
                        .mapMaterializedValue { ref =>
                          graph.relayTell(stqid, QuinePatternCommand.LoadNode(binding, nextCtx, ref))
                          NotUsed
                        }
                  }
                }
            }
        } else {
          Source.single(ctx)
        }
    }

  def interpretCreatePattern(
    pattern: Cypher.GraphPattern,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    interpretCreatePatternWithConnections(pattern.initial, pattern.path, target, namespace, parameters, ctx)

  def interpretEffect(
    effectPlan: QueryPlan.Effect,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    target match {
      case QueryTarget.Node(qid) =>
        effectPlan.effect match {
          case Cypher.Effect.SetProperty(_, property, value) =>
            val stqid = SpaceTimeQuineId(qid, namespace, None)
            Source
              .actorRefWithBackpressure[QueryContext](
                ackMessage = QuinePatternCommand.QuinePatternAck,
                completionMatcher = { case _: Success =>
                  CompletionStrategy.immediately
                },
                failureMatcher = PartialFunction.empty,
              )
              .mapMaterializedValue { ref =>
                val root = QuinePatternHelpers.getRootId(property.of)
                graph.relayTell(
                  stqid,
                  QuinePatternCommand.SetProperty(root, property.fieldName.name, value, ctx, parameters, ref),
                )
                NotUsed
              }
          case _: Cypher.Effect.SetProperties => throw new QuinePatternUnimplementedException("SetProperties")
          case Cypher.Effect.SetLabel(_, _, labels) =>
            val stqid = SpaceTimeQuineId(qid, namespace, None)
            Source
              .actorRefWithBackpressure[QueryContext](
                ackMessage = QuinePatternCommand.QuinePatternAck,
                completionMatcher = { case _: Success =>
                  CompletionStrategy.immediately
                },
                failureMatcher = PartialFunction.empty,
              )
              .mapMaterializedValue { ref =>
                graph.relayTell(stqid, QuinePatternCommand.SetLabels(labels, ctx, ref))
                NotUsed
              }
          case _: Cypher.Effect.Create => throw new QuinePatternUnimplementedException("Create")
          case _: Cypher.Effect.Foreach => throw new QuinePatternUnimplementedException("Foreach")
        }
      case QueryTarget.All => throw new QuinePatternUnimplementedException("Effects should not be applied to all nodes")
      case QueryTarget.None =>
        effectPlan.effect match {
          case Cypher.Effect.Foreach(_, binding, in, effects) =>
            QuinePatternExpressionInterpreter.eval(in, graph.idProvider, ctx, parameters) match {
              case PatternValue.List(values) =>
                values
                  .flatMap { v =>
                    val tempCtx = ctx + (binding -> QuinePatternHelpers.patternValueToCypherValue(v))
                    effects
                      .map(e => interpretEffect(QueryPlan.Effect(e), target, namespace, parameters, tempCtx))
                  }
                  .foldLeft(Source.single(ctx)) { (src, nextStream) =>
                    for {
                      goodCtx <- src
                      _ <- nextStream
                    } yield goodCtx
                  }
              case other => throw new QuinePatternUnimplementedException(s"Foreach not supported for $other")
            }
          case Cypher.Effect.SetProperty(_, property, value) =>
            val ident = QuinePatternHelpers.getRootId(property.of)
            ctx(ident.name) match {
              case Expr.Node(id, _, _) =>
                val stqid = SpaceTimeQuineId(id, namespace, None)
                Source
                  .actorRefWithBackpressure[QueryContext](
                    ackMessage = QuinePatternCommand.QuinePatternAck,
                    completionMatcher = { case _: Success =>
                      CompletionStrategy.immediately
                    },
                    failureMatcher = PartialFunction.empty,
                  )
                  .mapMaterializedValue { ref =>
                    graph.relayTell(
                      stqid,
                      QuinePatternCommand.SetProperty(ident, property.fieldName.name, value, ctx, parameters, ref),
                    )
                    NotUsed
                  }
              case other =>
                throw new QuinePatternUnimplementedException(s"Unexpected value type $other for effect.SetProperty")
            }
          case Cypher.Effect.SetProperties(_, of, properties) =>
            ctx(of.name) match {
              case Expr.Node(id, _, _) =>
                val stqid = SpaceTimeQuineId(id, namespace, None)
                QuinePatternHelpers.createNodeActionSource { ref =>
                  val result =
                    QuinePatternExpressionInterpreter.eval(properties, graph.idProvider, ctx, parameters) match {
                      case PatternValue.Map(values) => values
                      case _ => ???
                    }
                  graph.relayTell(stqid, QuinePatternCommand.SetProperties(result, ctx, ref))
                  NotUsed
                }
              case _ => ???
            }
          case Cypher.Effect.SetLabel(_, on, labels) =>
            ctx(on.name) match {
              case Expr.Node(id, _, _) =>
                val stqid = SpaceTimeQuineId(id, namespace, None)
                QuinePatternHelpers.createNodeActionSource { ref =>
                  graph.relayTell(stqid, QuinePatternCommand.SetLabels(labels, ctx, ref))
                  NotUsed
                }
              case _ => ???
            }
          case Cypher.Effect.Create(_, patterns) =>
            patterns.foldLeft(Source.single(ctx))((src, pattern) =>
              src.flatMapConcat(ctx => interpretCreatePattern(pattern, target, namespace, parameters, ctx)),
            )
        }
    }

  def interpretLoad(
    loadPlan: QueryPlan.LoadNode,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] = target match {
    case QueryTarget.Node(qid) =>
      val stqid = SpaceTimeQuineId(qid, namespace, None)
      QuinePatternHelpers.createNodeActionSource { ref =>
        graph.relayTell(stqid, QuinePatternCommand.LoadNode(loadPlan.binding, ctx, ref))
        NotUsed
      }
    case QueryTarget.All => ???
    case QueryTarget.None => ???
  }

  def interpretById(
    idPlan: QueryPlan.SpecificId,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    QuinePatternExpressionInterpreter.eval(idPlan.idExp, graph.idProvider, ctx, parameters) match {
      case PatternValue.NodeId(qid) =>
        idPlan.nodeInstructions.foldLeft(Source.single(ctx))((combinedStream, nextPlan) =>
          combinedStream.flatMapConcat(ctx => interpret(nextPlan, QueryTarget.Node(qid), namespace, parameters, ctx)),
        )
      case _ => ???
    }

  def interpretProjection(
    project: QueryPlan.Project,
    target: QueryTarget,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] = target match {
    case _: QueryTarget.Node => throw new QuinePatternUnimplementedException("Projections on nodes")
    case QueryTarget.All => throw new QuinePatternUnimplementedException("Projections on all nodes")
    case QueryTarget.None =>
      val projectedValue = QuinePatternExpressionInterpreter.eval(project.projection, graph.idProvider, ctx, parameters)
      val newCtx = ctx + (project.as -> QuinePatternHelpers.patternValueToCypherValue(projectedValue))
      Source.single(newCtx)
  }

  def interpretUnit(ctx: QueryContext): Source[QueryContext, NotUsed] =
    Source.single(ctx)

  def interpretApply(
    product: QueryPlan.Product,
    target: QueryTarget,
    namespace: NamespaceId,
    parameters: Map[Symbol, Value],
    ctx: QueryContext,
  )(implicit graph: QuinePatternOpsGraph): Source[QueryContext, NotUsed] =
    interpret(product.left, target, namespace, parameters, ctx)
      .flatMapConcat(leftContext =>
        interpret(product.right, QueryTarget.None, namespace, parameters, leftContext).map(rightContext =>
          leftContext ++ rightContext,
        ),
      )

  def interpretAllNodeScan(
    scan: QueryPlan.AllNodeScan,
    target: QueryTarget,
    symbolToValue: Map[Symbol, Value],
  ): Source[QueryContext, NotUsed] =
    throw new QuinePatternUnimplementedException("AllNodeScan")
}

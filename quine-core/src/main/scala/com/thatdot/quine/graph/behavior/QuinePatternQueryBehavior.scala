package com.thatdot.quine.graph.behavior

import scala.collection.mutable

import org.apache.pekko.actor.Actor

import cats.implicits._

import com.thatdot.quine.graph.cypher.{BinOp, Expr, Parameters, Projection, QueryContext, QuinePattern, Value}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.{BaseNodeActor, RunningStandingQuery, StandingQueryId, StandingQueryPattern}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId, QuineValue}

object ExampleMessages {
  sealed trait QuinePatternMessages extends QuineMessage

  object QuinePatternMessages {
    case class RegisterPattern(quinePattern: QuinePattern, pid: StandingQueryId, reportTo: QuineId)
        extends QuinePatternMessages

    case class NewResults(results: List[QueryContext], pid: StandingQueryId) extends QuinePatternMessages
  }
}

trait QuinePatternQueryBehavior
    extends Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior {

  var output: Map[StandingQueryId, List[QueryContext]] = Map.empty

  var selfPatterns: Map[StandingQueryId, () => Unit] = Map.empty
  var edgePatterns: Map[StandingQueryId, HalfEdge => Unit] = Map.empty
  var resultPatterns: Map[StandingQueryId, List[QueryContext] => Unit] = Map.empty

  case class Effects(
    selfEffect: Map[StandingQueryId, () => Unit],
    edgeEffect: Map[StandingQueryId, HalfEdge => Unit],
    resultEffect: Map[StandingQueryId, List[QueryContext] => Unit]
  )

  def publishResults(id: StandingQueryId, results: List[QueryContext]): Unit =
    resultPatterns.get(id).foreach(f => f(results))

  def project(proj: Projection, results: List[QueryContext]): Option[List[QueryContext]] = proj match {
    case prop: Projection.Property =>
      results.traverse { qc =>
        for {
          pv <- qc.get(prop.of)
          vm <- pv match {
            case Expr.Map(map) => Some(map)
            case _ => Option.empty[mutable.SortedMap[String, Value]]
          }
          v <- vm.get(prop.name.name)
        } yield qc + (prop.as -> v)
      }
    case Projection.Calculation(expr, name) =>
      Some(results.map { qc =>
        val result = expr.eval(qc)(this.idProvider, Parameters.empty)
        qc + (name -> result)
      })
    case Projection.Serial(_) => ???
    case Projection.Parallel(subs) =>
      results.flatTraverse(qc =>
        subs.toList.foldM(List(qc)) { (result, p) =>
          project(p, result)
        }
      )
  }

  def patternToEffect(qp: QuinePattern, id: StandingQueryId, sendTo: Option[QuineId]): Effects = {
    qp match {
      case QuinePattern.QuineUnit =>
        Effects(
          selfEffect = Map(id -> (() => publishResults(id, List.empty[QueryContext]))),
          edgeEffect = Map(),
          resultEffect = Map()
        )
      case QuinePattern.Node(name) =>
        Effects(
          selfEffect = Map(id -> (() => {
            val propMap: Iterable[(String, QuineValue)] =
              properties.map(p => p._1.name -> p._2.deserialized.get)
            val qv: QuineValue = QuineValue.Map.apply(propMap)
            val result = List(QueryContext.empty + (name -> Expr.fromQuineValue(qv)))
            publishResults(id, result)
          })),
          edgeEffect = Map(),
          resultEffect = Map()
        )
      case QuinePattern.Edge(edge, pattern) =>
        Effects(
          selfEffect = Map(),
          edgeEffect = Map(id -> (he => {
            he.direction match {
              case EdgeDirection.Outgoing =>
                if (he.edgeType == edge) {
                  he.other ! ExampleMessages.QuinePatternMessages.RegisterPattern(pattern, id, this.qid)
                }
              case EdgeDirection.Incoming => ()
              case EdgeDirection.Undirected => ()
            }
          })),
          resultEffect = Map()
        )
      case QuinePattern.Fold(init, over, f, proj) =>
        val binOpEffect = (results: List[QueryContext]) => {
          val ids = (0 to over.size).map(n => StandingQueryId.fresh())
          val result = f match {
            case BinOp.Merge =>
              ids.foldRight(List.empty[QueryContext]) { (rid, fresult) =>
                val previous = output.getOrElse(rid, List.empty[QueryContext])
                if (previous.isEmpty) {
                  fresult
                } else if (fresult.isEmpty) {
                  previous
                } else {
                  for {
                    lr <- previous
                    rr <- fresult
                  } yield lr ++ rr
                }
              }
            case BinOp.Append =>
              ids.foldRight(List.empty[QueryContext]) { (rid, fresult) =>
                output.getOrElse(rid, List.empty[QueryContext]) ++ fresult
              }
          }
          val prevResult = output.getOrElse(id, List.empty[QueryContext])
          println("**************************************")
          println(s"Triggering binop $id on ${this.qid} for pattern $qp")
          println(s"Here are the part ids: $ids")
          println(s"Here's the local cache: $output")
          println(s"Old results were $prevResult")
          println(s"New results are $result")
          println("**************************************")
          if (result != prevResult) {
            output += (id -> result)
            sendTo match {
              case Some(subscriberId) => subscriberId ! ExampleMessages.QuinePatternMessages.NewResults(result, id)
              case None => ???
            }
          }
        }
        val initId = StandingQueryId.fresh()
        val initEffect = patternToEffect(init, initId, None)
        val initResultsEffect = (results: List[QueryContext]) => {
          val prevResult = output.getOrElse(initId, List.empty[QueryContext])
          if (results != prevResult) {
            output += initId -> results
            publishResults(id, results)
          }
        }
        val overEffects = over.zipWithIndex.map { case (qp, _) =>
          val effectId = StandingQueryId.fresh()
          val effect = patternToEffect(qp, effectId, None)
          val resultEffectFunc = (results: List[QueryContext]) => {
            val prevResult = output.getOrElse(effectId, List.empty[QueryContext])
            val newResult = proj.fold(results)(p => project(p, results).getOrElse(List.empty[QueryContext]))
            if (newResult != prevResult) {
              output += (effectId -> newResult)
              publishResults(id, newResult)
            }
          }
          val finalResultEffect = effect.resultEffect.get(effectId) match {
            case Some(oldEffect) =>
              (results: List[QueryContext]) => {
                oldEffect(results)
                val prevResult = output.getOrElse(effectId, List.empty[QueryContext])
                val newResult = proj.fold(results)(p => project(p, prevResult).getOrElse(List.empty[QueryContext]))
                if (newResult != prevResult) {
                  output += (effectId -> newResult)
                  publishResults(id, newResult)
                }
              }
            case None => resultEffectFunc
          }
          Effects(
            selfEffect = effect.selfEffect,
            edgeEffect = effect.edgeEffect,
            resultEffect = effect.resultEffect + (effectId -> finalResultEffect)
          )
        }
        val combinedOverEffects = overEffects.foldLeft(Effects(Map(), Map(), Map())) { (b, a) =>
          Effects(
            selfEffect = b.selfEffect ++ a.selfEffect,
            edgeEffect = b.edgeEffect ++ a.edgeEffect,
            resultEffect = b.resultEffect ++ a.resultEffect
          )
        }
        Effects(
          selfEffect = initEffect.selfEffect ++ combinedOverEffects.selfEffect,
          edgeEffect = initEffect.edgeEffect ++ combinedOverEffects.edgeEffect,
          resultEffect =
            ((initEffect.resultEffect ++ combinedOverEffects.resultEffect) + (initId -> initResultsEffect)) + (id -> binOpEffect)
        )
    }
  }

  def updateQuinePatternOnNode(qp: QuinePattern, id: StandingQueryId, sendTo: Option[QuineId] = None): Unit = {
    val effect = patternToEffect(qp, id, sendTo)

    selfPatterns = selfPatterns ++ effect.selfEffect
    edgePatterns = edgePatterns ++ effect.edgeEffect
    resultPatterns = resultPatterns ++ effect.resultEffect
  }

  def updateQuinePatternsOnNode(): Unit = {
    val runningStandingQueries = // Silently empty if namespace is absent.
      graph.standingQueries(namespace).fold(Map.empty[StandingQueryId, RunningStandingQuery])(_.runningStandingQueries)

    for {
      (sqId, runningSQ) <- runningStandingQueries
      query <- runningSQ.query.queryPattern match {
        case qp: StandingQueryPattern.QuinePatternQueryPattern => Some(qp.quinePattern)
        case _ => None
      }
    } updateQuinePatternOnNode(query, sqId, None)
  }

}

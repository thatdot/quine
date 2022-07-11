package com.thatdot.quine.gremlin

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util._
import scala.util.parsing.input.{Position, Positional}

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import akka.util.Timeout

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.LiteralOpsGraph
import com.thatdot.quine.model.{EdgeDirection, Milliseconds, PropertyValue, QuineId, QuineIdProvider}

// Functionality for describing and running queries
private[gremlin] trait GremlinTypes extends LazyLogging {

  implicit val graph: LiteralOpsGraph
  implicit val idProvider: QuineIdProvider = graph.idProvider

  val gremlinEc: ExecutionContext = graph.shardDispatcherEC
  implicit val t: Timeout

  private type AtTime = Option[Milliseconds]

  /** Gremlin query. Example: `x = [1,2,3,4]; g.V(x).out().has("property")`. */
  trait Query {

    /** Run a query on the underlying graph
      *
      * @param store variables in scope at the moment of evaluation
      * @param atTime moment in time to query
      * @return back-pressured source of results from running the query
      */
    def run(store: VariableStore, atTime: AtTime): Source[Any, NotUsed]
  }

  /** Gremlin query prefixed by a literal assignment to add to the context of the query
    *
    * @example `x = [1,2,3,4]; g.V(x)`
    * @param name the variable to be assigned, like `x` in the example
    * @param value the value of the variable, like `[1, 2, 3, 4]` in the example
    * @param `then` the query to run with the new context, like `g.V(x)` in the example
    */
  case class AssignLiteral(name: Symbol, value: GremlinExpression, `then`: Query) extends Query {
    override def run(context: VariableStore, atTime: AtTime): Source[Any, NotUsed] = {
      val evaled = value.evalTo[Any]("unexpected type error - hitting this constitutes a bug")(
        implicitly,
        context,
        idProvider
      )
      `then`.run(context + (name -> evaled), atTime)
    }
  }

  /** Gremlin query with all values fixed
    *
    * @example `g.V([1,2,3,4])`
    * @param traversal the [[Traversal]] this query instructs the queryrunner to perform
    */
  case class FinalTraversal(traversal: Traversal) extends Query {
    override def run(context: VariableStore, atTime: AtTime): Source[Any, NotUsed] =
      traversal.flow(context, atTime) match {
        case Failure(err) => Source.failed(err)
        case Success(flow) => Source.empty[Result].via(flow).map(_.unwrap)
      }
  }

  // Warn if there are inputs, since these are going to be completely ignored
  private def dropAndWarn(stepName: String): Flow[Result, Result, NotUsed] = Flow[Result]
    .statefulMapConcat { () =>
      var warned: Boolean = false

      {
        case Result(u, _, _) if !warned =>
          warned = true
          logger.warn(
            s"Gremlin query step: `$stepName` discarded a result (logged at INFO level) additional discards will not be logged."
          )
          logger.info(s"Gremlin query step: `$stepName` step discarded a result: $u")
          List.empty
        case _ => List.empty
      }
    }

  case class Traversal(steps: Seq[TraversalStep]) {
    def flow(implicit ctx: VariableStore, time: AtTime): Try[Flow[Result, Result, NotUsed]] =
      Try(steps.foldLeft(Flow[Result])((acc, step) => acc.via(step.flow.get)))

    /** Ideally, this roundtrips parsing. We can't guarantee that though.
      * See [[GremlinExpression.pprint]] for why.
      */
    def pprint: String = steps.map(_.pprint).mkString
  }

  /** Intermediate type that gets threaded through traversals
    *
    * @param unwrap result value
    * @param path vertices encounted in this traversal
    * @param matchContext traversal-level variables
    */
  case class Result(
    unwrap: Any,
    path: List[QuineId],
    matchContext: VariableStore
  )

  sealed abstract class TraversalStep extends Positional {

    /** Build a flow for the traversal step
      *
      * @param ctx    query-level variables
      * @param atTime moment in time to query
      * @returns a flow which transforms input results into output ones
      */
    def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]]

    /** Ideally, this roundtrips parsing. We can't guarantee that though.
      * See [[GremlinExpression.pprint]] for why.
      */
    def pprint: String
  }

  /** Implicitly enumerate every node
    */
  case object EmptyVertices extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = {
      val allNodes = graph
        .enumerateAllNodeIds()
        .mapMaterializedValue(_ => NotUsed)
        .map(qid => Result(Vertex(qid), List(qid), VariableStore.empty))

      // TODO: is this right? We discard matchcontext + path
      Success(dropAndWarn(pprint).concat(allNodes))
    }

    override val pprint = ".V()"
  }

  // Invariant: vertices is non-empty
  case class Vertices(vertices: Seq[GremlinExpression]) extends TraversalStep {
    require(vertices.nonEmpty, "Use EmptyVertices, not Vertices(..), when there are no arguments")

    override def flow(implicit c: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {

      // Parse an ID (either because it is already the right type, or by parsing it from a string)
      def parseId(something: Any, original: GremlinExpression): QuineId =
        something
          .castTo[idProvider.CustomIdType](
            s"`.V(...)` requires its arguments to be ids, but ${original.pprint} was not",
            pos = Some(original.pos)
          )(
            idProvider.customIdTag
          )
          .map(idProvider.customIdToQid)
          .recoverWith { case err: Throwable =>
            Option(something)
              .collect { case str: String => str }
              .flatMap(str => idProvider.qidFromPrettyString(str).toOption)
              .fold[Try[QuineId]](Failure(err))(Success(_))
          }
          .get

      // Ugly work around for the fact that `.vertices()` accepts an alternate form
      // where it has one argument which is an array
      val vertValues: Seq[QuineId] =
        Try {
          vertices(0)
            .evalTo[Vector[Any]]("`.V([...])` requires its argument to be an array")
            .map(parseId(_, vertices(0)))
        } getOrElse {
          vertices.map { v =>
            val e = v.evalTo[Any]("unexpected type error - hitting this constitutes a bug")
            parseId(e, v)
          }
        }

      // Drop inputs, fetch some nodes, and emit those
      dropAndWarn(".V(...)")
        .concat(
          Source(
            vertValues.view
              .map(qid => Result(Vertex(qid), List(qid), VariableStore.empty))
              .toList
          )
        )
    }

    override def pprint: String = vertices.map(_.pprint).mkString(".V(", ",", ")")
  }

  /** Ignore all inputs and output some sample of recently touched nodes.
    * Not standard Gremlin!!!
    *
    * @param limit maximum number of nodes to return
    */
  case class RecentVertices(limit: Option[GremlinExpression]) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {
      // Determine the limit
      val lim = limit.fold(100L) {
        _.evalTo[Long]("`.recentV(...)` requires its limit argument to be a long")
      }

      // Drop inputs, fetch some nodes, and emit those
      dropAndWarn(".recentV(...)")
        .concat(Source.futureSource {
          graph
            .recentNodes(lim.toInt, atTime)
            .map { (qidSet: Set[QuineId]) =>
              Source(
                qidSet.view
                  .map(qid =>
                    Result(
                      unwrap = Vertex(qid),
                      path = List(qid),
                      matchContext = VariableStore.empty
                    )
                  )
                  .toList
              )
            }(gremlinEc)
        })
    }

    override def pprint: String = s".recentV($limit)"
  }

  case class EqToVar(
    key: GremlinExpression
  ) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {
      // Determine the variable name
      val str = key.evalTo[String]("`.eqToVar(...)` requires its argument to be a string")
      val pos = key.pos
      val keySym = Symbol(str)

      Flow[Result]
        .filter { case Result(curr, _, m) => curr == m.get(keySym, pos) }
    }

    def pprint: String = ".eqToVar(" + key.pprint + ")"
  }

  sealed abstract class HasTests
  case object NoTest extends HasTests
  case object NegatedTest extends HasTests
  case class ValueTest(value: GremlinPredicateExpression) extends HasTests

  case class Has(
    key: GremlinExpression,
    hasRestriction: HasTests
  ) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {
      // Determine the property key name
      val keyStr = key.evalTo[String]("`.has(...)` requires its key argument to be a string")

      // TODO: find a way for this to _not_ deserialize the value for NoTest
      //       by changing type to `filterTest: (Lazy[Any] => Future[Boolean])`
      val filterTest: (Any => Try[Boolean] @unchecked) = hasRestriction match {
        case NoTest => (_: Any) => Success(true)
        case NegatedTest => (_: Any) => Success(false)
        case ValueTest(v) => (x: Any) => v.evalPredicate().testAgainst(x, Some(pos))
      }

      Flow[Result]
        .flatMapConcat { case r @ Result(u, _, _) =>
          val vert = u.castTo[Vertex]("`.has(...)` requires vertex inputs", Some(pos)).get

          // Let through the results which correspond to vertices with the property
          val propsFut = graph.literalOps.getProps(vert.id, atTime)

          Source.futureSource(propsFut.map { props =>
            val optValue = props.get(Symbol(keyStr)).map { (prop: PropertyValue) =>
              val deserialized = prop.deserialized.getOrElse {
                throw FailedDeserializationError(keyStr, prop.serialized, Some(pos))
              }
              deserialized.underlyingJvmValue
            }

            val keepThisVertex = optValue.headOption match {
              case None if hasRestriction == NegatedTest => true
              case None => false
              case Some(value) => filterTest(value).get
            }

            if (keepThisVertex)
              Source.single(r)
            else
              Source.empty
          }(gremlinEc))
        }
    }

    override def pprint: String =
      hasRestriction match {
        case NoTest => s"has(${key.pprint})"
        case NegatedTest => s"hasNot(${key.pprint})"
        case ValueTest(v) => s"has(${key.pprint},${v.pprint})"
      }
  }

  case class HasId(
    ids: Seq[GremlinExpression]
  ) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {

      // Determine the accepted IDs
      val qidSet = ids.view.map { (id: GremlinExpression) =>
        val cid = id.evalTo("`.hadId(...)` requires its arguments to be an ids")(
          idProvider.customIdTag,
          ctx,
          idProvider
        )
        idProvider.customIdToQid(cid)
      }.toSet

      Flow[Result]
        .filter { case Result(u, _, _) =>
          val vert = u.castTo[Vertex]("`.has(...)` requires vertex inputs", Some(pos)).get
          qidSet.contains(vert.id)
        }
    }

    override def pprint: String = ids.map(_.pprint).mkString(".hasId(", ",", ")")
  }

  sealed abstract class HopTypes
  case object OutOnly extends HopTypes
  case object InOnly extends HopTypes
  case object OutAndIn extends HopTypes

  // covers Out, In, Both, OutV, InV, BothV (basically any hop _from_ a vertex)
  case class HopFromVertex(
    edgeNames: Seq[GremlinExpression],
    dirRestriction: HopTypes,
    toVertex: Boolean, // as opposed as to edge
    limitOpt: Option[GremlinExpression]
  ) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {

      // Determine the valid outgoing edge names
      val edgeLbls: List[Symbol] = edgeNames.toList.map { (edgeName: GremlinExpression) =>
        Symbol(
          edgeName
            .evalTo[String](s"`.$name(...)` requires its arguments to be strings")
        )
      }

      // Determine the limit
      val lim = limitOpt.map {
        _.evalTo[Long](s"`.$name(...)` requires its limit to be a long")
      }

      // Filter based on edge direction
      val filterDirections: Option[EdgeDirection] = dirRestriction match {
        case OutAndIn => None
        case OutOnly => Some(EdgeDirection.Outgoing)
        case InOnly => Some(EdgeDirection.Incoming)
      }

      val flw = Flow[Result]
        .flatMapConcat { case Result(u, path, matchContext) =>
          val vert = u.castTo[Vertex](s"`.$name(...)` requires vertex inputs", Some(pos)).get

          // Get all of the edges connected to each vertex
          val edgesFut = if (edgeLbls.isEmpty) {
            // Get all edges
            graph.literalOps.getEdges(
              vert.id,
              withDir = filterDirections,
              withLimit = lim.map(_.toInt),
              atTime = atTime
            )
          } else {
            // Get edges for the labels we asked for
            Future
              .traverse(edgeLbls.toSet) { lbl =>
                graph.literalOps.getEdges(
                  vert.id,
                  withType = Some(lbl),
                  withDir = filterDirections,
                  withLimit = lim.map(_.toInt),
                  atTime = atTime
                )
              }(implicitly, gremlinEc)
              .map(_.flatten)(gremlinEc)
          }

          Source.futureSource(edgesFut.map { edges =>
            val edgeMap =
              edges.foldLeft(Map.empty[Symbol, List[(EdgeDirection, QuineId)]]) { (acc, e) =>
                val prevEdges = acc.getOrElse(e.edgeType, List.empty)
                acc + (e.edgeType -> ((e.direction, e.other) :: prevEdges))
              }

            // Filter these edges to match the label and be outgoing
            Source.apply(
              for {
                edgeLbl: Symbol <- if (edgeLbls.nonEmpty) edgeLbls else edgeMap.keys.toList
                (dir, otherQid) <- edgeMap.getOrElse(edgeLbl, List.empty)

                // What we return depends on whether we were asked for edges or vertices
                newU =
                  (
                    if (toVertex)
                      Vertex(otherQid)
                    else if (dir == EdgeDirection.Outgoing)
                      Edge(vert.id, edgeLbl, otherQid)
                    else
                      /* if (dir == EdgeDirection.Incoming) */
                      Edge(otherQid, edgeLbl, vert.id)
                  )

                // Only add to the result path if we go to a vertex
                newPath = if (toVertex) otherQid :: path else path
              } yield Result(newU, newPath, matchContext)
            )
          }(gremlinEc))
        }

      lim.fold(flw)(flw.take(_))
    }

    override def pprint: String = edgeNames.map(_.pprint).mkString(s".$name(", ",", ")")

    def name: String = dirRestriction match {
      case OutAndIn => "both" + (if (toVertex) "" else "E")
      case OutOnly => "out" + (if (toVertex) "" else "E")
      case InOnly => "in" + (if (toVertex) "" else "E")
    }
  }

  // covers OutV, InV, and BothV
  case class HopFromEdge(
    dirRestriction: HopTypes
  ) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      Flow[Result]
        .flatMapConcat { case Result(u, path, matchContext) =>
          val edge = u.castTo[Edge](s"`.$name()` requires edge inputs", Some(pos)).get

          // Get all of the edges connected to each vertex
          val endpoints = dirRestriction match {
            case OutAndIn => List(edge.toId, edge.fromId)
            case OutOnly => List(edge.fromId)
            case InOnly => List(edge.toId)
          }
          val newResults = endpoints.map { edgeEndpoint =>
            Result(Vertex(edgeEndpoint), edgeEndpoint :: path, matchContext)
          }

          Source.apply(newResults)
        }
    }

    override def pprint: String = s".$name()"

    def name: String = dirRestriction match {
      case OutAndIn => "bothV"
      case OutOnly => "outV"
      case InOnly => "inV"
    }
  }

  sealed abstract class LogicalConnective

  case class Not(negated: Traversal) extends LogicalConnective
  case class Where(test: Traversal) extends LogicalConnective
  case class And(conjunctees: Seq[Traversal]) extends LogicalConnective
  case class Or(conjunctees: Seq[Traversal]) extends LogicalConnective

  // covers And, Or, Not, Where
  case class Logical(kind: LogicalConnective) extends TraversalStep {

    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {

      // Produce a flow which passes through elements only if those elements
      // returned something when run through the `traversal`.
      def ifResults(
        traversal: Traversal,
        ifEmpty: Result => Source[Result, NotUsed],
        ifNotEmpty: Result => Source[Result, NotUsed]
      ): Flow[Result, Result, NotUsed] = {
        val traversalFlow = traversal.flow.get
        Flow[Result].flatMapConcat { case r =>
          Source
            .single(r)
            .via(traversalFlow)
            .take(1)
            .fold(ifEmpty(r))((_, _) => ifNotEmpty(r))
            .flatMapConcat(identity[Source[Result, NotUsed]])
        }
      }

      // Apply logical tests
      kind match {
        case Not(t) =>
          ifResults(
            traversal = t,
            ifEmpty = Source.single(_),
            ifNotEmpty = _ => Source.empty
          )

        case Where(t) =>
          ifResults(
            traversal = t,
            ifEmpty = _ => Source.empty,
            ifNotEmpty = Source.single(_)
          )

        // Note the short-circuiting which allows us to not run some traversals
        case And(ts) =>
          ts.foldRight(Flow[Result]) { case (t, acc) =>
            ifResults(
              traversal = t,
              ifEmpty = _ => Source.empty,
              ifNotEmpty = r => Source.single(r).via(acc)
            )
          }

        case Or(ts) =>
          ts.foldRight(Flow[Result].take(0)) { case (t, acc) =>
            ifResults(
              traversal = t,
              ifEmpty = r => Source.single(r).via(acc),
              ifNotEmpty = Source.single
            )
          }
      }
    }

    override def pprint: String = s"$name(${subs.map(_.pprint).mkString("_", ",_", "")})"

    def subs: Seq[Traversal] = kind match {
      case Not(t) => Seq(t)
      case Where(t) => Seq(t)
      case And(ts) => ts
      case Or(ts) => ts
    }

    def name: String = kind match {
      case Not(_) => "not"
      case Where(_) => "where"
      case And(_) => "and"
      case Or(_) => "or"
    }
  }

  case class Union(combined: Seq[Traversal]) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {
      val unionFlows = combined.map(_.flow.get)
      Flow[Result].flatMapConcat { (elem: Result) =>
        val elemSource = Source.single(elem)
        unionFlows
          .map(elemSource.via(_))
          .fold(Source.empty[Result])(_ ++ _)
      }
    }

    def pprint: String = s"union(${combined.map(_.pprint).mkString("_", ",_", "")})"

  }

  // covers Values, Valuemap
  case class Values(keys: Seq[GremlinExpression], groupResultsInMap: Boolean) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {

      // Determine the valid outgoing edge names
      val writtenKeyStrs: List[String] = keys.toList.map {
        _.evalTo[String](s"`.$name(...)` requires its arguments to be strings")
      }

      Flow[Result]
        .flatMapConcat { case Result(u, path, matchContext) =>
          val vert = u.castTo[Vertex](s"`.$name(...)` requires vertex inputs", Some(pos)).get

          Source.futureSource(
            graph.literalOps
              .getProps(vert.id, atTime)
              .map { props =>
                // If the user specifies no properties, that means get _all_ of them
                val keyStrs =
                  if (writtenKeyStrs.nonEmpty)
                    writtenKeyStrs
                  else
                    props.keys.map(_.name).toList

                val keyValues: List[(String, Any)] = for {
                  keyStr <- keyStrs
                  value <- props.get(Symbol(keyStr)).map { (prop: PropertyValue) =>
                    prop.deserialized.getOrElse {
                      throw FailedDeserializationError(keyStr, prop.serialized, Some(pos))
                    }
                  }
                } yield (keyStr -> value.underlyingJvmValue)

                // `valueMap` vs. `values` case
                if (groupResultsInMap) {
                  Source.single(Result(keyValues.toMap, path, matchContext))
                } else {
                  Source.apply(keyValues.map { case kv: (String, Any) =>
                    Result(kv._2, path, matchContext)
                  })
                }
              }(gremlinEc)
          )
        }
    }

    override def pprint: String = keys.map(_.pprint).mkString(s".$name(", ",", ")")

    def name: String =
      if (groupResultsInMap) "valueMap"
      else /* if (allowIn) */ "values"
  }

  /** Filter out inputs which aren't equal to the test value. If the test value
    * is a predicate function, run the predicate instead of comparing.
    *
    * @param testAgainst value against which to test inputs
    */
  case class Is(testAgainst: GremlinPredicateExpression) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      val pred = testAgainst.evalPredicate()

      // Run the predicate on each node
      Flow[Result]
        .filter { case Result(u, _, _) => pred.testAgainst(u, Some(pos)).get }
    }

    override def pprint: String = "is(" + testAgainst.pprint + ")"
  }

  /** Only emit an input value if it hasn't already been emitted
    */
  case object Dedup extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      Flow[Result]
        .statefulMapConcat { () =>
          val seen = mutable.Set.empty[Any]

          {
            case r @ Result(u, _, _) if seen.add(u) => List(r)
            case _ => List.empty
          }
        }
    }

    override val pprint = ".dedup()"
  }

  /** Add the current input value to the result context and output it
    *
    * @param key the string key under which the value should be added context
    */
  case class As(key: GremlinExpression) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {
      val keyStr = key.evalTo[String]("`.as(...)` requires its argument to be a string")
      val keySym = Symbol(keyStr)
      Flow[Result]
        .map { case Result(u, p, m) => Result(u, p, m + (keySym -> u)) }
    }

    override def pprint: String = ".as(" + key.pprint + ")"
  }

  /** Given a set of keys (which must be strings), look inside the context of
    * each input and extract the values at those indices and output them. If
    * there are multiple keys being selected, returns a map with all of their
    * values.
    *
    * @param keys keys to select
    */
  case class Select(keys: Seq[GremlinExpression]) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {

      // Determine the variable name(s)
      val keySymPoss: Seq[(Symbol, Position)] = keys.map { (key: GremlinExpression) =>
        val str = key.evalTo[String]("`.select(...)` requires its arguments to be strings")
        (Symbol(str), key.pos)
      }

      keySymPoss match {
        // If we have one variable, we return the value
        case Seq((key, pos)) =>
          Flow[Result].map { case Result(_, path, m) =>
            val extractedValue = m.get(key, pos)
            Result(extractedValue, path, m)
          }

        // Otherwise we return a `Map` of variable name to value
        case _ =>
          Flow[Result].map { case Result(_, path, m) =>
            val extractedValues: Map[String, Any] = keySymPoss.view.map { case (key, pos) =>
              key.name -> m.get(key, pos)
            }.toMap
            Result(extractedValues, path, m)
          }
      }
    }

    override def pprint: String = keys.map(_.pprint).mkString(".select(", ",", ")")
  }

  /** Emits only the first `num` inputs
    *
    * @param num maximum number of outputs
    */
  case class Limit(num: GremlinExpression) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Try[Flow[Result, Result, NotUsed]] = Try {
      val limitBy = num.evalTo[Long](
        "`.limit(...)` requires its argument to be a long"
      )
      Flow[Result].take(limitBy)
    }

    override def pprint: String = ".limit(" + num.pprint + ")"
  }

  /** Emit the ID of every input (requires all inputs to be nodes)
    */
  case class Id(stringOutput: Boolean) extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      Flow[Result]
        .mapConcat { case Result(u, path, matchContext) =>
          val vert = u.castTo[Vertex]("`.id()` requires vertex inputs", Some(pos)).get
          val cidOpt = if (stringOutput) {
            Some(idProvider.qidToPrettyString(vert.id))
          } else {
            idProvider.customIdFromQid(vert.id).toOption
          }
          cidOpt match {
            case Some(cid) => List(Result(cid, path, matchContext))
            case None => Nil
          }
        }
    }

    override val pprint: String = if (stringOutput) ".strId()" else ".id()"
  }

  /** For every input, emits all of the nodes that were traversed getting to
    * that input
    */
  case object UnrollPath extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      Flow[Result]
        .flatMapConcat { case Result(_, path, matchContext) =>
          Source
            .apply(path.view.reverse.toList)
            .map((qId: QuineId) => Result(Vertex(qId), path, matchContext))
        }
    }

    override val pprint = ".unrollPath()"
  }

  /** Groups its inputs and emits one output at the end: a map from each
    * distinct input to the total number of occurences of that input
    */
  case object GroupCount extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      Flow[Result]
        .fold(Map.empty[Any, Long]) { case (seenCounts, Result(u, _, _)) =>
          seenCounts + (u -> (1 + seenCounts.getOrElse(u, 0L)))
        }
        .map { counts =>
          Result(counts, List.empty, VariableStore.empty)
        }
    }

    override val pprint = ".groupCount()"
  }

  /** Counts all of its inputs and emits one output at the end: the total
    */
  case object Count extends TraversalStep {
    override def flow(implicit ctx: VariableStore, atTime: AtTime): Success[Flow[Result, Result, NotUsed]] = Success {
      Flow[Result]
        .fold(0)((counter, _) => counter + 1)
        .map { len =>
          Result(len, List.empty, VariableStore.empty)
        }
    }

    override val pprint = ".count()"
  }
}

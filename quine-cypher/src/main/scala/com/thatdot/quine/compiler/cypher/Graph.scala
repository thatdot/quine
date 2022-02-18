package com.thatdot.quine.compiler.cypher

import scala.collection.mutable

import cats.Endo
import cats.implicits._
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions.{LogicalVariable, Range, RelationshipPattern}
import org.opencypher.v9_0.util.NodeNameGenerator

import com.thatdot.quine.graph.cypher
import com.thatdot.quine.model.EdgeDirection

/** Represents a pattern graph like what you would find in a `MATCH` or `CREATE`
  * clause. Mostly a canonical representation...
  *
  * This deserves its own type because compiling it requires making some
  * semi-arbitrary decisions about where to start and what to do next.
  *
  * TODO: should hints go here?
  * TODO: add clauses that mutate this graph here too (that way we can execute
  *       them while on the relevant node)
  *
  * @param nodes all of the node patterns in the graph
  * @param relationships all of the relationship patterns in the graph
  * @param nameParts named pattern components
  */
final case class Graph(
  nodes: Map[expressions.LogicalVariable, expressions.NodePattern],
  relationships: Set[Relationship],
  namedParts: Map[expressions.Variable, expressions.AnonymousPatternPart]
) {

  /** Synthesize a fetch query
    *
    * TODO: I think this might be easier to think about as turning a graph into
    *       a tree. Do that instead.
    *
    * @param freeConstraints constraints on the graph pattern which reference
    *        variables that are not yet in scope (but should come in scope as
    *        more of the graph gets matched).
    * @return a query which fetches the pattern from the graph
    */
  def synthesizeFetch(
    freeConstraints: WithFreeVariables[expressions.LogicalVariable, expressions.Expression]
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = for {

    // If possible, we want to jump straight to an anchor node
    scopeInfo <- CompM.getQueryScopeInfo
    nodesOfInterest = nodes.keys ++ relationships.flatMap(_.endpoints)
    anchorNode = nodesOfInterest.collectFirst {
      Function.unlift(lv => scopeInfo.getAnchor(lv).map(lv -> _))
    }

    fetchQuery <- anchorNode match {

      /* There is a starting spot */
      case Some((otherNodeLv, otherNodeAnchorExpr)) =>
        for {
          andThen <- synthesizeFetchOnNode(otherNodeLv, freeConstraints)
          enterAndThen = cypher.Query.ArgumentEntry(otherNodeAnchorExpr, andThen)
        } yield enterAndThen

      /* Base case: we are done! */
      case None if nodes.isEmpty =>
        assert(relationships.isEmpty, s"Relationship(s) not yet visited: $relationships")
        assert(freeConstraints.isEmpty, s"Constraint(s) not yet used: $freeConstraints")
        CompM.pure[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit())

      /* Then, the next best thing is to find a node with a label, and scan that.
       *
       * TODO: choose this based on sizes of label sets
       * TODO: implement this
       */

      /* Finally, the fallback position is to pick a node and try it against every
       * node in the DB.
       */
      case None =>
        val (nodeLv, _) = nodes.head
        for {
          andThen <- synthesizeFetchOnNode(nodeLv, freeConstraints)
          enterAndThen = cypher.Query.AnchoredEntry(cypher.EntryPoint.AllNodesScan, andThen)
        } yield enterAndThen
    }
  } yield fetchQuery

  /** Like [[synthesizeFetch]] but starts already in the graph
    *
    * @param atNode starting position in the graph
    * @param freeConstraints constraints on the graph pattern
    */
  def synthesizeFetchOnNode(
    atNode: expressions.LogicalVariable,
    freeConstraints: WithFreeVariables[expressions.LogicalVariable, expressions.Expression]
  ): CompM[cypher.Query[cypher.Location.OnNode]] = for {
    scopeInfo: QueryScopeInfo <- CompM.getQueryScopeInfo

    /* If we haven't visited the node, we need to add it to the context.
     *
     * If we've already visited this node, then it will be in the context (and
     * we need to check that this really is the same node).
     */
    (returnQuery, constraints1, remainingNodes) <- (
      scopeInfo.getVariable(atNode),
      nodes.get(atNode)
    ) match {

      case (Some(cypherVar), None) =>
        val tempBindName = NodeNameGenerator.name(atNode.position.bumped())
        val tempLv = expressions.Variable(tempBindName)(atNode.position)

        for {
          tempVarExpr <- CompM.addColumn(tempLv)
          localNode = cypher.Query.LocalNode(
            labelsOpt = None,
            propertiesOpt = None,
            bindName = Some(tempVarExpr.id)
          )
          filter = cypher.Query.filter(
            condition = cypher.Expr.Equal(
              cypher.Expr.Function(cypher.Func.Id, Vector(cypherVar)),
              cypher.Expr.Function(cypher.Func.Id, Vector(tempVarExpr))
            ),
            toFilter = cypher.Query.Unit()
          )
          returnQuery = (cont: cypher.Query[cypher.Location.OnNode]) => {
            cypher.Query.apply(cypher.Query.apply(localNode, filter), cont)
          }
        } yield (returnQuery, freeConstraints, nodes)

      case (cypherVarOpt, Some(nodePat)) =>
        for {
          nodeWQ <- nodePat.properties match {
            case None => CompM.pure(WithQuery(None))
            case Some(p) => Expression.compileM(p).map(_.map(Some(_)))
          }

          // Avoid re-aliasing something that is already aliased
          bindName <-
            if (cypherVarOpt.isDefined) {
              CompM.pure(None)
            } else {
              CompM.addColumn(atNode).map(v => Some(v.id))
            }

          // Find and apply any predicates that are now closed
          (closedConstraints, newFreeConstraints) = freeConstraints.bindVariable(atNode)
          constraintsWQ <- closedConstraints
            .traverse[WithQueryT[CompM, *], cypher.Expr](Expression.compile(_))
            .map(constraints => cypher.Expr.And(constraints.toVector))
            .runWithQuery

          labelsOpt =
            if (nodePat.labels.isEmpty) {
              None
            } else {
              Some(nodePat.labels.map(v => Symbol(v.name)))
            }
          localNode = nodeWQ.toNodeQuery { (props: Option[cypher.Expr]) =>
            cypher.Query.LocalNode(
              labelsOpt,
              propertiesOpt = props,
              bindName
            )
          }
          returnQuery = (cont: cypher.Query[cypher.Location.OnNode]) => {
            cypher.Query.apply(
              cypher.Query.apply(
                localNode,
                constraintsWQ.toNodeQuery(cypher.Query.filter(_, cypher.Query.Unit()))
              ),
              cont
            )
          }
        } yield (returnQuery, newFreeConstraints, nodes - atNode)

      case other =>
        CompM.raiseCompileError(s"Bug: node should either be in context or in graph: $other", atNode)
    }

    /* If we find an edge that is connected to the current node, traverse
     * that edge and recurse with the node on the other side.
     */
    connectedToNode: Option[(LogicalVariable, RelationshipPattern, Set[Relationship])] = relationships.view
      .collectFirst {
        case r @ Relationship(from, to, relPat) if from == atNode =>
          (to, relPat, relationships - r)

        case r @ Relationship(from, to, relPat) if to == atNode =>
          val adjustedRelPat = relPat.copy(direction = relPat.direction.reversed)(relPat.position)
          (from, adjustedRelPat, relationships - r)
      }

    remainingQuery: cypher.Query[cypher.Location.OnNode] <- connectedToNode match {
      case None =>
        /* If we've gotten this far, we've gotten through the connected component
         * of the query. It is time to find another entry point for the rest of the
         * query.
         */
        Graph(remainingNodes, relationships, namedParts)
          .synthesizeFetch(constraints1)
          .map(q => q: cypher.Query[cypher.Location.OnNode])

      case Some((otherNodeLv, rel, remainingEdges)) =>
        val edgeName = if (rel.types.isEmpty) {
          None
        } else {
          Some(rel.types.map(v => Symbol(v.name)))
        }

        val direction = rel.direction match {
          case expressions.SemanticDirection.OUTGOING => EdgeDirection.Outgoing
          case expressions.SemanticDirection.INCOMING => EdgeDirection.Incoming
          case expressions.SemanticDirection.BOTH => EdgeDirection.Undirected
        }
        val otherNode = scopeInfo.getAnchor(otherNodeLv)

        /* TODO: when `otherNode` is filled in, we can be more efficient with
         *       `synthesizeFetchOnNode` (no need to start by checking if the
         *       node is the one we want - we know it is the one we want since
         *       we hopped straight to it!)
         */
        for {
          bindRelation: Option[Symbol] <- rel.variable match {
            case None => CompM.pure(None)
            case Some(lv) => CompM.addColumn(lv).map(v => Some(v.id))
          }

          // Find and apply any predicates that are now closed
          (closedConstraints, constraints2) = rel.variable match {
            case None => (Nil, constraints1)
            case Some(lv) => constraints1.bindVariable(lv)
          }
          constraintsWQ: WithQuery[cypher.Expr] <- closedConstraints
            .traverse[WithQueryT[CompM, *], cypher.Expr](Expression.compile(_))
            .map(constraints => cypher.Expr.And(constraints.toVector))
            .runWithQuery

          range: Option[(Option[Long], Option[Long])] = rel.length match {
            case None => None
            case Some(None) => Some((Some(1L), None)) // support input ()-[*]-() as shorthand for 1 or more
            case Some(Some(Range(lower, upper))) =>
              Some((lower.map(_.value), upper.map(_.value)))
          }

          remainingGraph = Graph(remainingNodes, remainingEdges, namedParts)
          andThen <- remainingGraph.synthesizeFetchOnNode(otherNodeLv, constraints2)
        } yield cypher.Query.Expand(
          edgeName,
          toNode = otherNode,
          direction,
          bindRelation,
          range,
          cypher.VisitedVariableEdgeMatches.empty,
          constraintsWQ.toNodeQuery(cypher.Query.filter(_, andThen))
        )
    }
  } yield returnQuery(remainingQuery)

  /** Synthesize a create query
    *
    * @return a query which synthesizes the pattern in the graph
    */
  def synthesizeCreate: CompM[cypher.Query[cypher.Location.Anywhere]] = for {

    // If possible, we want to jump straight to an anchor node
    scopeInfo <- CompM.getQueryScopeInfo
    nodesOfInterest = nodes.keys ++ relationships.flatMap(_.endpoints)
    anchorNode = nodesOfInterest.collectFirst {
      Function.unlift(lv => scopeInfo.getAnchor(lv).map(lv -> _))
    }

    createQuery <- anchorNode match {

      /* There is a starting spot */
      case Some((otherNodeLv, otherNodeAnchorExpr)) =>
        for {
          andThen <- synthesizeCreateOnNode(otherNodeLv)
          enterAndThen = cypher.Query.ArgumentEntry(otherNodeAnchorExpr, andThen)
        } yield enterAndThen

      /* Base case: we are done! */
      case None if nodes.isEmpty =>
        assert(relationships.isEmpty, s"Relationship(s) not yet visited: $relationships")
        CompM.pure[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit())

      /* If we've not found the node, the time has come to create it! */
      case None =>
        val (nodeLv, _) = nodes.head
        for {
          andThen <- synthesizeCreateOnNode(nodeLv)
          createAndThen = cypher.Query.ArgumentEntry(cypher.Expr.FreshNodeId, andThen)
        } yield createAndThen
    }
  } yield createQuery

  /** Like [[synthesizeCreate]] but starts already in the graph */
  def synthesizeCreateOnNode(
    atNode: expressions.LogicalVariable
  ): CompM[cypher.Query[cypher.Location.OnNode]] = for {
    scopeInfo <- CompM.getQueryScopeInfo

    /* If we haven't visited the node, we need to set/create its properties and
     * add it to the context.
     */
    (returnQueryM, remainingNodes) = (scopeInfo.getVariable(atNode), nodes.get(atNode)) match {
      // Avoid re-creating a node (since it is already aliased)
      case (Some(cypherVar @ _), nodePatOpt @ _) =>
        val returnQuery = identity[cypher.Query[cypher.Location.OnNode]](_)
        val newNodes = if (nodePatOpt.isDefined) nodes - atNode else nodes
        (CompM.pure(returnQuery), newNodes)

      case (None, Some(nodePat)) =>
        val labelsOpt = if (nodePat.labels.isEmpty) {
          None
        } else {
          Some(nodePat.labels.map(v => Symbol(v.name)))
        }

        val returnQueryM: CompM[Endo[cypher.Query[cypher.Location.OnNode]]] = for {
          nodeWC <- nodePat.properties match {
            case None => CompM.pure(WithQuery(None))
            case Some(p) => Expression.compileM(p).map(_.map(Some(_)))
          }
          atNodeExpr <- CompM.addColumn(atNode)

          localNode = cypher.Query.LocalNode(
            labelsOpt = None,
            propertiesOpt = None,
            bindName = Some(atNodeExpr.id)
          )
          setData = nodeWC.toNodeQuery { (props: Option[cypher.Expr]) =>
            val setProps = cypher.Query.SetProperties(
              properties = props.getOrElse(cypher.Expr.Map.empty),
              includeExisting = true
            )
            val setLabels = labelsOpt match {
              case Some(lbls) => cypher.Query.SetLabels(lbls, add = true)
              case None => cypher.Query.Unit()
            }
            cypher.Query.apply(setProps, setLabels)
          }
        } yield (cont: cypher.Query[cypher.Location.OnNode]) =>
          cypher.Query.apply(setData, cypher.Query.apply(localNode, cont))

        (returnQueryM, nodes - atNode)

      case other =>
        (
          CompM.raiseCompileError[cypher.Query[cypher.Location.OnNode] => cypher.Query[cypher.Location.OnNode]](
            s"Bug: node should either be in context or in graph: $other",
            atNode
          ),
          nodes
        )
    }
    returnQuery <- returnQueryM

    /* If we find an edge that is connected to the current node, traverse
     * that edge and recurse with the node on the other side.
     */
    connectedToNode = relationships.view.collectFirst {
      case r @ Relationship(from, to, relPat) if from == atNode =>
        (to, relPat, relationships - r)

      case r @ Relationship(from, to, relPat) if to == atNode =>
        val adjustedRelPat = relPat.copy(direction = relPat.direction.reversed)(relPat.position)
        (from, adjustedRelPat, relationships - r)
    }

    remainingQuery: cypher.Query[cypher.Location.OnNode] <- connectedToNode match {
      case None =>
        /* If we've gotten this far, we've gotten through the connected component
         * of the query. It is time to find another entry point for the rest of the
         * query.
         */
        Graph(remainingNodes, relationships, namedParts).synthesizeCreate.map(q => q)

      case Some((otherNodeLv, rel, remainingEdges)) =>
        // Find an expression for the other node either in the context, or as a fresh node
        val otherNode = scopeInfo.getAnchor(otherNodeLv).getOrElse(cypher.Expr.FreshNodeId)

        for {
          direction: EdgeDirection <- rel.direction match {
            case expressions.SemanticDirection.OUTGOING => CompM.pure(EdgeDirection.Outgoing)
            case expressions.SemanticDirection.INCOMING => CompM.pure(EdgeDirection.Incoming)
            case expressions.SemanticDirection.BOTH =>
              CompM.raiseCompileError("Cannot create undirected relationship", rel)
          }

          edgeName: Symbol <- rel.types match {
            case List(edge) => CompM.pure(Symbol(edge.name))
            case labels =>
              CompM.raiseCompileError(
                s"Edges must be created with exactly one label (got ${labels.map(_.name).mkString(", ")})",
                rel
              )
          }

          () <-
            if (rel.properties.nonEmpty) {
              CompM.raiseCompileError("Properties on edges are not yet supported", rel)
            } else {
              CompM.pure(())
            }

          bindRelation <- rel.variable match {
            case None => CompM.pure(None)
            case Some(lv) => CompM.addColumn(lv).map(v => Some(v.id))
          }
          remainingGraph = Graph(remainingNodes, remainingEdges, namedParts)
          andThen <- remainingGraph.synthesizeCreateOnNode(otherNodeLv)
        } yield cypher.Query.SetEdge(
          edgeName,
          direction,
          bindRelation,
          otherNode,
          add = true,
          andThen
        )
    }
  } yield returnQuery(remainingQuery)
}

object Graph {

  /** Construct a graph from a pattern */
  def fromPattern(pattern: expressions.Pattern): CompM[Graph] = {
    val nodes = mutable.Map.empty[expressions.LogicalVariable, expressions.NodePattern]
    val relationships = Set.newBuilder[Relationship]
    val namedParts = Map.newBuilder[expressions.Variable, expressions.AnonymousPatternPart]

    def addNodePattern(
      nodeVar: expressions.LogicalVariable,
      nodePat: expressions.NodePattern
    ): Unit = nodes.get(nodeVar) match {
      // This is the first time we see the variable, so define it
      case None =>
        nodes += nodeVar -> nodePat

      // The variable has already been defined
      case Some(_) =>
        assert(nodePat.labels.isEmpty && nodePat.properties.isEmpty, s"Variable `$nodeVar` is already defined")
    }

    /* Add to `nodes`, `relationships`, and `namedParts` builders. */
    def visitPatternPart(pat: expressions.PatternPart)(implicit source: cypher.SourceText): Unit = pat match {
      case expressions.NamedPatternPart(v, anonPat) =>
        namedParts += v -> anonPat
        visitPatternPart(anonPat)

      case expressions.EveryPath(patElem) =>
        val _ = visitPatternElement(patElem)

      case pat: expressions.ShortestPaths =>
        throw cypher.CypherException.Compile(
          "`shortestPath` planning in graph patterns is not supported",
          Some(position(pat.position))
        )
    }

    /* Add to `nodes` and `relationships` builders.
     *
     * @return the rightmost node variable
     */
    def visitPatternElement(
      pat: expressions.PatternElement
    )(implicit
      source: cypher.SourceText
    ): expressions.LogicalVariable =
      pat match {
        case expressions.RelationshipChain(elem, rel, rightNode) =>
          val leftNodeVar = visitPatternElement(elem)
          val rightNodeVar = rightNode.variable.get // variable is filled in by semantic analysis
          relationships += Relationship(leftNodeVar, rightNodeVar, rel)
          addNodePattern(rightNodeVar, rightNode)
          rightNodeVar

        // TODO: what is the base node?
        case nodePat @ expressions.NodePattern(nodeVarOpt, _, _, _) =>
          val nodeVar = nodeVarOpt.get //  variable is filled in by semantic analysis
          addNodePattern(nodeVar, nodePat)
          nodeVar
      }

    CompM.getSourceText.flatMap[Graph] { implicit sourceText =>
      try {
        pattern.patternParts.foreach(visitPatternPart)
        CompM.pure(Graph(nodes.toMap, relationships.result(), namedParts.result()))
      } catch {
        case err: cypher.CypherException.Compile => CompM.raiseError(err)
      }
    }
  }
}

/** Wrapper around [[expressions.RelationshipPattern]], but including the
  * variables of the endpoints.
  *
  * @param start variable on the LHS of the relationship
  * @param end variable on the RHS of the relationship
  * @param relationshipPattern pattern for the relationship
  */
final case class Relationship(
  start: expressions.LogicalVariable,
  end: expressions.LogicalVariable,
  relationshipPattern: expressions.RelationshipPattern
) {
  def endpoints: List[expressions.LogicalVariable] = List(start, end)
}

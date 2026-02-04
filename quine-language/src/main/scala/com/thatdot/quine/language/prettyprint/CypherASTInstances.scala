package com.thatdot.quine.language.prettyprint

import com.thatdot.quine.cypher.ast._
import com.thatdot.quine.language.ast.{Operation, Projection => LangProjection, QueryDescription}

trait CypherASTInstances extends ASTInstances {
  import Doc._

  implicit lazy val nodePatternPrettyPrint: PrettyPrint[NodePattern] =
    PrettyPrint.instance { np =>
      val binding = np.maybeBinding match {
        case Some(id) => identifierEitherPrettyPrint.doc(id)
        case None => empty
      }
      val labels =
        if (np.labels.isEmpty) empty
        else concat(text(":"), text(np.labels.map(_.name).mkString(":")))
      val props = np.maybeProperties match {
        case Some(p) => concat(text(" "), expressionPrettyPrint.doc(p))
        case None => empty
      }
      val sourceDoc = sourcePrettyPrint.doc(np.source)
      concat(text("("), binding, labels, props, text(")"), text(" "), sourceDoc)
    }

  implicit lazy val edgePatternPrettyPrint: PrettyPrint[EdgePattern] =
    PrettyPrint.instance { ep =>
      val binding = ep.maybeBinding match {
        case Some(id) => identifierEitherPrettyPrint.doc(id)
        case None => empty
      }
      val edgeType = concat(text(":"), text(ep.edgeType.name))
      val arrow = ep.direction match {
        case com.thatdot.quine.language.ast.Direction.Left => (text("<-["), text("]-"))
        case com.thatdot.quine.language.ast.Direction.Right => (text("-["), text("]->"))
      }
      val sourceDoc = sourcePrettyPrint.doc(ep.source)
      concat(arrow._1, binding, edgeType, arrow._2, text(" "), sourceDoc)
    }

  implicit lazy val connectionPrettyPrint: PrettyPrint[Connection] =
    PrettyPrint.instance { conn =>
      concat(text(" "), edgePatternPrettyPrint.doc(conn.edge), text(" "), nodePatternPrettyPrint.doc(conn.dest))
    }

  implicit lazy val graphPatternPrettyPrint: PrettyPrint[GraphPattern] =
    PrettyPrint.instance { gp =>
      val pathDocs = gp.path.map(connectionPrettyPrint.doc)
      // Only emit the GraphPattern's own source annotation when the pattern has connections
      // (e.g. (n)-[r]->(m)), since it then spans a wider range than any individual child.
      // For single-node patterns (e.g. (n:Person)), the GraphPattern source is identical to
      // the NodePattern source, so showing both would produce a duplicate like "@[6-15] @[6-15]".
      val sourceDoc =
        if (gp.path.nonEmpty) concat(text(" "), sourcePrettyPrint.doc(gp.source))
        else empty
      concat(
        nodePatternPrettyPrint.doc(gp.initial),
        intercalate(empty, pathDocs),
        sourceDoc,
      )
    }

  implicit lazy val projectionCypherPrettyPrint: PrettyPrint[Projection] =
    PrettyPrint.instance { proj =>
      val sourceDoc = sourcePrettyPrint.doc(proj.source)
      concat(
        expressionPrettyPrint.doc(proj.expression),
        text(" AS "),
        identifierEitherPrettyPrint.doc(proj.as),
        text(" "),
        sourceDoc,
      )
    }

  implicit lazy val effectPrettyPrint: PrettyPrint[Effect] =
    PrettyPrint.instance {
      case Effect.Foreach(source, binding, in, effects) =>
        val effectDocs = effects.map(effectPrettyPrint.doc)
        concat(
          text("FOREACH("),
          nest(
            1,
            concat(
              line,
              text("binding = "),
              symbolPrettyPrint.doc(binding),
              text(","),
              line,
              text("in = "),
              expressionPrettyPrint.doc(in),
              text(","),
              line,
              text("effects = ["),
              nest(1, concat(line, intercalate(concat(text(","), line), effectDocs))),
              line,
              text("]"),
            ),
          ),
          line,
          text(")"),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case Effect.SetProperty(source, property, value) =>
        concat(
          text("SET "),
          expressionPrettyPrint.doc(property),
          text(" = "),
          expressionPrettyPrint.doc(value),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case Effect.SetProperties(source, of, properties) =>
        concat(
          text("SET "),
          identifierEitherPrettyPrint.doc(of),
          text(" = "),
          expressionPrettyPrint.doc(properties),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case Effect.SetLabel(source, on, labels) =>
        concat(
          text("SET "),
          identifierEitherPrettyPrint.doc(on),
          text(":"),
          text(labels.map(_.name).mkString(":")),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case Effect.Create(source, patterns) =>
        val patternDocs = patterns.map(graphPatternPrettyPrint.doc)
        concat(
          text("CREATE ["),
          nest(1, concat(line, intercalate(concat(text(","), line), patternDocs))),
          line,
          text("]"),
          text(" "),
          sourcePrettyPrint.doc(source),
        )
    }

  implicit lazy val readingClausePrettyPrint: PrettyPrint[ReadingClause] =
    PrettyPrint.instance {
      case ReadingClause.FromPatterns(source, patterns, maybePredicate) =>
        val patternDocs = patterns.map(graphPatternPrettyPrint.doc)
        val predDoc = maybePredicate match {
          case Some(pred) => concat(line, text("WHERE "), expressionPrettyPrint.doc(pred))
          case None => empty
        }
        concat(
          text("MATCH ["),
          nest(1, concat(line, intercalate(concat(text(","), line), patternDocs))),
          line,
          text("]"),
          predDoc,
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case ReadingClause.FromUnwind(source, list, as) =>
        concat(
          text("UNWIND "),
          expressionPrettyPrint.doc(list),
          text(" AS "),
          identifierEitherPrettyPrint.doc(as),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case ReadingClause.FromProcedure(source, name, args, yields) =>
        val argDocs = args.map(expressionPrettyPrint.doc)
        val yieldDocs = yields.map(yieldItemPrettyPrint.doc)
        concat(
          text("CALL "),
          text(name.name),
          text("("),
          intercalate(text(", "), argDocs),
          text(")"),
          if (yields.nonEmpty) concat(text(" YIELD "), intercalate(text(", "), yieldDocs))
          else empty,
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case ReadingClause.FromSubquery(source, bindings, subquery) =>
        val bindingDocs = bindings.map(identifierEitherPrettyPrint.doc)
        concat(
          text("CALL {"),
          nest(1, concat(line, queryPrettyPrint.doc(subquery))),
          line,
          text("} WITH "),
          intercalate(text(", "), bindingDocs),
          text(" "),
          sourcePrettyPrint.doc(source),
        )
    }

  implicit lazy val withClausePrettyPrint: PrettyPrint[WithClause] =
    PrettyPrint.instance { wc =>
      val distinctDoc = if (wc.isDistinct) text("DISTINCT ") else empty
      val wildcardDoc = if (wc.hasWildCard) text("*, ") else empty
      val bindingDocs = wc.bindings.map(projectionCypherPrettyPrint.doc)
      val predDoc = wc.maybePredicate match {
        case Some(pred) => concat(text(" WHERE "), expressionPrettyPrint.doc(pred))
        case None => empty
      }
      concat(
        text("WITH "),
        distinctDoc,
        wildcardDoc,
        intercalate(text(", "), bindingDocs),
        predDoc,
        text(" "),
        sourcePrettyPrint.doc(wc.source),
      )
    }

  implicit lazy val queryPartPrettyPrint: PrettyPrint[QueryPart] =
    PrettyPrint.instance {
      case QueryPart.ReadingClausePart(rc) => readingClausePrettyPrint.doc(rc)
      case QueryPart.WithClausePart(wc) => withClausePrettyPrint.doc(wc)
      case QueryPart.EffectPart(eff) => effectPrettyPrint.doc(eff)
    }

  implicit lazy val queryPrettyPrint: PrettyPrint[Query] =
    PrettyPrint.instance {
      case Query.Union(source, lhs, rhs) =>
        concat(
          queryPrettyPrint.doc(lhs),
          line,
          text("UNION"),
          line,
          queryPrettyPrint.doc(rhs),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case Query.SingleQuery.MultipartQuery(source, queryParts, into) =>
        val partDocs = queryParts.map(queryPartPrettyPrint.doc)
        concat(
          text("MultipartQuery("),
          nest(
            1,
            concat(
              line,
              text("parts = ["),
              nest(1, concat(line, intercalate(concat(text(","), line), partDocs))),
              line,
              text("],"),
              line,
              text("into = "),
              queryPrettyPrint.doc(into),
            ),
          ),
          line,
          text(")"),
          text(" "),
          sourcePrettyPrint.doc(source),
        )

      case Query.SingleQuery.SinglepartQuery(source, queryParts, hasWildcard, isDistinct, bindings) =>
        val partDocs = queryParts.map(queryPartPrettyPrint.doc)
        val bindingDocs = bindings.map(projectionCypherPrettyPrint.doc)
        val wildcardDoc = if (hasWildcard) text("*, ") else empty
        val distinctDoc = if (isDistinct) text("DISTINCT ") else empty
        concat(
          text("SinglepartQuery("),
          nest(
            1,
            concat(
              line,
              text("parts = ["),
              nest(1, concat(line, intercalate(concat(text(","), line), partDocs))),
              line,
              text("],"),
              line,
              text("bindings = ["),
              distinctDoc,
              wildcardDoc,
              nest(1, concat(line, intercalate(concat(text(","), line), bindingDocs))),
              line,
              text("]"),
            ),
          ),
          line,
          text(")"),
          text(" "),
          sourcePrettyPrint.doc(source),
        )
    }
  implicit lazy val yieldItemPrettyPrint: PrettyPrint[YieldItem] =
    PrettyPrint.instance { yi =>
      concat(symbolPrettyPrint.doc(yi.resultField), text(" AS "), identifierEitherPrettyPrint.doc(yi.boundAs))
    }

  implicit lazy val queryDescriptionPrettyPrint: PrettyPrint[QueryDescription] =
    PrettyPrint.instance { qd =>
      concat(
        text("QueryDescription("),
        nest(
          1,
          concat(
            line,
            text("graphPatterns = "),
            listPrettyPrint[GraphPattern].doc(qd.graphPatterns),
            text(","),
            line,
            text("nodePatterns = "),
            listPrettyPrint[NodePattern].doc(qd.nodePatterns),
            text(","),
            line,
            text("constraints = "),
            listPrettyPrint[com.thatdot.quine.language.ast.Expression].doc(qd.constraints),
            text(","),
            line,
            text("operations = "),
            listPrettyPrint[Operation].doc(qd.operations),
            text(","),
            line,
            text("projections = "),
            listPrettyPrint[LangProjection].doc(qd.projections),
          ),
        ),
        line,
        text(")"),
      )
    }
}

object CypherASTInstances extends CypherASTInstances

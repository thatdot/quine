package com.thatdot.quine.webapp.queryui

import com.thatdot.quine.webapp.resultspanel.{TapPoint, TapPointQuery}
import com.thatdot.quine.webapp.util.CypherReturnClause
import com.thatdot.quine.webapp.util.CypherReturnClause.Column

/** A result column that carries a node ID, and how: directly, or as the `id` field of a
  * serialized node (a returned node value is `{"id", "labels", "properties"}`). The column is
  * the result key as the server names it, unquoted; how it is spelled in Cypher is the
  * renderer's business.
  */
final case class IdRef(column: String, throughNode: Boolean)

/** Where a graph feed's suggested query gets its ID column from: which of a result's columns
  * hold node IDs, given the pipeline point the results arrive at and, when known, the query
  * producing them (its text, and for a standing query its mode). Empty means nothing could be
  * told, and the suggestion is offered with the spot left to fill in.
  *
  * An inference vouches for a column only when it knows the column holds a node ID. A guess
  * that is sometimes right is worse than an empty fill: the fill is preselected when the
  * suggestion is accepted, and a feed anchored on a value that is not a node ID silently
  * draws nothing.
  *
  * This is the seam between "what do we know about the result" and "what query do we
  * suggest": [[GraphFeedDefaults.suggestion]] only renders whatever an inference returns. The
  * one implementation today reads the producing query's mode and `RETURN` clause
  * ([[ReturnClauseIdInference]]); one fed by server-computed columns, or by sampled results at
  * runtime, would sit beside it and be tried with [[orElse]], the renderer unchanged.
  */
trait IdColumnInference { self =>

  def idColumns(tapPoint: TapPoint, source: Option[TapPointQuery]): Vector[IdRef]

  /** This inference, falling back to `that` when it finds nothing. */
  final def orElse(that: IdColumnInference): IdColumnInference = (tapPoint, source) => {
    val found = self.idColumns(tapPoint, source)
    if (found.nonEmpty) found else that.idColumns(tapPoint, source)
  }
}

object IdColumnInference {

  /** What the feed editor uses. */
  val default: IdColumnInference = ReturnClauseIdInference

  /** Finds nothing; the identity for [[IdColumnInference.orElse]]. */
  val none: IdColumnInference = (_, _) => Vector.empty
}

/** ID columns read off the producing query's `RETURN` clause (see [[CypherReturnClause]]),
  * tried tier by tier in order of confidence, the first tier to find anything winning. The
  * tiers are plain functions over the columns, so one can be tested, reordered, or replaced
  * on its own.
  *
  * Every tier rests on a fact, not a likelihood: the server's rule for a DistinctId pattern,
  * or an `id()` call or node pattern written in the query. There is no tier for a column that
  * merely looks like an ID (`customer_id` is a property, whatever its name) or for a lone
  * column (`WITH b.name AS x RETURN x` is one column and no ID), so those get an empty fill.
  *
  * Text-level by design: it sees the query's surface, not its semantics. It cannot follow a
  * rename (`WITH id(b) AS x RETURN x`), and wherever the text cannot say what a column holds
  * it vouches for nothing: a variable rebound along the way (`MATCH (n) WITH n.name AS n
  * RETURN n`, or by a `CALL` subquery), a variable parenthesised somewhere other than a
  * pattern (`WHERE (r).since > 1`), or a `UNION` whose branches may disagree. All of it is
  * the compiler's to settle, by serving the compiled columns with the standing query.
  */
object ReturnClauseIdInference extends IdColumnInference {

  /** One tier: the ID columns it can vouch for among `columns`, given the query they came from. */
  type Tier = (TapPointQuery, Vector[Column]) => Vector[IdRef]

  /** A DistinctId pattern's one column. The server compiles such a pattern only when it
    * returns exactly `id(x)` or `strId(x)` of the root node, with no filter and no projection
    * (quine-core's `GraphQueryPattern`, which throws `DistinctIdMustId` and friends otherwise),
    * so the result is one column holding a node ID whatever the text says. The text still
    * names the column, so this vouches only when it reads exactly one column off it.
    */
  val distinctId: Tier = {
    case (source, Vector(only)) if source.patternMode.contains(TapPointQuery.DistinctIdMode) && only.name.nonEmpty =>
      Vector(IdRef(only.name, throughNode = false))
    case _ => Vector.empty
  }

  /** `id(x)` / `strId(x)` columns, and bare variables bound by a node pattern in the query
    * (see [[CypherReturnClause.boundByNodePattern]]), whose serialized form carries the ID
    * under `id`. A variable some `AS` rebinds before the `RETURN` is left out: the node
    * pattern bound it, but it holds whatever came later.
    */
  val idTyped: Tier = (source, columns) => {
    val renamed = CypherReturnClause.renamedBeforeReturn(source.query)
    columns.collect {
      case c if c.idOf.isDefined => IdRef(c.name, throughNode = false)
      case c if c.variable.exists(v => !renamed(v) && CypherReturnClause.boundByNodePattern(source.query, v)) =>
        IdRef(c.name, throughNode = true)
    }
  }

  val tiers: Vector[Tier] = Vector(distinctId, idTyped)

  def idColumns(tapPoint: TapPoint, source: Option[TapPointQuery]): Vector[IdRef] =
    (for {
      src <- source
      // A UNION's columns hold every branch's values under the last branch's names, so no
      // branch's text vouches for a column.
      if !CypherReturnClause.isUnion(src.query)
      columns <- CypherReturnClause.columns(src.query)
      // A column the scanner could not name (an alias spelling a keyword, `RETURN id(n) AS
      // skip`) is no column to vouch for: an empty key would render as `$data.``.
      named = columns.filter(_.name.nonEmpty)
      found <- tiers.iterator.map(_(src, named)).find(_.nonEmpty)
    } yield found).getOrElse(Vector.empty)
}

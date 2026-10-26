package com.thatdot.quine.webapp.queryui

import com.thatdot.quine.webapp.resultspanel.{TapPoint, TapPointQuery, TapTarget}

/** Starting values for a graph feed drawn from a given pipeline point, so that creating one
  * is a matter of picking the point rather than writing Cypher from scratch.
  *
  * A feed's query runs once per result reaching the point, with the result's top-level
  * fields bound as parameters (`$data` and `$meta` for the raw match envelope, the columns
  * themselves after a transformation or enrichment), and must return the nodes to draw. The
  * only thing a feed can do without knowing the data is fetch the nodes the result already
  * names, so the suggested query looks a node up by ID, and the one thing left to fill in is
  * which column holds the ID. Which columns those are is an [[IdColumnInference]]'s call;
  * this object only renders its answer.
  */
object GraphFeedDefaults {

  /** A suggested query in three parts around the spot to fill in; `fill` is the inferred
    * value for that spot, empty when nothing could be inferred.
    */
  final case class Suggestion(before: String, fill: String, after: String) {
    def text: String = before + fill + after
  }

  /** The query to suggest for every result at `tapPoint`, given the query that produces those
    * results when known (the standing query's pattern, with its mode, for Raw and Transformed
    * points, the output's enrichment query for Enriched).
    *
    * The fill is the result's ID column as `inference` finds it, empty when it finds none.
    * Several ID columns are unwound into one lookup each: `id(n) = x` anchors a lookup only
    * against a single value, so `id(n) IN [...]` would not do.
    */
  def suggestion(
    tapPoint: TapPoint,
    source: Option[TapPointQuery],
    inference: IdColumnInference = IdColumnInference.default,
  ): Suggestion = {
    val prefix = paramPrefix(tapPoint)
    inference.idColumns(tapPoint, source).map(render) match {
      case Vector() => Suggestion(s"MATCH (n) WHERE id(n) = $prefix", "", " RETURN n")
      // A backticked column keeps its backticks outside the fill: whatever replaces the name
      // then stays valid Cypher, since backticks around a plain name are legal too.
      case Vector(only) if only.startsWith("`") && only.endsWith("`") =>
        Suggestion(s"MATCH (n) WHERE id(n) = $prefix`", only.substring(1, only.length - 1), "` RETURN n")
      case Vector(only) => Suggestion(s"MATCH (n) WHERE id(n) = $prefix", only, " RETURN n")
      case many =>
        Suggestion(
          "UNWIND [",
          many.map(prefix + _).mkString(", "),
          "] AS nodeId\nMATCH (n) WHERE id(n) = nodeId\nRETURN n",
        )
    }
  }

  /** A name for a new feed on `target`, unique among `taken`: the standing query's name,
    * suffixed with the output's for an output point, then numbered if that is in use.
    */
  def name(target: TapTarget.StandingQuery, taken: Set[String]): String = {
    val base = target.tapPoint match {
      case TapPoint.Raw => target.sqName
      case TapPoint.PreEnrichment(out) => s"${target.sqName}-$out"
      case TapPoint.PostEnrichment(out) => s"${target.sqName}-$out"
    }
    if (!taken(base)) base
    else Iterator.from(2).map(i => s"$base-$i").find(!taken(_)).get
  }

  /** How a result column is reached from the feed query: under `$data` for the raw envelope,
    * as a top-level parameter after a transformation or enrichment.
    */
  private def paramPrefix(tapPoint: TapPoint): String = tapPoint match {
    case TapPoint.Raw => "$data."
    case _: TapPoint.PreEnrichment | _: TapPoint.PostEnrichment => "$"
  }

  /** The ID as written after the parameter prefix: the column's key (backticked unless it is
    * a plain identifier), followed by `.id` when the column holds a serialized node.
    */
  private def render(ref: IdRef): String =
    key(ref.column) + (if (ref.throughNode) ".id" else "")

  private val PlainName = "[A-Za-z_][A-Za-z0-9_]*".r

  private def key(name: String): String =
    if (PlainName.matches(name)) name else "`" + name.replace("`", "``") + "`"
}

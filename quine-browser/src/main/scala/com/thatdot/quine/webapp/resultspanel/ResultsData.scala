package com.thatdot.quine.webapp.resultspanel

import io.circe.Json
import io.circe.Printer.noSpaces

import com.thatdot.quine.routes.CypherQueryResult

/** Pure transforms applied to a tabular result before it is displayed/exported:
  * substring filtering across the row's JSON, and single-column sorting — plus the
  * query-text display split both the identity bar and the switcher rows share.
  */
object ResultsData {

  /** How many trailing characters of a query always stay visible under middle-truncation —
    * the tail is where iterative refinements usually differ.
    */
  val queryTailChars = 18

  /** Middle-truncation split for one-line query display: whitespace collapses, then the text
    * divides into an ellipsizing head and an always-shown [[queryTailChars]]-character tail.
    * Shared by the identity bar and the switcher rows so the two can't drift.
    */
  def middleSplit(raw: String): (String, String) = {
    val q = raw.trim.replaceAll("\\s+", " ")
    (q.dropRight(queryTailChars), q.takeRight(queryTailChars))
  }

  /** Case-insensitive substring match across a row's JSON rendering. The needle is trimmed, so
    * leading/trailing whitespace doesn't change what matches (`" blah "` acts like `"blah"`).
    */
  def matches(row: Seq[Json], needle: String): Boolean = {
    val n = needle.trim
    n.isEmpty || row.map(noSpaces.print).mkString(" ").toLowerCase.contains(n.toLowerCase)
  }

  // One-slot memo: on every filter keystroke the header's row count and the body's derive both
  // filter the same (result, needle), so caching the last computation makes the second call a
  // hit instead of a second full-table scan. Single-threaded (Scala.js), so plain vars are safe;
  // the held reference is at most the result history already retains.
  private var memoIn: (CypherQueryResult, String) = null
  private var memoOut: CypherQueryResult = null

  def filter(result: CypherQueryResult, needle: String): CypherQueryResult =
    if (needle.trim.isEmpty) result
    else if ((memoIn ne null) && (memoIn._1 eq result) && memoIn._2 == needle) memoOut
    else {
      val out = result.copy(results = result.results.filter(matches(_, needle)))
      memoIn = (result, needle)
      memoOut = out
      out
    }

  def sort(result: CypherQueryResult, sortCol: Option[Int], dir: SortDir): CypherQueryResult =
    sortCol match {
      case Some(col) if result.columns.indices.contains(col) =>
        val sorted = result.results.sortBy(row => row.lift(col).getOrElse(Json.Null))(jsonOrdering)
        result.copy(results = if (dir == SortDir.Desc) sorted.reverse else sorted)
      case _ => result
    }

  /** Filter then sort. */
  def derive(result: CypherQueryResult, needle: String, sortCol: Option[Int], dir: SortDir): CypherQueryResult =
    sort(filter(result, needle), sortCol, dir)

  /** Type rank so heterogeneous columns sort into stable groups (nulls last). */
  private def rank(j: Json): Int =
    if (j.isNull) 4 else if (j.isNumber) 0 else if (j.isString) 1 else if (j.isBoolean) 2 else 3

  private val jsonOrdering: Ordering[Json] = new Ordering[Json] {
    def compare(a: Json, b: Json): Int = {
      val ra = rank(a)
      val rb = rank(b)
      if (ra != rb) ra - rb
      else
        (a.asNumber, b.asNumber) match {
          case (Some(na), Some(nb)) => na.toDouble.compare(nb.toDouble)
          case _ =>
            (a.asString, b.asString) match {
              case (Some(sa), Some(sb)) => sa.compareTo(sb)
              case _ =>
                (a.asBoolean, b.asBoolean) match {
                  case (Some(ba), Some(bb)) => ba.compare(bb)
                  case _ => noSpaces.print(a).compareTo(noSpaces.print(b))
                }
            }
        }
    }
  }
}

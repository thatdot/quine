package com.thatdot.quine.webapp.components.streams

import io.circe.Json

import com.thatdot.quine.webapp.fields.{FieldPath, PathSeg}

/** Layer 4 (pure part): turn a set of picked field paths into a Novelty JavaScript observation
  * transformation and its record format. The only Novelty-aware code in the stack; kept
  * pure (no Laminar, no DOM) so it is unit-testable.
  *
  * Paths are evaluated against `that` — the full standing-query result the destination sends as
  * the observation, i.e. `{ meta, data }` — so an object path like `data.amount` becomes
  * `that.data?.amount`. Keys that are not JavaScript identifiers (an unaliased `RETURN id(n)`
  * yields the key `id(n)`) are accessed with a quoted bracket lookup.
  *
  * Every result is not guaranteed to have every field, so the function never fails on a missing
  * one:
  *   - each step after `that` is optional (`?.`), so a missing or null parent yields nothing;
  *   - a scalar component that is missing or null becomes `""`, keeping its position in the
  *     observation (Novelty scores each component in the context of the ones before it);
  *   - an array segment (`[*]`) treats anything that is not an array as empty, maps over the
  *     elements (flat-mapping when arrays nest), drops missing values, and spreads the rest into
  *     the observation, each element contributing a component;
  *   - a missing tag is `null`.
  *
  * The function opens with only the small helpers it uses: `s` (scalar to string), `a` (array or
  * empty) and `v` (array values to strings).
  */
object NoveltyTransformGenerator {

  final case class Generated(
    function: String,
    recordFormat: String, // "TAGGED" when a tag was chosen, else "BARE"
  )

  private val JsIdentifier = "[A-Za-z_$][A-Za-z0-9_$]*".r

  private val Helpers: List[(String, String)] = List(
    "s" -> """const s = x => String(x ?? "");""",
    "a" -> "const a = x => Array.isArray(x) ? x : [];",
    "v" -> "const v = xs => xs.filter(x => x != null).map(String);",
  )

  /** Accessor for one object key: `acc?.key` for an identifier, else `acc?.["key"]`; a plain `.`
    * from `that`, which is always present.
    */
  private def keyAccess(acc: String, key: String): String = {
    val dot = if (acc == "that") "." else "?."
    if (JsIdentifier.matches(key)) s"$acc$dot$key"
    else s"$acc${if (acc == "that") "" else "?."}[${Json.fromString(key).noSpaces}]"
  }

  /** Accessor for an Elem-free path, e.g. `that.data?.user?.id`. */
  private def scalarAccess(rootExpr: String, segs: Vector[PathSeg]): String =
    segs.foldLeft(rootExpr) {
      case (acc, PathSeg.Key(k)) => keyAccess(acc, k)
      case (acc, PathSeg.Elem) => acc // not reached for Elem-free paths
    }

  /** A JS expression yielding the array of (possibly missing) values at `segs`, which passes
    * through at least one array wildcard.
    */
  private def arrayValues(rootExpr: String, segs: Vector[PathSeg]): String = {
    val elemIdx = segs.indexOf(PathSeg.Elem)
    val elems = s"a(${scalarAccess(rootExpr, segs.take(elemIdx))})"
    val rest = segs.drop(elemIdx + 1)
    if (rest.isEmpty) elems
    else if (rest.contains(PathSeg.Elem)) s"$elems.flatMap(e => ${arrayValues("e", rest)})"
    else s"$elems.map(e => ${scalarAccess("e", rest)})"
  }

  /** @param tagPath must not pass through an array (`[*]`): the tag is a single value */
  def generate(
    observationPaths: Vector[FieldPath],
    tagPath: Option[FieldPath],
  ): Generated = {
    require(!tagPath.exists(_.segs.contains(PathSeg.Elem)), "tag path must not pass through an array")
    val components = observationPaths.map { p =>
      if (p.segs.contains(PathSeg.Elem)) s"...v(${arrayValues("that", p.segs)})"
      else s"s(${scalarAccess("that", p.segs)})"
    }
    val observationArray = if (components.isEmpty) "[]" else components.mkString("[ ", ", ", " ]")
    val result = tagPath match {
      case Some(tp) => s"{ observation: $observationArray, tag: ${scalarAccess("that", tp.segs)} ?? null }"
      case None => observationArray
    }
    val helpers = Helpers.collect {
      case (name, definition) if raw"\b$name\(".r.findFirstIn(result).isDefined => definition
    }

    Generated(
      function = s"that => { ${(helpers :+ s"return $result;").mkString(" ")} }",
      recordFormat = if (tagPath.isDefined) "TAGGED" else "BARE",
    )
  }
}

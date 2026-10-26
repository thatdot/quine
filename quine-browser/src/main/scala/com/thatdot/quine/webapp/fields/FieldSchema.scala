package com.thatdot.quine.webapp.fields

import scala.annotation.tailrec

import io.circe.Json

/** Layer 1 of the wiretap field-learning stack: pure, dependency-free inference of the field
  * structure of a set of JSON samples.
  *
  * This package knows nothing about standing queries, wiretaps, Novelty, Laminar, or the DOM — it
  * folds a sequence of [[io.circe.Json]] values into a [[FieldSchema]] describing every path that
  * appears, its inferred kind(s), how often it was present, and a few example values. It is the
  * reusable core that any "learn the shape of some JSON" feature can build on.
  *
  * Arrays are descended with a single wildcard [[PathSeg.Elem]] segment, merging every element's
  * shape under one path (`data.items[*].name`), so a picked `[*]` path means "for each element".
  */

/** One segment of a path into a JSON value. */
sealed trait PathSeg
object PathSeg {

  /** An object field. */
  final case class Key(name: String) extends PathSeg

  /** An array element (all elements merged — rendered `[*]`). */
  case object Elem extends PathSeg
}

/** A path from the root of a sample to some nested value. */
final case class FieldPath(segs: Vector[PathSeg]) {
  def /(seg: PathSeg): FieldPath = FieldPath(segs :+ seg)
  def isRoot: Boolean = segs.isEmpty

  /** Dotted/bracketed rendering, e.g. `data.items[*].name`. A key that is not a plain name (it
    * holds a `.`, brackets, spaces, ...) renders as a quoted `["key"]`, so that distinct paths
    * never render alike: `{"a.b": 1}` is `["a.b"]`, while `{"a": {"b": 1}}` is `a.b`.
    */
  def render: String = {
    val sb = new StringBuilder
    segs.foreach {
      case PathSeg.Key(name) if FieldPath.PlainKey.matches(name) =>
        if (sb.nonEmpty) sb.append('.')
        sb.append(name)
      case PathSeg.Key(name) =>
        sb.append('[').append(Json.fromString(name).noSpaces).append(']')
      case PathSeg.Elem =>
        sb.append("[*]")
    }
    if (sb.isEmpty) "." else sb.toString
  }
}
object FieldPath {
  val root: FieldPath = FieldPath(Vector.empty)

  /** Read a path written as [[FieldPath.render]] writes it: keys joined by `.`, `[*]` for an array
    * element, and `["key"]` for any key that isn't a plain name (e.g. `data.items[*].name`,
    * `data["n.name"]`). The reason it can't be read, otherwise.
    */
  def parse(text: String): Either[String, FieldPath] = {
    @tailrec def loop(rest: String, acc: Vector[PathSeg]): Either[String, Vector[PathSeg]] =
      if (rest.isEmpty) Right(acc)
      else if (rest.startsWith("[*]")) loop(rest.drop(3), acc :+ PathSeg.Elem)
      else if (rest.startsWith("[\"")) quotedKey(rest) match {
        case Some((key, remaining)) => loop(remaining, acc :+ PathSeg.Key(key))
        case None => Left("""a quoted key must be a JSON string in brackets, e.g. ["n.name"]""")
      }
      else {
        // A plain key: after another segment it follows a `.`.
        val body = if (acc.nonEmpty && rest.startsWith(".")) rest.drop(1) else rest
        val key = body.takeWhile(c => c != '.' && c != '[')
        if (PlainKey.matches(key)) loop(body.drop(key.length), acc :+ PathSeg.Key(key))
        else if (body.startsWith("[")) Left("""`[` must start `[*]` or a quoted key such as ["n.name"]""")
        else if (key.isEmpty) Left("a `.` must be followed by a field name")
        else Left(s"""`$key` is not a plain name; write it as ["$key"]""")
      }

    loop(text.trim, Vector.empty).flatMap { segs =>
      if (segs.isEmpty) Left("enter a path, e.g. data.name") else Right(FieldPath(segs))
    }
  }

  /** The key of a quoted segment at the start of `rest` (which starts with `["`), and what follows
    * it; `None` if the JSON string isn't closed by `"]`.
    */
  private def quotedKey(rest: String): Option[(String, String)] = {
    @tailrec def closingQuote(i: Int): Option[Int] =
      if (i >= rest.length) None
      else
        rest.charAt(i) match {
          case '\\' => closingQuote(i + 2)
          case '"' => Some(i)
          case _ => closingQuote(i + 1)
        }
    closingQuote(2).filter(i => rest.startsWith("]", i + 1)).flatMap { i =>
      io.circe.parser.decode[String](rest.substring(1, i + 1)).toOption.map(_ -> rest.drop(i + 2))
    }
  }

  /** Keys rendered as they are; any other key is rendered quoted. */
  private val PlainKey = "[A-Za-z0-9_$-]+".r
}

/** The inferred kind of a JSON value at some path. */
sealed trait JsonKind
object JsonKind {
  case object Str extends JsonKind
  case object Num extends JsonKind
  case object Bool extends JsonKind
  case object Null extends JsonKind
  case object Obj extends JsonKind
  case object Arr extends JsonKind

  def of(j: Json): JsonKind =
    j.fold(
      jsonNull = Null,
      jsonBoolean = _ => Bool,
      jsonNumber = _ => Num,
      jsonString = _ => Str,
      jsonArray = _ => Arr,
      jsonObject = _ => Obj,
    )

  /** Scalar kinds are the pickable leaves. `Null` counts as scalar (an always-null field is still
    * a leaf); `Obj`/`Arr` are interior.
    */
  def isScalar(k: JsonKind): Boolean = k match {
    case Str | Num | Bool | Null => true
    case Obj | Arr => false
  }

  def label(k: JsonKind): String = k match {
    case Str => "string"
    case Num => "number"
    case Bool => "boolean"
    case Null => "null"
    case Obj => "object"
    case Arr => "array"
  }
}

/** Accumulated observations for a single path across all samples. */
final case class FieldStat(
  kinds: Set[JsonKind],
  presentIn: Int, // number of samples in which this path appeared at least once
  examples: Vector[Json], // small bounded reservoir of distinct scalar example values
  declared: Boolean = false, // added by hand ([[FieldSchema.declare]]) rather than only seen in samples
) {

  /** Non-null kinds seen — the ones that matter for "is this heterogeneous". */
  private def concreteKinds: Set[JsonKind] = kinds - JsonKind.Null

  /** A pickable leaf: every kind seen is scalar (so not an object/array), and it's not purely
    * absent. An always-null field is a (degenerate) scalar leaf.
    */
  def isScalar: Boolean = kinds.nonEmpty && kinds.forall(JsonKind.isScalar)

  /** Seen as more than one concrete (non-null) kind, e.g. sometimes string, sometimes number. */
  def isMixed: Boolean = concreteKinds.size > 1
}

/** The field structure learned from `sampleCount` samples. `stats` holds every path observed —
  * interior (object/array) and leaf — keyed by [[FieldPath]].
  */
final case class FieldSchema(
  sampleCount: Int,
  stats: Map[FieldPath, FieldStat],
) {

  /** Scalar-valued, non-root paths — the fields a user can pick — sorted by rendered path. */
  def leaves: Vector[(FieldPath, FieldStat)] =
    stats.toVector
      .filter { case (p, s) => !p.isRoot && s.isScalar }
      .sortBy(_._1.render)

  def isEmpty: Boolean = sampleCount == 0
}

object FieldSchema {
  val empty: FieldSchema = FieldSchema(0, Map.empty)

  /** Max distinct scalar example values retained per path. */
  private val MaxExamples = 3

  /** Fold one sample into the schema. Safe to call incrementally as samples stream in. */
  def observe(schema: FieldSchema, sample: Json): FieldSchema = {
    // First pass: collect every path present in THIS sample (deduped by path), unioning kinds and
    // gathering scalar examples. Presence is binary per sample, so a path seen many times (array
    // elements) still counts once.
    val inSample = scala.collection.mutable.LinkedHashMap.empty[FieldPath, (Set[JsonKind], Vector[Json])]

    def visit(path: FieldPath, j: Json): Unit = {
      val kind = JsonKind.of(j)
      val (priorKinds, priorExamples) = inSample.getOrElse(path, (Set.empty[JsonKind], Vector.empty[Json]))
      val examples =
        if (JsonKind.isScalar(kind) && priorExamples.size < MaxExamples && !priorExamples.contains(j))
          priorExamples :+ j
        else priorExamples
      inSample.update(path, (priorKinds + kind, examples))

      j.asObject.foreach(_.toIterable.foreach { case (k, v) => visit(path / PathSeg.Key(k), v) })
      j.asArray.foreach(_.foreach(v => visit(path / PathSeg.Elem, v)))
    }
    visit(FieldPath.root, sample)

    // Second pass: merge this sample's findings into the running schema (presentIn += 1 per path).
    val mergedStats = inSample.foldLeft(schema.stats) { case (m, (path, (kinds, examples))) =>
      m.get(path) match {
        case None => m.updated(path, FieldStat(kinds, presentIn = 1, examples))
        case Some(existing) =>
          m.updated(
            path,
            existing.copy(
              kinds = existing.kinds ++ kinds,
              presentIn = existing.presentIn + 1,
              examples = mergeExamples(existing.examples, examples),
            ),
          )
      }
    }
    FieldSchema(schema.sampleCount + 1, mergedStats)
  }

  /** Add a field by hand, as holding values of `kind`, without a sample. Counts toward no sample, and
    * leaves a field already learned from samples as it is apart from also allowing `kind`.
    */
  def declare(schema: FieldSchema, path: FieldPath, kind: JsonKind): FieldSchema = {
    val stat = schema.stats.get(path) match {
      case Some(existing) => existing.copy(kinds = existing.kinds + kind, declared = true)
      case None => FieldStat(Set(kind), presentIn = 0, Vector.empty, declared = true)
    }
    schema.copy(stats = schema.stats.updated(path, stat))
  }

  private def mergeExamples(a: Vector[Json], b: Vector[Json]): Vector[Json] =
    b.foldLeft(a)((acc, j) => if (acc.size < MaxExamples && !acc.contains(j)) acc :+ j else acc)

  def fromSamples(samples: IterableOnce[Json]): FieldSchema =
    samples.iterator.foldLeft(empty)(observe)
}

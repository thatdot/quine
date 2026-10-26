package com.thatdot.quine.webapp.util

import java.util.regex.Pattern

/** The columns a Cypher query's final `RETURN` clause produces, read off the query text.
  *
  * This is the shape of a standing query's result `data` map and of an enrichment query's rows:
  * the server names each column by its `AS` alias when one is given and by the item's verbatim
  * expression text otherwise (`RETURN DISTINCT id(n)` yields the key `id(n)`). The reader here
  * mirrors that naming without a Cypher parser: it scans the text once, blanking out string
  * literals, backticked names, and comments and tracking bracket depth, so `RETURN`, `AS`, and
  * the item-separating commas are only recognised at the top level of the final clause.
  * A comment at either end of a column's text is no part of it, one in the middle is
  * (`id( /* c */ n)` is the key `id( /* c */ n)`), and a top-level `;` ends the clause, as
  * the server has it for all three.
  *
  * Heuristic by design: it knows nothing of the query's semantics, only its surface. Callers
  * treat `None` as "the columns are unknown" (no `RETURN`, or a `RETURN *`) and fall back.
  */
object CypherReturnClause {

  /** One returned column: the expression as written and the name the result row uses for it. */
  final case class Column(expression: String, name: String) {

    /** The node variable when the expression is `id(x)` or `strId(x)`: the column holds a node
      * ID.
      */
    val idOf: Option[String] = expression match {
      case IdCall(_, argument) => Some(unquote(argument))
      case _ => None
    }

    /** The variable when the expression is a bare identifier (`RETURN n`). */
    val variable: Option[String] = expression match {
      case BareName(identifier) if !ReservedWords.contains(identifier.toLowerCase) => Some(unquote(identifier))
      case _ => None
    }
  }

  private val IdCall = """(?i)^(id|strId)\s*\(\s*(`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_]*)\s*\)$""".r
  private val BareName = """^(`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_]*)$""".r
  private val ReservedWords = Set("null", "true", "false")

  /** Strip surrounding backticks and undo their doubling escape. */
  private def unquote(name: String): String =
    if (name.length >= 2 && name.startsWith("`") && name.endsWith("`"))
      name.substring(1, name.length - 1).replace("``", "`")
    else name

  /** The columns of the last top-level `RETURN` clause, in order. `None` when the query has no
    * top-level `RETURN`, or when it returns `*` (columns unknowable from the text).
    */
  def columns(query: String): Option[Vector[Column]] = {
    val scan = Scanner.scan(query)
    scan.lastKeywordAt("RETURN").flatMap { returnStart =>
      val bodyStart = returnStart + "RETURN".length
      // The projection ends where a trailing modifier, a set operation, or the statement ends.
      val bodyEnd =
        (Vector("ORDER", "SKIP", "LIMIT", "UNION").flatMap(kw => scan.firstKeywordAt(kw, from = bodyStart)) ++
          scan.firstCharAt(';', from = bodyStart)).minOption
          .getOrElse(query.length)
      val itemsStart = scan.firstKeywordAt("DISTINCT", from = bodyStart) match {
        case Some(at) if scan.trimmed(bodyStart, at).isEmpty => at + "DISTINCT".length
        case _ => bodyStart
      }
      val items = scan
        .splitTopLevel(itemsStart, bodyEnd)
        .map { case (from, to) => (from, to, scan.trimmed(from, to)) }
        .filter(_._3.nonEmpty)
      if (items.exists(_._3 == "*")) None
      else
        Some(items.map { case (from, to, text) =>
          scan.lastKeywordAt("AS", from = from, until = to) match {
            case Some(asAt) =>
              val expression = scan.trimmed(from, asAt)
              val alias = unquote(scan.trimmed(asAt + "AS".length, to))
              Column(expression, alias)
            case None => Column(text, text)
          }
        }.toVector)
    }
  }

  /** The names an `AS` binds ahead of the final `RETURN`, at any depth (`WITH n.name AS n`,
    * `UNWIND xs AS x`, `CALL { ... RETURN m.name AS m }`). A `WITH` may rebind a name already
    * in scope and a subquery binds its names for the enclosing query, so whatever such a name
    * was bound to earlier in the query, it need not be that by the `RETURN`.
    */
  def renamedBeforeReturn(query: String): Set[String] = {
    val scan = Scanner.scan(query)
    val until = scan.lastKeywordAt("RETURN").getOrElse(query.length)
    scan
      .keywordsAt("AS", from = 0, until = until, topLevel = false)
      .flatMap(at => scan.nameAfter(at + "AS".length))
      .toSet
  }

  /** Whether the query joins branches with a top-level `UNION`. Its columns then carry the
    * last branch's names but every branch's values, so no branch's text says what a column
    * holds.
    */
  def isUnion(query: String): Boolean = Scanner.scan(query).firstKeywordAt("UNION").isDefined

  /** Whether `variable` is bound by a node pattern in `query`, read off its surface with
    * string literals, backticked names, and comments blanked out. A binding is `(v`, `(v:Label`,
    * or `(v {` opening a pattern: after a clause keyword, a comma, a relationship arrow, a path
    * assignment, or an opening bracket, or else running on into a relationship. A `(v)` as a
    * function's argument (`id(v)`) says nothing either way. A `(v)` anywhere else
    * (`WHERE (v).since > 1`) counts against it: the text then cannot say whether `v` is a
    * node, so it is not one.
    */
  def boundByNodePattern(query: String, variable: String): Boolean = {
    val surface = Scanner.scan(query).surface
    val verdicts = ("""\(\s*""" + Pattern.quote(variable) + """\s*[:{)]""").r
      .findAllMatchIn(surface)
      .map { m =>
        val before = surface.substring(0, m.start)
        val after = surface.substring(m.end)
        if (
          m.matched.last != ')' || PatternOpener.findFirstIn(before).isDefined ||
          ContinuesPattern.findPrefixOf(after).isDefined
        ) Some(true)
        else
          WordBefore.findFirstMatchIn(before) match {
            case Some(word) if !ExpressionKeywords(word.group(1).toLowerCase) => None
            case _ => Some(false)
          }
      }
      .toVector
    verdicts.contains(Some(true)) && !verdicts.contains(Some(false))
  }

  /** What may come right before a `(` that opens a node pattern. */
  private val PatternOpener = """(?i)(^|\bMATCH|\bMERGE|\bCREATE|[,\-=>(\[])\s*$""".r

  /** A relationship leaving the node just closed: `(v)-`, `(v)<-`. */
  private val ContinuesPattern = """\s*[-<]""".r

  /** The word right before a `(`: a function's name, or a keyword. */
  private val WordBefore = """([A-Za-z_][A-Za-z0-9_]*)\s*$""".r

  /** Keywords a parenthesised expression may follow; any other word before a `(` names a
    * function.
    */
  private val ExpressionKeywords = Set(
    "where",
    "and",
    "or",
    "xor",
    "not",
    "return",
    "with",
    "unwind",
    "set",
    "delete",
    "detach",
    "remove",
    "order",
    "by",
    "skip",
    "limit",
    "distinct",
    "as",
    "is",
    "in",
    "when",
    "then",
    "else",
    "case",
    "yield",
    "starts",
    "ends",
    "contains",
  )

  /** A single pass over the text recording, per character, a masked copy (string literals,
    * backticked names, and comments replaced by `#`, so no keyword or comma inside them can
    * match), whether the character is part of a comment, and the bracket depth it sits at.
    */
  final private class Scanner(text: String, masked: Array[Char], comment: Array[Boolean], depth: Array[Int]) {

    private def isWordChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'

    /** Whether a word starting right after `c` is a property (`n.order`) or parameter
      * (`$limit`) name rather than a keyword.
      */
    private def isNamePrefix(c: Char): Boolean = c == '.' || c == '$'

    /** The text with string literals, backticked names, and comments blanked out. */
    def surface: String = new String(masked)

    /** Positions of `keyword` (case-insensitive, whole word, and at the top level unless
      * `topLevel` is off) in `[from, until)`.
      */
    def keywordsAt(keyword: String, from: Int, until: Int, topLevel: Boolean = true): Iterator[Int] = {
      val upper = keyword.toUpperCase
      val n = upper.length
      Iterator
        .range(from, until - n + 1)
        .filter { i =>
          (!topLevel || depth(i) == 0) &&
          (i == 0 || !(isWordChar(masked(i - 1)) || isNamePrefix(masked(i - 1)))) &&
          (i + n == masked.length || !isWordChar(masked(i + n))) &&
          Iterator.range(0, n).forall(k => masked(i + k).toUpper == upper(k))
        }
    }

    def firstKeywordAt(keyword: String, from: Int = 0, until: Int = text.length): Option[Int] =
      keywordsAt(keyword, from, until).nextOption()

    def lastKeywordAt(keyword: String, from: Int = 0, until: Int = text.length): Option[Int] =
      keywordsAt(keyword, from, until).foldLeft(Option.empty[Int])((_, at) => Some(at))

    /** The first top-level `c` outside strings, backticks, and comments in `[from, until)`. */
    def firstCharAt(c: Char, from: Int, until: Int = text.length): Option[Int] =
      Iterator.range(from, until).find(i => masked(i) == c && depth(i) == 0)

    /** `[from, until)` of the text with whitespace and comments trimmed off both ends. A
      * comment in the middle stays, as the server keeps it in a column's name.
      */
    def trimmed(from: Int, until: Int): String = {
      def padding(i: Int): Boolean = comment(i) || text.charAt(i).isWhitespace
      val start = Iterator.range(from, until).dropWhile(padding).nextOption().getOrElse(until)
      val end = Iterator.range(start, until).filterNot(padding).foldLeft(start)((_, i) => i + 1)
      text.substring(start, end)
    }

    /** The name written at `pos` after any whitespace: a backticked name unquoted, or a plain
      * identifier. `None` when something else is there.
      */
    def nameAfter(pos: Int): Option[String] = {
      val start = Iterator.range(pos, text.length).find(i => !text.charAt(i).isWhitespace).getOrElse(text.length)
      if (start < text.length && text.charAt(start) == '`')
        Some(unquote(text.substring(start, Scanner.closingBacktick(text, start))))
      else {
        val end = Iterator.range(start, text.length).find(i => !isWordChar(text.charAt(i))).getOrElse(text.length)
        Some(text.substring(start, end)).filter(name => name.nonEmpty && !name.charAt(0).isDigit)
      }
    }

    /** `[from, until)` cut at its top-level commas, as `(start, end)` slices. */
    def splitTopLevel(from: Int, until: Int): Vector[(Int, Int)] = {
      val cuts = Iterator.range(from, until).filter(i => masked(i) == ',' && depth(i) == 0).toVector
      val starts = from +: cuts.map(_ + 1)
      val ends = cuts :+ until
      starts.zip(ends)
    }
  }

  private object Scanner {

    /** The index just past the backtick closing the quoted name opening at `open`, or the
      * text's end when it never closes. A doubled backtick is an escaped backtick, not a close.
      */
    def closingBacktick(text: String, open: Int): Int = {
      var j = open + 1
      var closed = false
      while (j < text.length && !closed)
        if (text.charAt(j) == '`')
          if (j + 1 < text.length && text.charAt(j + 1) == '`') j += 2 else closed = true
        else j += 1
      math.min(j + 1, text.length)
    }

    def scan(text: String): Scanner = {
      val masked = text.toCharArray
      val comment = new Array[Boolean](text.length)
      val depth = new Array[Int](text.length)
      var level = 0
      var i = 0
      def blank(from: Int, to: Int): Unit =
        Iterator.range(from, to).foreach(k => if (masked(k) != '\n') masked(k) = '#')
      while (i < text.length) {
        val c = text.charAt(i)
        depth(i) = level
        c match {
          case '/' if i + 1 < text.length && text.charAt(i + 1) == '/' =>
            val end = text.indexOf('\n', i) match { case -1 => text.length; case nl => nl }
            Iterator.range(i, end).foreach { k => depth(k) = level; comment(k) = true }
            blank(i, end)
            i = end
          case '/' if i + 1 < text.length && text.charAt(i + 1) == '*' =>
            val end = text.indexOf("*/", i + 2) match { case -1 => text.length; case close => close + 2 }
            Iterator.range(i, end).foreach { k => depth(k) = level; comment(k) = true }
            blank(i, end)
            i = end
          case '\'' | '"' =>
            var j = i + 1
            while (j < text.length && text.charAt(j) != c) j = if (text.charAt(j) == '\\') j + 2 else j + 1
            val end = math.min(j + 1, text.length)
            Iterator.range(i, end).foreach(k => depth(k) = level)
            blank(i, end)
            i = end
          case '`' =>
            val end = closingBacktick(text, i)
            Iterator.range(i, end).foreach(k => depth(k) = level)
            blank(i, end)
            i = end
          case '(' | '[' | '{' =>
            level += 1
            i += 1
          case ')' | ']' | '}' =>
            level = math.max(0, level - 1)
            i += 1
          case _ =>
            i += 1
        }
      }
      new Scanner(text, masked, comment, depth)
    }
  }
}

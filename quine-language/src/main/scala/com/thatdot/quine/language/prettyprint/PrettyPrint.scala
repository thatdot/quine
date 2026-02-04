package com.thatdot.quine.language.prettyprint

/** Document algebra for structured pretty printing output. */
sealed trait Doc

object Doc {
  case class Text(s: String) extends Doc
  case object Line extends Doc
  case class Concat(left: Doc, right: Doc) extends Doc
  case class Nest(depth: Int, doc: Doc) extends Doc
  case object Empty extends Doc

  def text(s: String): Doc = Text(s)
  def line: Doc = Line
  def empty: Doc = Empty
  def nest(depth: Int, doc: Doc): Doc = Nest(depth, doc)

  def concat(docs: Doc*): Doc = docs.foldLeft(empty: Doc) { (acc, d) =>
    if (acc == Empty) d
    else if (d == Empty) acc
    else Concat(acc, d)
  }

  def intercalate(sep: Doc, docs: List[Doc]): Doc = docs match {
    case Nil => Empty
    case d :: Nil => d
    case d :: ds => ds.foldLeft(d)((acc, x) => Concat(Concat(acc, sep), x))
  }

  def render(doc: Doc, indentWidth: Int = 2): String = {
    val sb = new StringBuilder

    def go(d: Doc, currentIndent: Int): Unit = d match {
      case Text(s) => sb.append(s)
      case Line => sb.append("\n"); sb.append(" " * currentIndent)
      case Concat(l, r) => go(l, currentIndent); go(r, currentIndent)
      case Nest(depth, inner) => go(inner, currentIndent + depth * indentWidth)
      case Empty => ()
    }

    go(doc, 0)
    sb.toString
  }
}

/** Typeclass for pretty printing values to structured documents. */
trait PrettyPrint[A] {
  def doc(a: A): Doc

  def pretty(a: A, indentWidth: Int = 2): String =
    Doc.render(doc(a), indentWidth)
}

object PrettyPrint {
  def apply[A](implicit pp: PrettyPrint[A]): PrettyPrint[A] = pp

  def instance[A](f: A => Doc): PrettyPrint[A] = new PrettyPrint[A] {
    def doc(a: A): Doc = f(a)
  }
}

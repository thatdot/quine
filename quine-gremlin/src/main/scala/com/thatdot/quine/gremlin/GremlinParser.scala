package com.thatdot.quine.gremlin

import scala.util.parsing.combinator._

import com.thatdot.quine.model.QuineValue

import language.implicitConversions

/** Trait which encapsulates all of the parsing functionality for the supported subset of Gremlin */
// format: off
private[gremlin] trait GremlinParser extends PackratParsers {
  self: GremlinTypes =>

  private val labelKey = QuineValue.Str(graph.labelsProperty.name)

  override type Elem = GremlinToken
  import GremlinToken._

  // This makes it easier to possible to use strings in productions to refer to
  // identifiers
  implicit private def mkIdentifier(str: String): PackratParser[Elem] = accept(Identifier(str))

  private lazy val empty = success(())

  private lazy val ident: PackratParser[String] =
    accept("identifier", { case Identifier(i) => i })

  private lazy val value: PackratParser[GremlinExpression] = positioned {
    ( accept("literal", { case LitToken(v) => TypedValue(v) })
    | `[` ~>! repsep(value, `,`) <~ `]`  ^^ { xs => RawArr(xs.toVector) }
    | "idFrom" ~>! p(values)             ^^ { xs => IdFromFunc(xs.toVector) }
    | ident                              ^^ { i => Variable(Symbol(i)) }
    ).withFailureMessage("malformed expression")
  }

  private lazy val pred: PackratParser[GremlinPredicateExpression] = positioned {
    ( "eq"     ~>! p(value)              ^^ { EqPred.apply }
    | "neq"    ~>! p(value)              ^^ { NeqPred.apply }
    | "within" ~>! p(value)              ^^ { WithinPred.apply }
    | "regex"  ~>! p(value)              ^^ { RegexPred.apply }
    | value                              ^^ { EqPred.apply }
    ).withFailureMessage("malformed predicate")
  }

  private lazy val values = repsep(value, `,`)

  private lazy val values1 = rep1sep(value, `,`)

  private def p[A](inner: PackratParser[A]): PackratParser[A] = `(` ~> inner <~ `)`

  private lazy val traversalStep: PackratParser[TraversalStep] = positioned {
    ( ("v"|"V")    ~> p(empty)               ^^^ { EmptyVertices }
    | ("v"|"V")    ~> p("recent_nodes")      ^^^ { RecentVertices(None) }
    | "recentV"    ~>! p(opt(value))         ^^  { RecentVertices.apply }
    | ("v"|"V")    ~> p(values)              ^^  { Vertices.apply }
    | "has"        ~> p(value)               ^^  { v  => Has(v, NoTest) }
    | "hasNot"     ~>! p(value)              ^^  { v  => Has(v, NegatedTest) }
    | "hasLabel"   ~>! p(pred)               ^^  { l  => Has(TypedValue(labelKey), ValueTest(l)) }
    | "has" ~> p(value ~ (`,` ~>! pred))     ^^  { xy => Has(xy._1, ValueTest(xy._2)) }
    | "has" ~> p("label" ~> `,` ~>! pred)    ^^  { l  => Has(TypedValue(labelKey), ValueTest(l)) }
    | "hasId"      ~>! p(values)             ^^  { HasId.apply }
    | "eqToVar"    ~>! p(value)              ^^  { EqToVar.apply }
    | "out"        ~>! p(values)             ^^  { vs => HopFromVertex(vs, OutOnly, toVertex = true, None) }
    | "outLimit"   ~>! p(values1)            ^^  { vs => HopFromVertex(vs.init, OutOnly, toVertex = true, Some(vs.last)) }
    | "in"         ~>! p(values)             ^^  { vs => HopFromVertex(vs, InOnly, toVertex = true, None) }
    | "inLimit"    ~>! p(values1)            ^^  { vs => HopFromVertex(vs.init, InOnly, toVertex = true, Some(vs.last)) }
    | "both"       ~>! p(values)             ^^  { vs => HopFromVertex(vs, OutAndIn, toVertex = true, None) }
    | "bothLimit"  ~>! p(values1)            ^^  { vs => HopFromVertex(vs.init, OutAndIn, toVertex = true, Some(vs.last)) }
    | "outE"       ~>! p(values)             ^^  { vs => HopFromVertex(vs, OutOnly, toVertex = false, None) }
    | "outELimit"  ~>! p(values1)            ^^  { vs => HopFromVertex(vs.init, OutOnly, toVertex = false, Some(vs.last)) }
    | "inE"        ~>! p(values)             ^^  { vs => HopFromVertex(vs, InOnly, toVertex = false, None) }
    | "inELimit"   ~>! p(values1)            ^^  { vs => HopFromVertex(vs.init, InOnly, toVertex = false, Some(vs.last)) }
    | "bothE"      ~>! p(values)             ^^  { vs => HopFromVertex(vs, OutAndIn, toVertex = false, None) }
    | "bothELimit" ~>! p(values1)            ^^  { vs => HopFromVertex(vs.init, OutAndIn, toVertex = false, Some(vs.last)) }
    | "outV"       ~>! p(empty)              ^^^ { HopFromEdge(OutOnly) }
    | "inV"        ~>! p(empty)              ^^^ { HopFromEdge(InOnly) }
    | "bothV"      ~>! p(empty)              ^^^ { HopFromEdge(OutAndIn) }
    | "values"     ~>! p(values)             ^^  { ks => Values(ks, groupResultsInMap = false) }
    | "valueMap"   ~>! p(values)             ^^  { ks => Values(ks, groupResultsInMap = true) }
    | "dedup"      ~>! p(empty)              ^^^ { Dedup }
    | "as"         ~>! p(value)              ^^  { As.apply }
    | "select"     ~>! p(values)             ^^  { Select.apply }
    | "limit"      ~>! p(value)              ^^  { Limit.apply }
    | "id"         ~>! p(empty)              ^^^ { Id(stringOutput = false) }
    | "strId"      ~>! p(empty)              ^^^ { Id(stringOutput = true) }
    | "unrollPath" ~>! p(empty)              ^^^ { UnrollPath }
    | "count"      ~>! p(empty)              ^^^ { Count }
    | "groupCount" ~>! p(empty)              ^^^ { GroupCount }
    | "not"        ~>! p(anonTrav)           ^^  { t  => Logical(Not(t)) }
    | "where"      ~>! p(anonTrav)           ^^  { t  => Logical(Where(t)) }
    | "or"         ~>! p(anonTravs1)         ^^  { ts => Logical(Or(ts)) }
    | "and"        ~>! p(anonTravs1)         ^^  { ts => Logical(And(ts)) }
    | "is"         ~>! p(pred)               ^^  { Is.apply }
    | "union"      ~>! p(anonTravs1)         ^^  { ts => Union(ts) }
    ).withFailureMessage("malformed traversal step")
  }
  private lazy val traversalStep1 = rep1sep(traversalStep, `.`)

  private lazy val anonTrav: PackratParser[Traversal] =
    opt(Underscore ~! `.`) ~> traversalStep1 ^^ { ts => Traversal(ts) }
  private lazy val anonTravs1 = rep1sep(anonTrav, `,`)

  private lazy val query: PackratParser[Query] =
    ( "g" ~>! (`.` ~>! traversalStep).+           ^^ { ts => FinalTraversal(Traversal(ts)) }
    | ident ~! (`=` ~>! value) ~! (`;` ~>! query) ^^ { case x ~ v ~ q => AssignLiteral(Symbol(x), v, q) }
    ).withFailureMessage("malformed query")

  /** Try to parse a query from an input string. */
  @throws[QuineGremlinException]("if the Gremlin query cannot be parsed")
  def parseQuery(input: Input): Query = phrase(query)(input) match {
    case Success(matched, _) => matched
    case NoSuccess(_, in) if in.first == Err => throw LexicalError(in.pos)
    case NoSuccess(msg, in) => throw ParseError(msg, Some(in.pos))
    case _ => throw ParseError("Parser failed in an unexpected way", None)
  }
}

package com.thatdot.quine.language.functions

/** The canonical names of the standard openCypher functions Quine recognizes, mirrored here so
  * the language server can offer them as completions.
  *
  * == Where the truth lives ==
  *
  * The scalar built-ins are declared in quine-core as `Func.builtinFunctions`
  * (`com.thatdot.quine.graph.cypher.Func`), whose `name` strings — `abs`, `coalesce`,
  * `toInteger`, ... — are the authoritative spellings reproduced here. quine-language is
  * upstream of quine-core, so it cannot read that vector directly;
  * `BuiltinFunctionNamesSyncTest` in quine-cypher (which sees both modules) pins this list to
  * `Func.builtinFunctions.map(_.name)` so a scalar function added, removed, or renamed there
  * fails that test until it is mirrored here.
  *
  * The aggregating functions (`count`, `collect`, `sum`, `avg`, `min`, `max`, `stdev`,
  * `stdevp`, `percentileCont`, `percentileDisc`) are not in `Func.builtinFunctions` — they are
  * recognized by the openCypher frontend rather than declared as `BuiltinFunc`s — so they are
  * listed here by hand and pinned against a hardcoded allow-set in the same sync test.
  *
  * Quine's own additions (`idFrom`, `reify.time`, ...) are not duplicated here; they come from
  * [[FunctionDocRegistry]] and [[com.thatdot.quine.language.procedures.ProcedureDocRegistry]],
  * which also carry signatures and descriptions.
  */
object BuiltinFunctionNames {

  /** Standard openCypher function names, in their canonical (mixed) case, sorted
    * case-insensitively. Scalar built-ins mirror `Func.builtinFunctions`; the rest are the
    * aggregating functions (see the object documentation).
    */
  val all: Vector[String] = Vector(
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "avg",
    "ceil",
    "coalesce",
    "collect",
    "cos",
    "cot",
    "count",
    "degrees",
    "e",
    "exp",
    "floor",
    "haversin",
    "head",
    "id",
    "keys",
    "labels",
    "last",
    "left",
    "length",
    "log",
    "log10",
    "lTrim",
    "max",
    "min",
    "nodes",
    "percentileCont",
    "percentileDisc",
    "pi",
    "properties",
    "radians",
    "rand",
    "range",
    "relationships",
    "replace",
    "reverse",
    "right",
    "round",
    "rTrim",
    "sign",
    "sin",
    "size",
    "split",
    "sqrt",
    "stdev",
    "stdevp",
    "substring",
    "sum",
    "tail",
    "tan",
    "timestamp",
    "toBoolean",
    "toFloat",
    "toInteger",
    "toLower",
    "toString",
    "toUpper",
    "trim",
    "type",
  )
}

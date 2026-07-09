package com.thatdot.quine.compiler.cypher

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.Func
import com.thatdot.quine.language.functions.BuiltinFunctionNames

/** Asserts that quine-language's [[BuiltinFunctionNames]] mirrors the standard openCypher
  * functions Quine recognizes.
  *
  * quine-language is upstream of quine-core, so the language server's completion list cannot
  * read `Func.builtinFunctions` directly; it consults the mirrored name list instead. This
  * suite (which can see both modules) pins the mirror in both directions:
  *
  *   - every scalar built-in `Func.builtinFunctions` declares has an entry in
  *     [[BuiltinFunctionNames]], so a scalar function added or renamed there fails here until
  *     the mirror is updated;
  *   - every extra name in [[BuiltinFunctionNames]] is one of the aggregating functions, which
  *     are recognized by the openCypher frontend rather than declared as `BuiltinFunc`s and so
  *     are absent from `Func.builtinFunctions`. They are pinned against the hardcoded
  *     [[aggregateAllowSet]]; a name that is neither a scalar built-in nor a known aggregate
  *     fails here.
  */
class BuiltinFunctionNamesSyncTest extends AnyFunSuite {

  /** The aggregating functions Quine recognizes that are not in `Func.builtinFunctions`. This
    * set is maintained by hand: aggregates are resolved by the openCypher frontend, so there is
    * no runtime vector to pin them against. Update it when Quine's aggregate support changes.
    */
  private val aggregateAllowSet: Set[String] =
    Set("count", "collect", "sum", "avg", "min", "max", "stdev", "stdevp", "percentileCont", "percentileDisc")

  test("every scalar built-in function name is mirrored in BuiltinFunctionNames") {
    val runtimeNames = Func.builtinFunctions.map(_.name).toSet
    val mirrored = BuiltinFunctionNames.all.toSet
    val missing = runtimeNames -- mirrored
    assert(missing.isEmpty, s"BuiltinFunctionNames is missing scalar built-ins: ${missing.mkString(", ")}")
  }

  test("every extra name in BuiltinFunctionNames is a known aggregate") {
    val runtimeNames = Func.builtinFunctions.map(_.name).toSet
    val extras = BuiltinFunctionNames.all.toSet -- runtimeNames
    val unexpected = extras -- aggregateAllowSet
    assert(
      unexpected.isEmpty,
      s"BuiltinFunctionNames has names that are neither scalar built-ins nor known aggregates: " +
      unexpected.mkString(", "),
    )
  }

  test("every allow-listed aggregate is present in BuiltinFunctionNames") {
    val mirrored = BuiltinFunctionNames.all.toSet
    val missingAggregates = aggregateAllowSet -- mirrored
    assert(
      missingAggregates.isEmpty,
      s"BuiltinFunctionNames is missing aggregates: ${missingAggregates.mkString(", ")}",
    )
  }

  test("BuiltinFunctionNames has no duplicate names") {
    val duplicates =
      BuiltinFunctionNames.all.groupBy(identity).collect { case (name, occurrences) if occurrences.length > 1 => name }
    assert(duplicates.isEmpty, s"BuiltinFunctionNames lists duplicates: ${duplicates.mkString(", ")}")
  }
}

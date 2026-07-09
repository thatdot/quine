package com.thatdot.quine.compiler.cypher

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.language.functions.FunctionDocRegistry

/** Asserts that quine-language's [[FunctionDocRegistry]] mirrors the Quine-specific Cypher
  * functions this module registers.
  *
  * quine-language is upstream of quine-cypher, so the language server's hover documentation
  * cannot read [[resolveFunctions.additionalFeatures]] directly; it consults the mirrored
  * registry instead. This suite pins the mirror to the runtime registrations name-by-name:
  * adding, removing, or renaming a custom function fails here until the registry is updated
  * to match, so hover entries always name functions users can actually call.
  */
class FunctionDocRegistrySyncTest extends AnyFunSuite {

  test("every registered Quine-specific function has a documentation entry under its canonical name") {
    resolveFunctions.additionalFeatures.foreach { function =>
      FunctionDocRegistry.lookup(function.name) match {
        case None =>
          fail(s"FunctionDocRegistry has no entry for the registered function `${function.name}`")
        case Some(doc) =>
          assert(
            doc.name == function.name,
            s"FunctionDocRegistry records the canonical name `${doc.name}` for `${function.name}`",
          )
      }
    }
  }

  test("every documentation entry names a registered function") {
    val registeredNames = resolveFunctions.additionalFeatures.map(_.name.toLowerCase).toSet
    FunctionDocRegistry.all.foreach { doc =>
      assert(
        registeredNames.contains(doc.name.toLowerCase),
        s"FunctionDocRegistry documents `${doc.name}`, which the runtime does not register",
      )
    }
  }

  test("function names resolve case-insensitively in both registries") {
    // Func.userDefinedFunctions keys are lowercased (runtime resolution lowercases the
    // invoked name), so the documentation registry's lookup must be case-insensitive too.
    resolveFunctions.additionalFeatures.foreach { function =>
      assert(
        FunctionDocRegistry.lookup(function.name.toUpperCase).isDefined,
        s"FunctionDocRegistry.lookup is not case-insensitive for `${function.name}`",
      )
    }
  }
}

package com.thatdot.quine.compiler.cypher

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.language.functions.FunctionDocRegistry
import com.thatdot.quine.language.procedures.{ProcedureDocRegistry, ProcedureRegistry}

/** Asserts that quine-language's [[ProcedureDocRegistry]] mirrors the procedures Quine ships.
  *
  * quine-language is upstream of quine-cypher, so the language server's hover documentation
  * cannot read [[resolveCalls.builtInProcedures]] directly; it consults the mirrored registry
  * instead. The pin is two-fold:
  *
  *   - every built-in procedure this module declares has a documentation entry under its
  *     canonical name, so adding or renaming a built-in fails here until the documentation is
  *     updated to match;
  *   - the documentation registry names exactly the procedures [[ProcedureRegistry]] names, so the
  *     two quine-language mirrors cannot drift from each other.
  *
  * These suites only pin procedures declared in this module (`ProcedureRegistrySyncTest` and this
  * one iterate [[resolveCalls.builtInProcedures]]). Procedures registered outside this module — the
  * app-startup registrations (`standing.wiretap`, `parseProtobuf`, `toProtobuf`) and `help.builtins`
  * (registered in quine-core's `QuinePatternProcedureRegistry`) — are pinned instead by
  * `AppRegisteredProcedureSyncTest` in the `quine` module.
  */
class ProcedureDocRegistrySyncTest extends AnyFunSuite {

  test("every built-in procedure has a documentation entry under its canonical name") {
    resolveCalls.builtInProcedures.foreach { procedure =>
      ProcedureDocRegistry.lookup(procedure.name) match {
        case None =>
          fail(s"ProcedureDocRegistry has no entry for the built-in procedure `${procedure.name}`")
        case Some(doc) =>
          assert(
            doc.name == procedure.name,
            s"ProcedureDocRegistry records the canonical name `${doc.name}` for `${procedure.name}`",
          )
      }
    }
  }

  test("the documentation registry names exactly the procedures ProcedureRegistry names") {
    val documentedNames = ProcedureDocRegistry.all.map(_.name).toSet
    val registeredNames = ProcedureRegistry.all.map(_.name).toSet
    val undocumented = registeredNames -- documentedNames
    val unregistered = documentedNames -- registeredNames
    assert(
      undocumented.isEmpty,
      s"ProcedureRegistry procedures with no documentation entry: ${undocumented.mkString(", ")}",
    )
    assert(
      unregistered.isEmpty,
      s"ProcedureDocRegistry documents procedures ProcedureRegistry does not name: ${unregistered.mkString(", ")}",
    )
  }

  test("procedure names resolve case-insensitively in the documentation registry") {
    // resolveCalls.rewriteCall lowercases the call's procedure name before lookup; the
    // documentation registry must resolve the same spellings.
    resolveCalls.builtInProcedures.foreach { procedure =>
      assert(
        ProcedureDocRegistry.lookup(procedure.name.toUpperCase).isDefined,
        s"ProcedureDocRegistry does not resolve `${procedure.name.toUpperCase}` case-insensitively",
      )
    }
  }

  test("function and procedure documentation registries share no names") {
    // The hover lookup consults FunctionDocRegistry before ProcedureDocRegistry; the runtime
    // resolves functions and procedures in separate namespaces, so the registries must stay
    // disjoint for the lookup order to never select between two candidates.
    val functionNames = FunctionDocRegistry.all.map(_.name.toLowerCase).toSet
    val procedureNames = ProcedureDocRegistry.all.map(_.name.toLowerCase).toSet
    val shared = functionNames.intersect(procedureNames)
    assert(shared.isEmpty, s"names documented as both a function and a procedure: ${shared.mkString(", ")}")
  }
}

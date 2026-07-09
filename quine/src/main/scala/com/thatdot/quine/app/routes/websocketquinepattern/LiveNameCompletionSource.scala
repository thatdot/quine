package com.thatdot.quine.app.routes.websocketquinepattern

import com.thatdot.quine.compiler.cypher.{resolveCalls, resolveFunctions}
import com.thatdot.quine.graph.cypher.{Func, Proc}
import com.thatdot.quine.language.functions.FunctionDocRegistry
import com.thatdot.quine.language.procedures.ProcedureDocRegistry
import com.thatdot.quine.language.server.{CompletionKind, CompletionName, NameCompletionSource}

/** A [[NameCompletionSource]] backed by the running graph's live Cypher registries.
  *
  * Reads the runtime function/procedure registries on each request, so the editor offers every
  * function and procedure the running instance knows — Quine's scalar built-ins, the functions and
  * procedures it registers at startup (`idFrom`, `reify.time`, ...), and any user-defined functions
  * or procedures registered at runtime.
  *
  * Curated documentation from [[FunctionDocRegistry]]/[[ProcedureDocRegistry]] is overlaid by name
  * where present (its quine.io-sourced text is richer than the runtime `description`); names the
  * curated registries do not cover fall back to the runtime signature and description.
  *
  * openCypher's aggregating functions (`count`, `collect`, ...) are recognized by the openCypher
  * frontend, not declared in `Func.builtinFunctions`, so they are in no runtime registry and are
  * listed here explicitly. They are fixed by the openCypher spec and do not change with Quine.
  */
object LiveNameCompletionSource extends NameCompletionSource {

  // Quine's built-in functions and procedures register into Func/Proc as a side effect of these
  // compiler rewriters' initialization, which otherwise happens only when the first query is
  // compiled. Referencing them forces that, so the editor offers those names even before any query
  // has run.
  private def forceCompilerRegistrations(): Unit = {
    val _ = resolveFunctions
    val _ = resolveCalls
  }
  forceCompilerRegistrations()

  /** openCypher aggregating functions, recognized by the frontend rather than declared in
    * `Func.builtinFunctions`. Fixed by the openCypher spec.
    */
  private val aggregatingFunctions: Vector[String] =
    Vector("avg", "collect", "count", "max", "min", "percentileCont", "percentileDisc", "stdev", "stdevp", "sum")

  def names(): Seq[CompletionName] = {
    // User-defined (incl. Quine's own, registered as UDFs) first so a documented entry wins the
    // de-dup in nameCompletions over a same-named bare built-in or aggregate.
    val userFunctions: Vector[CompletionName] =
      Func.userDefinedFunctions.values.toVector.map { f =>
        val doc = FunctionDocRegistry.lookup(f.name)
        CompletionName(
          label = f.name,
          kind = CompletionKind.Function,
          detail = doc.map(_.signature).orElse(f.signatures.headOption.map(_.pretty(f.name))),
          documentation = doc.map(_.description).orElse(f.signatures.headOption.map(_.description)),
        )
      }

    val userProcedures: Vector[CompletionName] =
      Proc.userDefinedProcedures.values.toVector.map { p =>
        val doc = ProcedureDocRegistry.lookup(p.name)
        CompletionName(
          label = p.name,
          kind = CompletionKind.Procedure,
          detail = doc.map(_.signature).orElse(Some(p.signature.pretty(p.name))),
          documentation = doc.map(_.description).orElse(Some(p.signature.description)),
        )
      }

    val builtinFunctions: Vector[CompletionName] =
      Func.builtinFunctions.map(f =>
        CompletionName(f.name, CompletionKind.Function, Some(f.signature), Some(f.description)),
      )

    val aggregates: Vector[CompletionName] =
      aggregatingFunctions.map(name => CompletionName(name, CompletionKind.Function, None, None))

    userFunctions ++ userProcedures ++ builtinFunctions ++ aggregates
  }
}

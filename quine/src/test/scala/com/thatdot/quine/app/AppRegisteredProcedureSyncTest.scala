package com.thatdot.quine.app

import scala.annotation.nowarn

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.model.ingest.serialization.{CypherParseProtobuf, CypherToProtobuf}
import com.thatdot.quine.compiler.cypher.CypherStandingWiretap
import com.thatdot.quine.graph.cypher.quinepattern.procedures.HelpBuiltinsProcedure
import com.thatdot.quine.language.procedures.{ProcedureDocRegistry, ProcedureRegistry}
import com.thatdot.quine.serialization.ProtobufSchemaCache

/** Pins the procedures Quine registers outside quine-cypher's `builtInProcedures` against
  * quine-language's [[ProcedureRegistry]] and [[ProcedureDocRegistry]] mirrors.
  *
  * `ProcedureRegistrySyncTest`/`ProcedureDocRegistrySyncTest` both iterate `builtInProcedures`, so
  * neither sees these — renaming one would leave both mirrors stale while every quine-cypher sync
  * test stayed green. This suite closes that gap by reading each procedure's runtime `name`:
  *
  *   - `standing.wiretap`, `parseProtobuf`, and `toProtobuf` need app/graph state, so [[QuineApp]]
  *     registers them at startup rather than in `builtInProcedures`;
  *   - `help.builtins` is registered in quine-core's `QuinePatternProcedureRegistry`
  *     ([[HelpBuiltinsProcedure]]), not in `builtInProcedures`. (The legacy `CypherBuiltinFunctions`
  *     definition of the same name in quine-cypher is never added to `builtInProcedures` either.)
  */
class AppRegisteredProcedureSyncTest extends AnyFunSuite {

  /** The runtime names of procedures registered outside `builtInProcedures`, read from their
    * runtime registrations so a rename fails here. The three app-registered procedures are
    * constructed as [[QuineApp]] constructs them; the constructor arguments do not affect the
    * `name` (`ProtobufSchemaCache.Blocking` and a no-op standing-query lookup are inert
    * placeholders).
    */
  private val pinnedProcedureNames: List[String] = List(
    new CypherParseProtobuf(ProtobufSchemaCache.Blocking: @nowarn("cat=deprecation")).name,
    new CypherToProtobuf(ProtobufSchemaCache.Blocking: @nowarn("cat=deprecation")).name,
    new CypherStandingWiretap((_, _) => None).name,
    HelpBuiltinsProcedure.name,
  )

  test("every runtime-registered procedure's name is mirrored in quine-language's ProcedureRegistry") {
    pinnedProcedureNames.foreach { name =>
      assert(
        ProcedureRegistry.lookup(name).exists(_.name == name),
        s"ProcedureRegistry has no entry mirroring runtime-registered procedure `$name`",
      )
    }
  }

  test("every runtime-registered procedure has a quine-language documentation entry under its name") {
    pinnedProcedureNames.foreach { name =>
      assert(
        ProcedureDocRegistry.lookup(name).exists(_.name == name),
        s"ProcedureDocRegistry has no entry mirroring runtime-registered procedure `$name`",
      )
    }
  }
}

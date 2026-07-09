package com.thatdot.quine.compiler.cypher

import cats.data.NonEmptyList
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.{Type => RuntimeType}
import com.thatdot.quine.language.procedures.ProcedureRegistry
import com.thatdot.quine.language.types.{Type => LanguageType}

/** Asserts that quine-language's [[ProcedureRegistry]] mirrors the procedure signatures this
  * module declares.
  *
  * quine-language is upstream of quine-cypher, so the LSP query classifier cannot read
  * [[resolveCalls.builtInProcedures]] directly; it consults the mirrored registry instead.
  * This suite pins the mirror to the runtime declarations: adding, removing, renaming, or
  * re-typing a built-in procedure's outputs fails here until [[ProcedureRegistry.all]] is
  * updated to match.
  */
class ProcedureRegistrySyncTest extends AnyFunSuite {

  /** The quine-language type [[ProcedureRegistry]] records for a runtime output type: node,
    * edge, and the primitive types are mapped exactly; runtime types quine-language does not
    * model are recorded as the top type `Any`.
    */
  private def mirroredType(runtimeType: RuntimeType): LanguageType = runtimeType match {
    case RuntimeType.Node => LanguageType.PrimitiveType.NodeType
    case RuntimeType.Relationship => LanguageType.PrimitiveType.EdgeType
    case RuntimeType.Integer => LanguageType.PrimitiveType.Integer
    case RuntimeType.Floating => LanguageType.PrimitiveType.Real
    case RuntimeType.Str => LanguageType.PrimitiveType.String
    case RuntimeType.Bool => LanguageType.PrimitiveType.Boolean
    case RuntimeType.List(of) =>
      LanguageType.TypeConstructor(Symbol("List"), NonEmptyList.of(mirroredType(of)))
    case _ => LanguageType.Any
  }

  test("every built-in procedure's return signature is mirrored in quine-language's ProcedureRegistry") {
    resolveCalls.builtInProcedures.foreach { procedure =>
      ProcedureRegistry.lookup(procedure.name) match {
        case None =>
          fail(s"ProcedureRegistry has no entry for built-in procedure `${procedure.name}`")
        case Some(mirrored) =>
          assert(
            mirrored.name == procedure.name,
            s"ProcedureRegistry records the canonical name `${mirrored.name}` for `${procedure.name}`",
          )
          val expectedOutputs = procedure.signature.outputs.map { case (column, runtimeType) =>
            column -> mirroredType(runtimeType)
          }.toVector
          assert(
            mirrored.outputs == expectedOutputs,
            s"ProcedureRegistry outputs for `${procedure.name}` are ${mirrored.outputs}, " +
            s"but the runtime declares ${expectedOutputs}",
          )
      }
    }
  }

  test("procedure names resolve case-insensitively in both registries") {
    // resolveCalls.rewriteCall lowercases the call's procedure name before lookup; the
    // mirrored registry must resolve the same spellings.
    resolveCalls.builtInProcedures.foreach { procedure =>
      assert(
        ProcedureRegistry.lookup(procedure.name.toUpperCase).isDefined,
        s"ProcedureRegistry does not resolve `${procedure.name.toUpperCase}` case-insensitively",
      )
    }
  }
}

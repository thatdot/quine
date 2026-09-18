package com.thatdot.quine.language.procedures

import munit.FunSuite

import com.thatdot.quine.language.types.Type.PrimitiveType

class ProcedureRegistryTest extends FunSuite {

  test("a signature registered at runtime resolves case-insensitively and appears in all") {
    val signature = ProcedureSignature("test.registeredAtRuntime", Vector("node" -> PrimitiveType.NodeType))
    assertEquals(ProcedureRegistry.lookup(signature.name), None)

    ProcedureRegistry.register(signature)

    assertEquals(ProcedureRegistry.lookup("TEST.REGISTEREDATRUNTIME"), Some(signature))
    assertEquals(ProcedureRegistry.outputType(signature.name, "node"), Some(PrimitiveType.NodeType))
    assert(ProcedureRegistry.all.contains(signature))
  }
}

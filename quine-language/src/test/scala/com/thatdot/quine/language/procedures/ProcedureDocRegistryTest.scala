package com.thatdot.quine.language.procedures

import munit.FunSuite

class ProcedureDocRegistryTest extends FunSuite {

  test("documentation registered at runtime resolves case-insensitively and appears in all") {
    val doc = ProcedureDoc(
      name = "test.registeredAtRuntime",
      signature = "test.registeredAtRuntime() :: (node :: NODE)",
      description = "Registered by the test.",
      docsUrl = "https://example.invalid/test",
    )
    assertEquals(ProcedureDocRegistry.lookup(doc.name), None)

    ProcedureDocRegistry.register(doc)

    assertEquals(ProcedureDocRegistry.lookup("TEST.REGISTEREDATRUNTIME"), Some(doc))
    assert(ProcedureDocRegistry.all.contains(doc))
  }
}

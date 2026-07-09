package com.thatdot.quine.language.functions

import munit.FunSuite

class FunctionDocRegistryTest extends FunSuite {

  test("idFrom resolves to a documented entry with signature, description, and docs link") {
    val doc = FunctionDocRegistry.lookup("idFrom").getOrElse(fail("idFrom is not in the registry"))
    assertEquals(doc.name, "idFrom")
    assert(doc.signature.startsWith("idFrom("), s"signature is a function signature: ${doc.signature}")
    assert(doc.description.nonEmpty)
    assertEquals(doc.docsUrl, "https://quine.io/core-concepts/id-provider/#idfrom")
  }

  test("lookup is case-insensitive, matching the runtime's lowercased function resolution") {
    val expected = FunctionDocRegistry.lookup("idFrom")
    assert(expected.isDefined)
    assertEquals(FunctionDocRegistry.lookup("IDFROM"), expected)
    assertEquals(FunctionDocRegistry.lookup("idfrom"), expected)
    assertEquals(FunctionDocRegistry.lookup("text.SPLIT"), FunctionDocRegistry.lookup("text.split"))
  }

  test("names that are not Quine-specific functions have no entry") {
    assertEquals(FunctionDocRegistry.lookup("definitelyNotAFunction"), None)
    // Standard Cypher functions are not Quine additions, so they are not documented here.
    assertEquals(FunctionDocRegistry.lookup("count"), None)
    assertEquals(FunctionDocRegistry.lookup("id"), None)
  }

  test("the registry covers every Quine-specific function family documented on quine.io") {
    // One spot check per family of `resolveFunctions.additionalFeatures` (quine-cypher);
    // FunctionDocRegistrySyncTest in quine-cypher pins the registry entry-by-entry.
    val expected = List(
      "idFrom",
      "locIdFrom",
      "quineId",
      "strId",
      "bytes",
      "convert.stringToBytes",
      "hash",
      "kafkaHash",
      "getHost",
      "toJson",
      "parseJson",
      "text.utf8Decode",
      "text.utf8Encode",
      "map.fromPairs",
      "map.sortedProperties",
      "map.removeKey",
      "map.merge",
      "map.dropNullValues",
      "text.split",
      "text.regexFirstMatch",
      "text.regexGroups",
      "text.regexReplaceAll",
      "text.urlencode",
      "text.urldecode",
      "datetime",
      "localdatetime",
      "date",
      "time",
      "localtime",
      "duration",
      "duration.between",
      "temporal.format",
      "coll.max",
      "coll.min",
      "meta.type",
      "gen.string.from",
      "gen.integer.from",
      "gen.float.from",
      "gen.boolean.from",
      "gen.bytes.from",
      "gen.node.from",
      "castOrThrow.integer",
      "castOrThrow.list",
      "castOrNull.string",
      "castOrNull.localdatetime",
    )
    expected.foreach { name =>
      assert(FunctionDocRegistry.lookup(name).isDefined, s"registry has no entry for `$name`")
    }
  }

  test("every entry has a well-formed signature, description, docs URL, and markdown body") {
    assert(FunctionDocRegistry.all.nonEmpty)
    FunctionDocRegistry.all.foreach { doc =>
      assert(doc.name.nonEmpty)
      doc.signature.linesIterator.foreach { signatureLine =>
        assert(
          signatureLine.startsWith(s"${doc.name}("),
          s"every signature line of `${doc.name}` starts with the function name: $signatureLine",
        )
      }
      assert(doc.description.trim.nonEmpty, s"`${doc.name}` has a description")
      assert(doc.docsUrl.startsWith("https://quine.io/"), s"`${doc.name}` links to quine.io: ${doc.docsUrl}")
      assert(doc.markdown.contains(doc.signature), s"`${doc.name}` markdown contains the signature")
      assert(doc.markdown.contains(doc.description), s"`${doc.name}` markdown contains the description")
      assert(
        doc.markdown.contains(s"[Documentation](${doc.docsUrl})"),
        s"`${doc.name}` markdown links to the documentation",
      )
    }
  }

  test("entry names are unique case-insensitively") {
    val lowerCaseNames = FunctionDocRegistry.all.map(_.name.toLowerCase)
    assertEquals(lowerCaseNames.distinct.length, lowerCaseNames.length)
  }
}

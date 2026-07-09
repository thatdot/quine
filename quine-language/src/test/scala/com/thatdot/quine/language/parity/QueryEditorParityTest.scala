package com.thatdot.quine.language.parity

import java.io.File
import java.nio.file.Files

import scala.io.Source

import com.thatdot.quine.language.semantic.SemanticType
import com.thatdot.quine.language.server.QueryKind

/** Guards the deliberate type duplication between the Quine language server (the source of
  * truth, in this module) and the in-tree query editor package
  * (`public/query-editor`, TypeScript). The editor cannot depend on this JVM module, so a
  * handful of constants are restated in TypeScript; this test fails if they drift.
  *
  * Only the names/values are asserted to match — not their ordering. The editor reads the
  * authoritative semantic-token legend (with its index ordering) from the server's LSP
  * `initialize` result at runtime, so the TypeScript list exists purely to enumerate the
  * theme-rule names.
  */
class QueryEditorParityTest extends munit.FunSuite {

  /** Locate a repo-relative path by walking up from `startDir`, so the test is robust to whether
    * sbt runs it from the repo root or the module directory.
    *
    * Copybara strips the `public/` prefix when this repo is pushed to `thatdot/quine` (OSS), so a
    * file the call sites request as `public/query-editor/...` lives at `query-editor/...` there.
    * Each ancestor is probed both as given and with `public/` stripped, so the test passes under
    * either layout. Call sites keep the `public/`-prefixed strings; this resolver absorbs the rest.
    */
  private def repoFile(relative: String, startDir: File = new File(".").getCanonicalFile): File = {
    val candidates = Seq(relative, relative.stripPrefix("public/")).distinct
    var dir = startDir
    while (dir != null)
      candidates.map(new File(dir, _)).find(_.isFile) match {
        case Some(file) => return file
        case None => dir = dir.getParentFile
      }
    fail(s"Could not locate $relative (or its public/-stripped form) by walking up from $startDir")
  }

  private def readFile(relative: String): String = {
    val src = Source.fromFile(repoFile(relative), "UTF-8")
    try src.mkString
    finally src.close()
  }

  test("repoFile resolves a public/-prefixed path against the copybara-stripped (OSS) layout") {
    // Stage the OSS layout (no `public/` prefix) in a temp dir, then resolve the `public/`-form.
    val tmp = Files.createTempDirectory("parity-copybara").toFile
    try {
      val ossFile = new File(tmp, "query-editor/src/tokenLegend.ts")
      ossFile.getParentFile.mkdirs()
      Files.write(ossFile.toPath, "// staged".getBytes("UTF-8"))

      val located = repoFile("public/query-editor/src/tokenLegend.ts", tmp)
      assertEquals(located.getCanonicalFile, ossFile.getCanonicalFile)
    } finally Files.walk(tmp.toPath).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.delete(p))
  }

  test("tokenLegend.ts lists exactly the SemanticType names the server advertises") {
    val ts = readFile("public/query-editor/src/tokenLegend.ts")
    // Every legend entry is an object literal `{ name: "SomeName", ... }`.
    val tsNames = """name:\s*"([^"]+)"""".r.findAllMatchIn(ts).map(_.group(1)).toSet
    val serverNames = SemanticType.semanticTypes.map(_.toString).toSet

    assertEquals(
      tsNames,
      serverNames,
      "public/query-editor/src/tokenLegend.ts has drifted from SemanticType in Semantics.scala. " +
      s"Only in TypeScript: ${tsNames -- serverNames}; only in Scala: ${serverNames -- tsNames}.",
    )
  }

  test("queryKind.ts union lists exactly the QueryKind verdicts the server returns") {
    val ts = readFile("public/query-editor/src/queryKind.ts")
    // Extract the elements of `const QUERY_KINDS = ["node", "table", ...] as const;` — the single
    // source of truth from which the `QueryKind` union type and `isQueryKind` guard are derived.
    val arrayLiteral = """const\s+QUERY_KINDS\s*=\s*\[([^\]]+)\]""".r
      .findFirstMatchIn(ts)
      .getOrElse(fail("Could not find the `const QUERY_KINDS = [...]` declaration in queryKind.ts"))
      .group(1)
    val tsKinds = """"([^"]+)"""".r.findAllMatchIn(arrayLiteral).map(_.group(1)).toSet
    // The wire token each verdict serializes to (QueryKind owns it via @SerializedName), derived
    // from the enum so a newly added verdict is covered without editing this test.
    val serverKinds = QueryKind.values().iterator.map(_.wireValue()).toSet

    assertEquals(
      tsKinds,
      serverKinds,
      "public/query-editor/src/queryKind.ts has drifted from QueryKind.java. " +
      s"Only in TypeScript: ${tsKinds -- serverKinds}; only in Java: ${serverKinds -- tsKinds}.",
    )
  }
}

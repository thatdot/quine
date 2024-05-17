package com.thatdot.quine.app.ingest.serialization

import cats.implicits.toFunctorOps

import com.thatdot.quine.app.ingest.serialization.ProtobufTest._
import com.thatdot.quine.compiler.cypher.{CypherHarness, registerUserDefinedProcedure}
import com.thatdot.quine.graph.cypher.Expr

class CypherProtobufConversionsTest extends CypherHarness("procedure-parse-protobuf") {
  registerUserDefinedProcedure(new CypherParseProtobuf(testSchemaCache))
  registerUserDefinedProcedure(new CypherToProtobuf(testSchemaCache))

  val testPersonBytes: Expr.Bytes = Expr.Bytes(bytesFromURL(testPersonFile))
  val testReadablePersonCypher: Expr.Map = Expr.Map(
    testReadablePerson
      .fmap(Expr.fromQuineValue)
  )
  val testAnyZoneBytes: Expr.Bytes = Expr.Bytes(bytesFromURL(testAnyZone))

  describe("saving protobuf bytes as a property") {
    val query =
      """
         |MATCH (p) WHERE id(p) = idFrom("procedure-parse-protobuf", "bob")
         |SET p:Person,
         |    p.protobuf = $personBytes
         |WITH id(p) AS pId
         |MATCH (p) WHERE id(p) = pId
         |RETURN p.protobuf AS pbBytes
         |""".stripMargin

    testQuery(
      queryText = query,
      parameters = Map("personBytes" -> testPersonBytes),
      expectedColumns = Vector("pbBytes"),
      expectedRows = Seq(Vector(testPersonBytes)),
      expectedIsReadOnly = false,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
  }

  describe("parseProtobuf procedure") {
    // from an in-memory value
    testQuery(
      """CALL parseProtobuf($personBytes, $schemaUrl, "Person") YIELD value RETURN value AS personDeserialized""",
      parameters = Map("personBytes" -> testPersonBytes, "schemaUrl" -> Expr.Str(addressBookSchemaFile.toString)),
      expectedColumns = Vector("personDeserialized"),
      expectedRows = Seq(Vector(testReadablePersonCypher)),
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    // from a property
    testQuery(
      """
        |MATCH (p) WHERE id(p) = idFrom("procedure-parse-protobuf", "bob")
        |CALL parseProtobuf(p.protobuf, $schemaUrl, "Person") YIELD value RETURN value AS personDeserialized""".stripMargin,
      parameters = Map("personBytes" -> testPersonBytes, "schemaUrl" -> Expr.Str(addressBookSchemaFile.toString)),
      expectedColumns = Vector("personDeserialized"),
      expectedRows = Seq(Vector(testReadablePersonCypher)),
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
    // from an invalid value
    testQuery(
      """CALL parseProtobuf($invalidBytes, $schemaUrl, "Person") YIELD value RETURN value AS personDeserialized""",
      parameters = Map(
        "invalidBytes" -> testPersonBytes.copy(b = testPersonBytes.b.updated(2, 0xFF.toByte)),
        "schemaUrl" -> Expr.Str(addressBookSchemaFile.toString)
      ),
      expectedColumns = Vector("personDeserialized"),
      expectedRows = Seq(Vector(Expr.Null)),
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
  }
  describe("toProtobuf procedure") {
    testQuery(
      """CALL toProtobuf(
        |  $anyZoneCypher,
        |  $schemaUrl,
        |  "com.thatdot.test.azeroth.expansions.cataclysm.AnyZone"
        |) YIELD protoBytes
        |RETURN protoBytes AS personSerialized""".stripMargin,
      parameters = Map("anyZoneCypher" -> testAnyZoneCypher, "schemaUrl" -> Expr.Str(warcraftSchemaFile.toString)),
      expectedColumns = Vector("personSerialized"),
      expectedRows = Seq(Vector(testAnyZoneBytes)),
      expectedIsReadOnly = true,
      expectedCannotFail = false,
      expectedIsIdempotent = true,
      expectedCanContainAllNodeScan = false
    )
  }
}

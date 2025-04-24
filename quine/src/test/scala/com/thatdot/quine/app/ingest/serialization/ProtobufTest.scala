package com.thatdot.quine.app.ingest.serialization

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Try, Using}

import cats.implicits._
import com.google.common.io.ByteStreams
import com.google.protobuf.Descriptors
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest.serialization.ProtobufTest.{
  addressBookSchemaFile,
  bytesFromURL,
  testAnyZone,
  testAnyZoneCypher,
  testAzerothZone,
  testCataclysmZone1,
  testPerson,
  testPersonFile,
  testReadablePerson,
  testSchemaCache,
  testWritablePerson,
  warcraftSchemaFile,
}
import com.thatdot.quine.app.ingest2.core.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.model.ingest.serialization.ProtobufParser
import com.thatdot.quine.app.serialization.ProtobufSchemaError.{
  AmbiguousMessageType,
  InvalidProtobufSchema,
  NoSuchMessageType,
  UnreachableProtobufSchema,
}
import com.thatdot.quine.app.serialization.{ProtobufSchemaCache, ProtobufSchemaError, QuineValueToProtobuf}
import com.thatdot.quine.graph.cypher.Expr.toQuineValue
import com.thatdot.quine.graph.cypher.{CypherException, Expr, Value}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.util.MonadHelpers._

// See also [[CypherParseProtobufTest]] for the UDP interface to this functionality
class ProtobufTest extends AnyFunSpecLike with Matchers with EitherValues {

  @throws[ProtobufSchemaError]
  private def parserFor(schemaUrl: URL, typeName: String): ProtobufParser = {
    val desc = Await.result(testSchemaCache.getMessageDescriptor(schemaUrl, typeName, flushOnFail = true), Duration.Inf)
    new ProtobufParser(desc)
  }

  val testEnvironmentCanMakeWebRequests = true

  describe("ProtobufParser") {
    it("should fail to construct a parser for an invalid schema file") {
      an[InvalidProtobufSchema] should be thrownBy {
        parserFor(testAzerothZone, "anything")
      }

      if (testEnvironmentCanMakeWebRequests) {
        an[InvalidProtobufSchema] should be thrownBy {
          parserFor(new URL("https://httpbounce.dev.thatdot.com/200"), "NoSuchType")
        }
      }
    }
    it("should fail to construct a parser for an unreachable or unreadable schema file") {
      an[UnreachableProtobufSchema] should be thrownBy {
        parserFor(new URL("file:///thisfile_does_notexist.txt"), "NoSuchType")
      }

      if (testEnvironmentCanMakeWebRequests) {
        an[UnreachableProtobufSchema] should be thrownBy {
          parserFor(new URL("https://httpbounce.dev.thatdot.com/401"), "NoSuchType")
        }
        an[UnreachableProtobufSchema] should be thrownBy {
          parserFor(new URL("https://httpbounce.dev.thatdot.com/403"), "NoSuchType")
        }
        an[UnreachableProtobufSchema] should be thrownBy {
          parserFor(new URL("https://httpbounce.dev.thatdot.com/404"), "NoSuchType")
        }
      }
    }
    it("should fail to construct a parser for a non-existent type, offering the full list of available types") {

      val error = the[NoSuchMessageType] thrownBy {
        parserFor(addressBookSchemaFile, "NoSuchType")
      }

      error.validTypes should contain theSameElementsAs (Seq(
        "tutorial.AddressBook",
        "tutorial.Person",
        "tutorial.Person.MapFieldEntry",
        "tutorial.Person.PhoneNumber",
      ))
    }
    it("should fail to construct a parser for an ambiguous type, listing all candidates for ambiguity") {
      val error = the[AmbiguousMessageType] thrownBy {
        parserFor(warcraftSchemaFile, "Zone")
      }
      error.possibleMatches should contain theSameElementsAs (Seq(
        "com.thatdot.test.azeroth.Zone",
        "com.thatdot.test.azeroth.expansions.crusade.Zone",
        "com.thatdot.test.azeroth.expansions.cataclysm.Zone",
      ))
    }
    val barrensZoneAsMap = Expr.Map(
      "name" -> Expr.Str("Barrens"),
      "owner" -> Expr.Str("HORDE"),
      "continent" -> Expr.Str("KALIMDOR"),
    )
    it(
      "should parse a protobuf value with an ambiguous type name, provided the parser was initialized unambiguously",
    ) {
      val parser = parserFor(warcraftSchemaFile, "com.thatdot.test.azeroth.Zone")
      val result = parser.parseBytes(bytesFromURL(testAzerothZone))
      result shouldBe barrensZoneAsMap
    }
    it(
      "should parse a protobuf value with an ambiguous type name that references a different user of that name",
    ) {
      val parser = parserFor(warcraftSchemaFile, "com.thatdot.test.azeroth.expansions.cataclysm.Zone")
      val result = parser.parseBytes(bytesFromURL(testCataclysmZone1))
      result shouldBe Expr.Map(
        "name" -> Expr.Str("Northern Barrens"),
        "owner" -> Expr.Str("HORDE"),
        "region" -> Expr.Str("KALIMDOR"),
        "changelog" -> Expr.Str("Split from some of the Barrens, now a separate zone"),
        "original_zone" -> Expr.Map(
          "azeroth_zone" -> barrensZoneAsMap,
        ),
      )
    }
    it("should parse a value that is oneof ambiguously-named types") {
      val parser = parserFor(warcraftSchemaFile, "com.thatdot.test.azeroth.expansions.cataclysm.AnyZone")
      val result = parser.parseBytes(bytesFromURL(testAnyZone))
      result shouldBe testAnyZoneCypher
    }

    it("should map protobuf bytes to Cypher using an unambiguous short type name") {
      val addressBookPersonParser: ProtobufParser = parserFor(addressBookSchemaFile, "Person")
      val result = addressBookPersonParser.parseBytes(bytesFromURL(testPersonFile))

      testReadablePerson.foreach { case (k, v) =>
        result shouldBe a[Expr.Map]
        result.asInstanceOf[Expr.Map].map.get(k) shouldBe Some(Expr.fromQuineValue(v))
      }
    }

  }

  describe("QuineValueToProtobuf") {
    it("should map QuineValue to a Protobuf DynamicMessage") {

      val desc = testSchemaCache.getMessageDescriptor(addressBookSchemaFile, "Person", flushOnFail = true).futureValue
      val protobufSerializer = new QuineValueToProtobuf(desc)

      val message = protobufSerializer.toProtobuf(testWritablePerson).value

      def extractList(
        xs: List[Descriptors.FieldDescriptor],
      ): (Descriptors.FieldDescriptor, Descriptors.FieldDescriptor, Descriptors.FieldDescriptor) =
        xs match {
          case List(name, id, email) => (name, id, email)
          case _ => sys.error("This shouldn't happen.")
        }

      val (name, id, email) = extractList(List("name", "id", "email").map(message.getDescriptorForType.findFieldByName))

      message.getField(name) shouldBe "Bob"
      message.getField(id) shouldBe 10L
      message.getField(email) shouldBe "bob@example.com"

    }
  }

  describe("Dynamic Message Folding") {
    it("folding via dynamicMessageFoldable should generate the same values as the protobufParser") {
      val desc: Descriptors.Descriptor =
        testSchemaCache.getMessageDescriptor(addressBookSchemaFile, "Person", flushOnFail = true).futureValue
      val protobufSerializer = new QuineValueToProtobuf(desc)
      val message = protobufSerializer.toProtobuf(testWritablePerson).value
      val foldableFrom = DataFoldableFrom.protobufDataFoldable
      val asCypherValue: Value = foldableFrom.fold(message, DataFolderTo.cypherValueFolder)

      testPerson.foreach { case (k, v) =>
        val folded: Value = asCypherValue.getField("")(k).getOrThrow
        toQuineValue(folded) shouldBe v.asRight[CypherException]
      }
    }
  }

}

object ProtobufTest {
  def bytesFromURL(url: URL): Array[Byte] = Using.resource(url.openStream)(ByteStreams.toByteArray)

  // NB anything using this must be a `def` -- otherwise the classloader will not find the resource
  private def getClasspathResource(path: String): URL =
    Option(getClass.getResource("/" + path))
      .orElse {
        // fallback for intelliJ (assuming the test is run from the root of the project)
        val pathCandidates = Seq(s"public/quine/src/test/resources/$path", s"quine/src/test/resources/$path")
        Try(pathCandidates.view.map(new File(_)).collectFirst {
          case f if f.exists() => f.toURI.toURL
        }).toOption.flatten
      }
      .getOrElse {
        throw new RuntimeException(s"Could not find test resource at ${path} -- is your test classpath broken?")
      }

  def addressBookSchemaFile: URL = getClasspathResource("addressbook.desc")

  /** Contains the equivalent of [[testReadablePerson]], serialized according to addressbook.desc, plus
    * an extra (dynamic) field "mapField" not present in the addressbook schema
    *
    * The protobuf_test.binpb file was made by using scalapb to serialize a case class.
    * import com.google.protobuf.ByteString
    * import tutorial.addressbook.Person
    * import tutorial.addressbook.Person.PhoneType
    * import tutorial.addressbook.Person.TestOneof.NumPets
    * val record = Person("Bob", 10, Some("bob@example.com"), Seq(PhoneNumber("503-555-1234", PhoneType.MOBILE), PhoneNumber("360-555-1234", PhoneType.HOME)), Some(ByteString.copyFrom("foo".getBytes)), NumPets(8), Map("ANumber" -> 1.5.toFloat)
    * val outstream = new BufferedOutputStream(new FileOutputStream("/tmp/protobuf_test.data"))
    * record.writeTo(outstream)
    * outstream.close()
    */
  def testPersonFile: URL = getClasspathResource("protobuf_test.binpb")
  private val testPerson: Map[String, QuineValue] = Map(
    "name" -> QuineValue.Str("Bob"),
    "id" -> QuineValue.Integer(10L),
    "email" -> QuineValue.Str("bob@example.com"),
    "phones" -> QuineValue.List(
      Vector(
        QuineValue.Map(Map("number" -> QuineValue.Str("503-555-1234"), "type" -> QuineValue.Str("MOBILE"))),
        QuineValue.Map(Map("number" -> QuineValue.Str("360-555-1234"), "type" -> QuineValue.Str("HOME"))),
      ),
    ),
    "blob" -> QuineValue.Bytes("foo".getBytes(UTF_8)),
    "numPets" -> QuineValue.Integer(8L),
  )
  private val readableButNotWritable = Map(
    "mapField" -> QuineValue.Map(
      Map(
        "ANumber" -> QuineValue.Floating(1.5),
      ),
    ),
  )
  private val writableButNotReadable = Map(
    "garbage" -> QuineValue.Null,
  )
  val testReadablePerson: Map[String, QuineValue] = testPerson ++ readableButNotWritable
  val testWritablePerson: Map[String, QuineValue] = testPerson ++ writableButNotReadable

  def warcraftSchemaFile: URL = getClasspathResource("multi_file_proto_test/schema/warcraft.desc")
  // test files for ambiguous schemas -- these types are all named "Zone".
  // 0, 1, and 3 all have nearly the same schema by-shape, but use different types all named "Zone"
  // 2 is also a type named "Zone", but has a different structure, including one field that itself has a "Zone"
  // type.
  // See multi_file_proto_test/README.md for more details.
  def testAzerothZone: URL = getClasspathResource("multi_file_proto_test/data/example_zone_0.binpb")
  def testCrusadeZone: URL = getClasspathResource("multi_file_proto_test/data/example_zone_1.binpb")
  def testCataclysmZone1: URL = getClasspathResource("multi_file_proto_test/data/example_zone_2.binpb")
  def testCataclysmZone2: URL = getClasspathResource("multi_file_proto_test/data/example_zone_3.binpb")
  // Finally, anyzone has a message whose only member is a oneof between the 3 "Zone"-named types.
  def testAnyZone: URL = getClasspathResource("multi_file_proto_test/data/example_anyzone.binpb")
  val testAnyZoneCypher: Expr.Map = Expr.Map(
    "cataclysm_zone" -> Expr.Map(
      "owner" -> Expr.Str("ALLIANCE"),
      "region" -> Expr.Str("EASTERN_KINGDOMS"),
      "changelog" -> Expr.Str("Added as the worgen starting zone"),
      "name" -> Expr.Str("Gilneas"),
    ),
  )

  val testSchemaCache: ProtobufSchemaCache.Blocking.type = ProtobufSchemaCache.Blocking: @nowarn
}

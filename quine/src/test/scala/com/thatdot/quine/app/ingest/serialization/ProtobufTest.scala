package com.thatdot.quine.app.ingest.serialization

import java.net.URL
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.Using

import com.google.common.io.ByteStreams
import com.google.protobuf.Descriptors
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest.serialization.ProtobufTest.{
  bytesFromURL,
  testParserCache,
  testReadablePerson,
  testSchemaFile,
  testWritablePerson
}
import com.thatdot.quine.app.serialization.QuineValueToProtobuf
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.model.QuineValue

// See also [[CypherParseProtobufTest]] for the UDP interface to this functionality
class ProtobufTest extends AnyFlatSpec with Matchers with EitherValues {
  val protobufParser: ProtobufParser = testParserCache.get(testSchemaFile, "Person")

  // NB this test will not work in intelliJ without some configuration tweaking as the classloader will not find the
  // resource. You can patch it by replacing the schema file URLs with fully-qualified file:/// URLs corresponding
  // to the descriptors' locations on your filesystem.
  if (testSchemaFile == null) {
    throw new RuntimeException("Could not find test schema file -- is your test classpath broken?")
  }

  /** The protobuf_test.dat file was made by using scalapb to serialize a case class.
    * import com.google.protobuf.ByteString
    * import tutorial.addressbook.Person
    * import tutorial.addressbook.Person.PhoneType
    * import tutorial.addressbook.Person.TestOneof.NumPets
    * val record = Person("Bob", 10, Some("bob@example.com"), Seq(PhoneNumber("503-555-1234", PhoneType.MOBILE), PhoneNumber("360-555-1234", PhoneType.HOME)), Some(ByteString.copyFrom("foo".getBytes)), NumPets(8), Map("ANumber" -> 1.5.toFloat)
    * val outstream = new BufferedOutputStream(new FileOutputStream("/tmp/protobuf_test.data"))
    * record.writeTo(outstream)
    * outstream.close()
    */
  "The protobuf deserializer" should "map protobuf bytes to Cypher" in {
    val result = protobufParser.parseBytes(bytesFromURL(getClass.getResource("/protobuf_test.dat")))

    testReadablePerson.foreach { case (k, v) =>
      result.getField("")(k) shouldBe Expr.fromQuineValue(v)
    }
  }

  "The protobuf serializer" should "map QuineValue to a Protobuf DynamicMessage" in {

    val protobufSerializer = new QuineValueToProtobuf(testSchemaFile, "Person")

    val message = protobufSerializer.toProtobuf(testWritablePerson).value

    def extractList(
      xs: List[Descriptors.FieldDescriptor]
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

object ProtobufTest {
  def bytesFromURL(url: URL): Array[Byte] = Using.resource(url.openStream)(ByteStreams.toByteArray)

  // NB anything using this must be a `def` -- otherwise the classloader will not find the resource
  private def getClasspathResource(path: String): URL =
    getClass.getResource("/" + path)

  def testSchemaFile: URL = getClasspathResource("addressbook.desc")

  // contains the equivalent of [[testReadablePerson]], serialized according to addressbook.desc, plus
  // an extra (dynamic) field "mapField" not present in the addressbook schema
  def testPersonFile: URL = getClasspathResource("protobuf_test.dat")

  private val testPerson: Map[String, QuineValue] = Map(
    "name" -> QuineValue.Str("Bob"),
    "id" -> QuineValue.Integer(10L),
    "email" -> QuineValue.Str("bob@example.com"),
    "phones" -> QuineValue.List(
      Vector(
        QuineValue.Map(Map("number" -> QuineValue.Str("503-555-1234"), "type" -> QuineValue.Str("MOBILE"))),
        QuineValue.Map(Map("number" -> QuineValue.Str("360-555-1234"), "type" -> QuineValue.Str("HOME")))
      )
    ),
    "blob" -> QuineValue.Bytes("foo".getBytes(UTF_8)),
    "numPets" -> QuineValue.Integer(8L)
  )
  private val readableButNotWritable = Map(
    "mapField" -> QuineValue.Map(
      Map(
        "ANumber" -> QuineValue.Floating(1.5)
      )
    )
  )
  private val writableButNotReadable = Map(
    "garbage" -> QuineValue.Null
  )
  val testReadablePerson: Map[String, QuineValue] = testPerson ++ readableButNotWritable
  val testWritablePerson: Map[String, QuineValue] = testPerson ++ writableButNotReadable

  val testParserCache: ProtobufParser.BlockingWithoutCaching.type = ProtobufParser.BlockingWithoutCaching
}

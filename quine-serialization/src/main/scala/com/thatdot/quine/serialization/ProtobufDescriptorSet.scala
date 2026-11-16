package com.thatdot.quine.serialization

import java.io.InputStream

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}

/** An immutable index of the protobuf message types defined in a compiled descriptor set (the binary file usually
  * ending in `.desc` written by `protoc --descriptor_set_out`). Nested message types are included.
  *
  * Each message type has two names:
  *  - its fully-qualified name, such as `tutorial.Person.PhoneNumber`
  *  - its package-relative name, such as `Person.PhoneNumber`
  *
  * A package-relative name shared by message types in different packages is ambiguous, so it is not indexed.
  *
  * @param byFullName message types keyed by fully-qualified name
  * @param byRelativeName unambiguous message types keyed by package-relative name
  */
final class ProtobufDescriptorSet private (
  byFullName: Map[String, Descriptor],
  byRelativeName: Map[String, Descriptor],
) {

  /** Returns the message type with the given package-relative name, or else the one with the given fully-qualified
    * name.
    */
  def messageDescriptor(name: String): Option[Descriptor] =
    byRelativeName.get(name).orElse(byFullName.get(name))

  /** Returns the fully-qualified names of all message types. */
  def messageTypes: Set[String] = byFullName.keySet
}

object ProtobufDescriptorSet {

  /** Thrown when a protobuf descriptor set was successfully parsed as a protobuf message, and each `.proto` file
    * contained therein is valid on its own, but the files are not valid ''together'' (e.g., a cyclic import exists).
    */
  final class InvalidDescriptorSet(message: String) extends Exception(message)

  /** Given a stream of the bytes of a compiled descriptor set, reads the stream to its end (without closing it) and
    * returns the index of the described message types.
    *
    * @throws com.google.protobuf.InvalidProtocolBufferException if the bytes are not a descriptor set
    * @throws com.google.protobuf.Descriptors.DescriptorValidationException if a described file is invalid
    * @throws InvalidDescriptorSet if files are duplicated, imports are missing or cyclic, or message names collide
    */
  def parseFrom(in: InputStream): ProtobufDescriptorSet = {
    val fileProtos = FileDescriptorSet.parseFrom(in).getFileList.asScala.toList
    val fileProtosByName: Map[String, FileDescriptorProto] = fileProtos.map(p => p.getName -> p).toMap
    fileProtos.groupBy(_.getName).collectFirst {
      case (name, ps) if ps.size > 1 =>
        throw new InvalidDescriptorSet(s"Duplicate file: $name")
    }

    // Build each file after the files it imports. `importers` is the chain of files that led to this import.
    val builtFiles = mutable.Map.empty[String, FileDescriptor]
    def buildFile(fileName: String, importers: List[String]): FileDescriptor =
      builtFiles.getOrElse(
        fileName, {
          if (importers.contains(fileName))
            throw new InvalidDescriptorSet(s"Cyclic import: ${(fileName :: importers).reverse.mkString(" -> ")}")
          val proto = fileProtosByName.getOrElse(
            fileName,
            throw new InvalidDescriptorSet(s"Cannot resolve import $fileName in ${importers.head}"),
          )
          val dependencies = proto.getDependencyList.asScala.map(buildFile(_, fileName :: importers)).toArray
          val file = FileDescriptor.buildFrom(proto, dependencies)
          builtFiles(fileName) = file
          file
        },
      )
    val files = fileProtos.map(p => buildFile(p.getName, Nil))

    def withNested(message: Descriptor): List[Descriptor] =
      message :: message.getNestedTypes.asScala.toList.flatMap(withNested)
    val messages = files.flatMap(_.getMessageTypes.asScala.toList.flatMap(withNested))

    val byFullName = messages.groupBy(_.getFullName).map {
      case (name, List(message)) => name -> message
      case (name, _) => throw new InvalidDescriptorSet(s"Duplicate message type: $name")
    }
    val byRelativeName = messages
      .groupBy { message =>
        val pkg = message.getFile.getPackage
        if (pkg.isEmpty) message.getFullName else message.getFullName.stripPrefix(pkg + ".")
      }
      .collect { case (name, List(message)) => name -> message }

    new ProtobufDescriptorSet(byFullName, byRelativeName)
  }
}

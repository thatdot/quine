package com.thatdot.quine.app.serialization

import java.net.URL

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}

/** Provides common utilities for its inheritors to parse protobuf descriptors.
  * @see [[com.thatdot.quine.app.ingest.serialization.ProtobufParser]]
  * @see [[QuineValueToProtobuf]]
  * @throws java.io.IOException If opening the schema file fails
  * @throws DescriptorValidationException If the schema file is invalid
  * @throws IllegalArgumentException If the schema file does not contain the specified type
  */
abstract class ProtobufSchema(schemaUrl: URL, typeName: String) {
  // TODO: All of this IO and validating that the type name exists in the file should probably
  // be done in a different mechanism, not blocking the thread at class construction time
  // and throwing exceptions.
  private val files: Seq[FileDescriptorProto] =
    Using.resource(schemaUrl.openStream)(FileDescriptorSet.parseFrom).getFileList.asScala.toVector
  private val fileMap = files.map(f => f.getName -> f).toMap
  private val fileDescriptorDeps: mutable.Map[FileDescriptorProto, FileDescriptor] =
    mutable.Map.empty[FileDescriptorProto, FileDescriptor]
  private def resolveFileDescriptor(file: FileDescriptorProto): FileDescriptor =
    fileDescriptorDeps.getOrElseUpdate(
      file,
      FileDescriptor.buildFrom(file, file.getDependencyList.asScala.map(d => resolveFileDescriptor(fileMap(d))).toArray)
    )

  private def getMessageTypeNames(file: FileDescriptorProto) = file.getMessageTypeList.asScala.map(_.getName)

  final protected val messageType: Descriptor =
    files.filter(file => getMessageTypeNames(file) contains typeName) match {
      // Check that our named type was found in one and only one file:
      case Seq(file) => resolveFileDescriptor(file).findMessageTypeByName(typeName)
      case _ =>
        throw new IllegalArgumentException(
          s"No message of type '$typeName' found among " + files.flatMap(getMessageTypeNames).mkString("[", ", ", "]")
        )
    }
}

package com.thatdot.connect.serialization

import java.net.URL

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}

abstract class ProtobufSchema(schemaUrl: URL, typeName: String) {
  // TODO: All of this IO and validating that the type name exists in the file should probably
  // be done in a different mechanism, not blocking the thread at class construction time
  // and throwing exceptions.
  private val files: Seq[FileDescriptorProto] =
    Using.resource(schemaUrl.openStream)(FileDescriptorSet.parseFrom).getFileList.asScala
  private val fileMap = files.map(f => f.getName -> f).toMap
  private val fileDescriptorDeps: mutable.Map[FileDescriptorProto, FileDescriptor] =
    mutable.Map.empty[FileDescriptorProto, FileDescriptor]
  private def resolveFileDescriptor(file: FileDescriptorProto): FileDescriptor =
    fileDescriptorDeps.getOrElseUpdate(
      file,
      FileDescriptor.buildFrom(file, file.getDependencyList.asScala.map(d => resolveFileDescriptor(fileMap(d))).toArray)
    )

  private def getMessageTypeNames(file: FileDescriptorProto) = file.getMessageTypeList.asScala.map(_.getName)

  protected val messageType: Descriptor = files.filter(file => getMessageTypeNames(file) contains typeName) match {
    // Check that our named type was found in one and only one file:
    case Seq(file) => resolveFileDescriptor(file).findMessageTypeByName(typeName)
    case _ =>
      throw new IllegalArgumentException(
        s"No message of type '$typeName' found among " + files.flatMap(getMessageTypeNames).mkString("[", ", ", "]")
      )
  }
}

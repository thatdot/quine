package com.thatdot.model.v2.outputs.destination

import java.nio.file.{Paths, StandardOpenOption}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.model.v2.outputs.ResultDestination
import com.thatdot.quine.graph.NamespaceId

final case class File(
  path: String,
) extends ResultDestination.Bytes.File {

  override def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Array[Byte], NotUsed] =
    FileIO
      .toPath(
        Paths.get(path),
        Set(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
      )
      .named(sinkName(name))
      .contramap[Array[Byte]](ByteString.fromArray)
      .mapMaterializedValue(_ => NotUsed)

  private def sinkName(name: String): String = s"result-destination--file--$name"
}

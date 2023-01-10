package com.thatdot.quine.app

import java.io.{InputStream, StringWriter}

import org.snakeyaml.engine.v2.api.lowlevel.Compose
import org.snakeyaml.engine.v2.api.{DumpSettings, LoadSettings, StreamDataWriter}
import org.snakeyaml.engine.v2.emitter.Emitter
import org.snakeyaml.engine.v2.nodes.Node
import org.snakeyaml.engine.v2.serializer.Serializer

package object yaml {

  private val loadSettings = LoadSettings.builder.build
  private val dumpSettings = DumpSettings.builder.build
  private val yamlJson = new YamlJson(loadSettings)

  def parse(inStream: InputStream): Node =
    new Compose(loadSettings).composeInputStream(inStream).get

  def renderFromJson(json: ujson.Value): String = {
    val yaml = json.transform(yamlJson)
    val writer = new StringWriter with StreamDataWriter {
      override def flush(): Unit = super.flush() // to fix "conflicting members"
    }
    val serializer = new Serializer(dumpSettings, new Emitter(dumpSettings, writer))
    serializer.emitStreamStart()
    serializer.serializeDocument(yaml)
    serializer.emitStreamEnd()
    writer.toString
  }

  def parseToJson(inStream: InputStream): ujson.Value =
    yamlJson.transform(parse(inStream), ujson.Value)
}

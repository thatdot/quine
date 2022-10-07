package com.thatdot.quine.app

import java.io.InputStream

import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.lowlevel.Compose
import org.snakeyaml.engine.v2.nodes.Node

package object yaml {

  private val loadSettings = LoadSettings.builder.build
  private val yamlJson = new YamlJson(loadSettings)

  def parse(inStream: InputStream): Node =
    new Compose(loadSettings).composeInputStream(inStream).get

  def parseToJson(inStream: InputStream): ujson.Value =
    yamlJson.transform(parse(inStream), ujson.Value)
}

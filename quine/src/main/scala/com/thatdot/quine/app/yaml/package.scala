package com.thatdot.quine.app

import java.io.{BufferedReader, InputStream}

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.reader.UnicodeReader

package object yaml {

  def parse(inStream: InputStream): Node =
    new Yaml().compose(new BufferedReader(new UnicodeReader(inStream)))

  def parseToJson(inStream: InputStream): ujson.Value = {
    val yamlJson = new YamlJson
    yamlJson.transform(parse(inStream), ujson.Value)
  }
}

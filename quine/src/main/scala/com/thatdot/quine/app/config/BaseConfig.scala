package com.thatdot.quine.app.config

import java.nio.file.{Files, Path}

import cats.syntax.either._
import com.typesafe.config.{Config, ConfigRenderOptions}
import io.circe
import io.circe.Json

trait BaseConfig {

  def configVal: Config

  /** @return JSON representation of the current config */
  def loadedConfigJson: Json =
    circe.config.parser.parse(configVal).valueOr(throw _)

  /** @return HOCON representation of the current config */
  def loadedConfigHocon: String = configVal.root render (
    ConfigRenderOptions.defaults.setOriginComments(false).setJson(false),
  )

  /** Write the config out to a file
    *
    * @param path file path at which to write the config file
    */
  def writeConfig(path: String): Unit = {
    Files.writeString(Path.of(path), loadedConfigJson.spaces2)
    ()
  }

}

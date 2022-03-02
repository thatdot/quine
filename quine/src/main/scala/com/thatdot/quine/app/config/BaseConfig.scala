package com.thatdot.quine.app.config

import java.io.PrintWriter

import com.typesafe.config.{ConfigRenderOptions, ConfigValue}

trait BaseConfig {

  def configVal: ConfigValue

  /** @return JSON representation of the current config */
  def loadedConfigJson: ujson.Value = {

    // See Javadocs of `ConfigRenderOptions` about options for producing valid JSON
    val renderOpts = ConfigRenderOptions
      .defaults()
      .setOriginComments(false)
      .setComments(false)
      .setJson(true)

    val configStr = configVal.render(renderOpts)
    ujson.read(configStr)
  }

  /** @return HOCON representation of the current config */
  def loadedConfigHocon: String = configVal.render(
    ConfigRenderOptions.defaults().setOriginComments(false).setJson(false)
  )

  /** Write the config out to a file
    *
    * @param path file path at which to write the config file
    */
  def writeConfig(path: String): Unit = {
    val writer = new PrintWriter(path)
    writer.println(loadedConfigJson.render(indent = 2))
    writer.close()
  }
}

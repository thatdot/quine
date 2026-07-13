package com.thatdot.quine.app.config

import com.typesafe.config.{Config, ConfigRenderOptions}

trait BaseConfig {

  def configVal: Config

  def fileIngest: FileIngestConfig

  def defaultApiVersion: String

  /** Safe, non-secret projection of this config -- the only config data exposed over the API.
    * See [[SystemConfigSummary]].
    */
  def systemConfigSummary: SystemConfigSummary

  /** @return HOCON representation of the current config, including sensitive values.
    * Only ever logged locally (via `dumpConfig`) -- never exposed over the network.
    * See `SystemConfigView`/`configLogic` for the safe, externally-exposed subset.
    */
  def loadedConfigHocon: String = configVal.root render (
    ConfigRenderOptions.defaults.setOriginComments(false).setJson(false),
  )

}

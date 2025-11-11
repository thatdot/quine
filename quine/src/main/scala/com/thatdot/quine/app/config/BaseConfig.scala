package com.thatdot.quine.app.config

import java.nio.file.{Files, Path}

import cats.syntax.either._
import com.typesafe.config.{Config, ConfigRenderOptions}
import io.circe
import io.circe.Json

trait BaseConfig {

  def configVal: Config

  def defaultApiVersion: String

  /** @return JSON representation of the current config with sensitive values masked */
  def loadedConfigJson: Json = {
    val rawJson = circe.config.parser.parse(configVal).valueOr(throw _)
    maskSensitiveFields(rawJson)
  }

  /** Mask sensitive configuration values in JSON
    *
    * @param json The raw configuration JSON
    * @return JSON with sensitive fields masked (e.g., "****-bf9e")
    */
  private def maskSensitiveFields(json: Json): Json = {
    // Paths to mask (works for both Enterprise and Novelty)
    val pathsToMask = List(
      List("quine", "license-key"), // Enterprise: quine.license-key
      List("thatdot", "novelty", "license-key"), // Novelty: thatdot.novelty.license-key
    )

    pathsToMask.foldLeft(json) { (currentJson, path) =>
      maskJsonPath(currentJson, path)
    }
  }

  /** Mask a value at a specific JSON path
    *
    * @param json The JSON to modify
    * @param path Path components (e.g., List("quine", "license-key"))
    * @return Modified JSON with value masked, or original if path doesn't exist
    */
  private def maskJsonPath(json: Json, path: List[String]): Json =
    path match {
      case Nil => json
      case field :: Nil =>
        // Last component - mask the value
        json.mapObject { obj =>
          obj(field) match {
            case Some(valueJson) =>
              valueJson.asString match {
                case Some(str) => obj.add(field, Json.fromString(maskValue(str)))
                case None => obj // Not a string, leave as-is
              }
            case None => obj // Field doesn't exist, no change
          }
        }
      case field :: rest =>
        // Recurse into nested object
        json.mapObject { obj =>
          obj(field) match {
            case Some(nestedJson) =>
              obj.add(field, maskJsonPath(nestedJson, rest))
            case None => obj // Field doesn't exist, no change
          }
        }
    }

  /** Mask a sensitive string value
    *
    * @param value The value to mask (e.g., "e67008aa-c018-440b-8f74-5be9d448bf9e")
    * @return Masked value showing only last 4 characters (e.g., "****-bf9e")
    */
  private def maskValue(value: String): String =
    if (value.length <= 4) {
      "****" // Too short to show partial value
    } else {
      "****" + value.takeRight(4)
    }

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

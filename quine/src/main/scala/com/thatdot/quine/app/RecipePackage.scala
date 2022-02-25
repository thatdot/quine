package com.thatdot.quine.app

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** Container for a Recipe that also includes data not modelled in the Recipe itself
  * (the Recipe source and canonical name).
  *
  * @param name canonical name of the recip
  * @param recipe parsed recipe
  * @param source YAML/JSON source from which the recipe was parsed
  */
final case class RecipePackage(
  name: String,
  recipe: Recipe,
  source: String
)
object RecipePackage {

  /** Parse a recipe package from a recipe file
    *
    * @param file path at which the recipe file is located
    * @return package of all information about the recipe
    */
  def fromFile(file: Path): RecipePackage = {

    // Check that the recipe corresponds to a valid name
    val recipeFileName: String = file.getFileName.toString
    val name: String = if (recipeFileName.endsWith(".yml")) {
      recipeFileName.stripSuffix(".yml")
    } else if (recipeFileName.endsWith(".yaml")) {
      recipeFileName.stripSuffix(".yaml")
    } else if (recipeFileName.endsWith(".json")) {
      recipeFileName.stripSuffix(".json")
    } else {
      throw new IllegalArgumentException(
        s"File $file does not have an accepted recipe extension"
      )
    }

    // Get the recipe contents
    val sourceBytes = Files.readAllBytes(file)
    val source = new String(sourceBytes, StandardCharsets.UTF_8)

    // Parse the recipe
    val recipe = Recipe.fromJson(yaml.parseToJson(new ByteArrayInputStream(sourceBytes))) match {
      case Left(errs) =>
        throw new IllegalArgumentException(s"Malformed recipe: ${errs.mkString("\n", "\n", "")}")
      case Right(parsed) =>
        parsed
    }

    RecipePackage(name, recipe, source)
  }
}

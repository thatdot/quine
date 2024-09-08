package com.thatdot.quine.app

import java.nio.file.{Files, Path}

import io.circe

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
  source: String,
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
    val name = recipeFileName.split('.') match {
      case Array(name, ext) if Seq("yml", "yaml", "json").contains(ext) => name
      case _ =>
        throw new IllegalArgumentException(
          s"File $file does not have an accepted recipe extension",
        )
    }

    // Get the recipe contents
    val source = Files.readString(file)

    // Parse the recipe
    val recipe = circe.yaml.v12.parser.decodeAccumulating[Recipe](source) valueOr { errs =>
      throw new IllegalArgumentException("Malformed recipe: \n" + errs.toList.mkString("\n"))
    }

    RecipePackage(name, recipe, source)
  }
}

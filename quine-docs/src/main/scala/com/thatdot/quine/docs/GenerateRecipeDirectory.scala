package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.immutable
import scala.compat.java8.StreamConverters._

import com.thatdot.quine.app.{Recipe, RecipePackage}

object GenerateRecipeDirectory extends App {

  val (recipeSourceInputDir, recipeMarkdownOutputDir): (Path, Path) = args match {
    case Array(a, b) =>
      (Paths.get(a), Paths.get(b))
    case _ =>
      println(s"GenerateRecipeTable expected 2 command line arguments but got: ${args.mkString(",")}")
      sys.exit(1)
  }

  Files.createDirectories(recipeMarkdownOutputDir)

  val recipes: immutable.Seq[RecipePackage] = Files
    .list(recipeSourceInputDir)
    .filter(x => x.getFileName.toString.endsWith("yaml"))
    .map[RecipePackage](RecipePackage.fromFile)
    .toScala

  println(s"Read ${recipes.length} Recipes from input directory $recipeSourceInputDir")
  println(s"Generating markdown in output directory $recipeMarkdownOutputDir")

  // write recipes/index.md
  // contains a table listing every Recipe
  Files.write(
    Paths.get(recipeMarkdownOutputDir.toAbsolutePath.toString, "index.md"),
    recipeListingMarkdown(recipes).getBytes(StandardCharsets.UTF_8),
    StandardOpenOption.TRUNCATE_EXISTING,
    StandardOpenOption.CREATE
  )

  // write recipes/recipe-canonical-name.md files
  for (recipePackage <- recipes) {
    val filePath = recipeMarkdownOutputDir.resolve(recipePackage.name + ".md")
    val markdown = recipeDetailMarkdown(recipePackage).getBytes(StandardCharsets.UTF_8)
    Files.write(
      filePath,
      markdown,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.CREATE
    )
  }

  /** Render a markdown page that is an index listing of recipes
    *
    * @param recipes recipes to list and link to
    * @return rendered markdown source
    */
  def recipeListingMarkdown(recipes: Iterable[RecipePackage]): String = {
    val builder = new StringBuilder
    builder ++= "# Recipes\n\n"

    builder ++= "@@@index\n"
    for (recipePackage <- recipes) {
      val name = recipePackage.name
      builder ++= s"  * @ref:[$name]($name.md)\n"
    }
    builder ++= "@@@\n"

    builder ++= "@@@ div { .recipe-list }\n"
    for (recipePackage <- recipes) {
      val recipe = recipePackage.recipe
      val title = recipe.title
      val summary = recipe.summary.getOrElse("")
      val contributor = recipe.contributor.fold("")("<small>" + _ + "</small>")

      builder ++= s"""
        |@@@@ div
        |### @ref:[$title](${recipePackage.name}.md)
        |$contributor
        |
        |$summary
        |@@@@
        |""".stripMargin
    }
    builder ++= "@@@\n"

    builder.result
  }

  /** Render a markdown page associated with a recipe
    *
    * @param recipePackage recipe, its source, and its name
    * @return rendered markdown source
    */
  def recipeDetailMarkdown(recipePackage: RecipePackage): String = {
    val recipe = recipePackage.recipe

    val description = recipe.description.filter(_.trim.nonEmpty).fold("") { (desc: String) =>
      s"""
      |@@@@ div
      |$desc
      |@@@@
      |""".stripMargin
    }

    val contributor = recipe.contributor.filter(_.trim.nonEmpty).fold("") { (contributor: String) =>
      s"""
      |@@@@ div
      |<small>Contributed by</small> $contributor
      |@@@@
      |""".stripMargin
    }

    val cliParameters = Recipe
      .applySubstitutions(recipe, Map.empty)
      .fold(_.map(_.name).toList, _ => Nil)
      .distinct
      .zipWithIndex
      .map { case (name, idx) => s" --recipe-value $name=$$PARAM${idx + 1}" }
      .mkString

    s"""
    |# ${recipe.title}
    |
    |@@@ div
    |
    |$contributor
    |$description
    |
    |@@@
    |
    |### Command line invocation
    |
    |@@@vars { start-delimiter="&" stop-delimiter="&" }
    |```bash
    |$$ java -jar &quine.jar& -r ${recipePackage.name}$cliParameters
    |```
    |@@@
    |
    |### Recipe
    |
    |```yaml
    |${recipePackage.source}
    |```
    |
    |""".stripMargin
  }
}

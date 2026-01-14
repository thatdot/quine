// resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
val scalajsBundlerVersion = "0.21.1"
addDependencyTreePlugin
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.20.1")
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % scalajsBundlerVersion)
addSbtPlugin("ch.epfl.scala" % "sbt-web-scalajs-bundler" % scalajsBundlerVersion)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.5")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.6")
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.1.0")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.8")
libraryDependencies += "software.amazon.awssdk" % "ecr" % "2.17.231"
libraryDependencies += "org.eclipse.jgit" % "org.eclipse.jgit" % "7.5.0.202512021534-r"
addSbtPlugin("com.github.sbt" % "sbt-boilerplate" % "0.8.0")
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "5.1.0")
addSbtPlugin("com.github.sbt" %% "sbt-sbom" % "0.5.0")

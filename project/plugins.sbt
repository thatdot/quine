// resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
val scalajsBundlerVersion = "0.21.1"
addDependencyTreePlugin
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.18.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % scalajsBundlerVersion)
addSbtPlugin("ch.epfl.scala" % "sbt-web-scalajs-bundler" % scalajsBundlerVersion)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.2")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.10.7")
addSbtPlugin("com.github.sbt" % "sbt-paradox-material-theme" % "0.7.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.7.0")
addSbtPlugin("com.github.sbt" % "sbt-ghpages" % "0.7.0")
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.1.0")
addSbtPlugin("com.github.sbt" % "sbt-proguard" % "0.6.0")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")
libraryDependencies += "software.amazon.awssdk" % "ecr" % "2.17.231"
libraryDependencies += "org.eclipse.jgit" % "org.eclipse.jgit" % "7.1.0.202411261347-r"
addSbtPlugin("com.github.sbt" % "sbt-boilerplate" % "0.7.0")
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "5.1.0")
addSbtPlugin("com.github.sbt" %% "sbt-sbom" % "0.4.0")

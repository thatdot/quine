// resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
val scalajsBundlerVersion = "0.21.1"
// addDependencyTreePlugin
addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.12.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalajs-bundler" % scalajsBundlerVersion)
addSbtPlugin("ch.epfl.scala" % "sbt-web-scalajs-bundler" % scalajsBundlerVersion)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.2.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.0")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.9.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.10.3")
addSbtPlugin("com.github.jkugiya" % "sbt-paradox-material-theme" % "0.6.0-fork3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")
addSbtPlugin("com.github.sbt" % "sbt-ghpages" % "0.7.0")
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.1")
addSbtPlugin("com.github.sbt" % "sbt-proguard" % "0.5.0")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")
libraryDependencies += "software.amazon.awssdk" % "ecr" % "2.17.231"
libraryDependencies += "org.eclipse.jgit" % "org.eclipse.jgit" % "6.4.0.202211300538-r"

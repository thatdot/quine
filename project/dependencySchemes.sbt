// scala-xml should be compatible across 1.x and 2.x. Dependencies of the meta-build itself require
// conflicting major versions. Tell SBT they are always compatible to prevent it from failing to compile
// (just running "sbt" in this project could fail).
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

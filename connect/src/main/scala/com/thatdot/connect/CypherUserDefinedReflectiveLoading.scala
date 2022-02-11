package com.thatdot.connect

import java.io.File
import java.net.{URL, URLClassLoader}

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import io.github.classgraph._

import com.thatdot.quine.compiler.cypher.{registerUserDefinedFunction, registerUserDefinedProcedure}
import com.thatdot.quine.graph.cypher._

object CypherUserDefinedReflectiveLoading {

  /** Register (or overwrite) all UDFs with the `@CypherUDF` annotation and all
    * UDPs with the `@CypherUDP` annotation found in a JAR
    *
    * @note the UDFs and UDPs must have a constructor that takes no arguments
    * @param jarFile name of the JAR file
    */
  def registerUserDefinedJar(jarFile: File): Unit = {

    val cypherExtsLoader: URLClassLoader = new URLClassLoader(
      Array[URL](jarFile.toURI.toURL),
      this.getClass().getClassLoader()
    )

    val scanResult: ScanResult = new ClassGraph()
      .addClassLoader(cypherExtsLoader)
      .acceptJars(jarFile.getName)
      .enableAnnotationInfo()
      .scan()

    val udfs: Vector[UserDefinedFunction] = scanResult
      .getClassesWithAnnotation(classOf[CypherUDF].getName)
      .asScala
      .view
      .map { (classInfo: ClassInfo) =>
        Try(
          classInfo.loadClass
            .getConstructor()
            .newInstance()
            .asInstanceOf[UserDefinedFunction]
        )
      }
      .collect { case Success(udf) => udf }
      .toVector

    val udps: Vector[UserDefinedProcedure] = scanResult
      .getClassesWithAnnotation(classOf[CypherUDP].getName)
      .asScala
      .view
      .map { (classInfo: ClassInfo) =>
        Try(
          classInfo.loadClass
            .getConstructor()
            .newInstance()
            .asInstanceOf[UserDefinedProcedure]
        )
      }
      .collect { case Success(udp) => udp }
      .toVector

    udfs.foreach(registerUserDefinedFunction)
    udps.foreach(registerUserDefinedProcedure)
    println(s"Registered from $jarFile: ${(udfs.map(_.name) ++ udps.map(_.name)).mkString(", ")}")
  }

}

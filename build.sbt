name := "spark-org-anonymizer"

version := "1.0.0"
scalaVersion := "2.11.12"
organization := "spark.org"

scalacOptions := Seq("-unchecked", "-deprecation", "-Yrangepos", "-Ywarn-unused-import")
Compile / doc / scalacOptions := Seq("-groups", "-implicits", "-diagrams", "-diagrams-debug")
lazy val copyDocAssetsTask = taskKey[Unit]("Copy doc assets")
copyDocAssetsTask := {
  println("Copying doc assets")
  val sourceDir = file("docs/images/")
  val targetDir = file((target in (Compile, doc)).value + "/lib")
  IO.copyDirectory(sourceDir, targetDir)
}

copyDocAssetsTask := (copyDocAssetsTask triggeredBy (doc in Compile)).value
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.1"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.11.550"
libraryDependencies += "com.amazonaws" % "aws-encryption-sdk-java" % "1.3.6"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.550"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.3"
libraryDependencies += "io.delta" %% "delta-core" % "0.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

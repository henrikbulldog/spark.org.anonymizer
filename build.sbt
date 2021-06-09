name := "spark-org-anonymizer"

version := "1.0.0"
scalaVersion := "2.11.12"
organization := "com.laerdal"
organizationName := "Laerdal Copenhagen"

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
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "io.spray" %% "spray-json" % "1.3.6"

homepage := Some(url("https://github.com/henrikbulldog/spark.org.anonymizer"))
licenses := Seq("UNLICENSE" -> url("https://unlicense.org"))
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
publishTo in ThisBuild := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
scmInfo := Some(
  ScmInfo(
    url("https://github.com/henrikbulldog/spark.org.anonymizer"),
    "scm:git:git@github.com:henrikbulldog/spark.org.anonymizer.git"
  )
)
developers := List(
  Developer("henrik", "Henrik Thomsen", "henrik.thomsen.dk@gmail.com", url("https://www.linkedin.com/in/henrikt/"))
)
import sbtassembly.AssemblyPlugin.assemblySettings

name := "spark-org-anonymizer"
organization := "io.github.henrikbulldog"
organizationName := "io.github.henrikbulldog"

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
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

homepage := Some(url("https://github.com/henrikbulldog/spark.org.anonymizer"))
licenses := Seq("UNLICENSE" -> url("https://unlicense.org"))
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
parallelExecution in Test := false

sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
publishTo in ThisBuild := {
  val nexus = "https://s01.oss.sonatype.org/"
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
//Assembly
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
addArtifact(artifact in (Compile, assembly), assembly)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

import ReleaseTransformations._
releaseCrossBuild := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // check that there are no SNAPSHOT dependencies
  inquireVersions, // ask user to enter the current and next version
  runClean, // clean
  runTest, // run tests
  setReleaseVersion, // set release version in version.sbt
  commitReleaseVersion, // commit the release version
  tagRelease, // create git tag
  releaseStepCommandAndRemaining("+publishSigned"), // run +publishSigned command to sonatype stage release
  setNextVersion, // set next version in version.sbt
  commitNextVersion, // commit next version
  releaseStepCommand("sonatypeRelease"), // run sonatypeRelease and publish to maven central
  pushChanges // push changes to git
)





developers := List(
  Developer("henrik", "Henrik Thomsen", "henrik.thomsen.dk@gmail.com", url("https://www.linkedin.com/in/henrikt/")),
  Developer("rajesh", "Rajesh Dash", "rajesh.dash@laerdal.com", url("https://www.rajeshblogs.in/"))
)

name := "director-v2"
organization := "io.github.uptane"
scalaVersion := "2.12.10"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-Ypartial-unification")

resolvers += "Artifactory Realm" at "https://artifactory-horw.int.toradex.com/artifactory/ota-sbt-dev-horw"

libraryDependencies ++= {
  val akkaV = "2.6.5"
  val akkaHttpV = "10.1.12"
  val scalaTestV = "3.0.8"
  val bouncyCastleV = "1.59"
  val tufV = "0.7.3-11-g4e7ccc6"
  val libatsV = "0.4.0-22-g5c9af8f"

  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "org.scalatest"     %% "scalatest" % scalaTestV % Test,
    "org.scalacheck" %% "scalacheck" % "1.13.4" % Test,

    "io.github.uptane" %% "libats" % libatsV,
    "io.github.uptane" %% "libats-messaging" % libatsV,
    "io.github.uptane" %% "libats-messaging-datatype" % libatsV,
    "io.github.uptane" %% "libats-metrics-akka" % libatsV,
    "io.github.uptane" %% "libats-metrics-prometheus" % libatsV,
    "io.github.uptane" %% "libats-http-tracing" % libatsV,
    "io.github.uptane" %% "libats-slick" % libatsV,
    "io.github.uptane" %% "libats-logging" % libatsV,
    "io.github.uptane" %% "libtuf" % tufV,
    "io.github.uptane" %% "libtuf-server" % tufV,

    "org.bouncycastle" % "bcprov-jdk15on" % bouncyCastleV,
    "org.bouncycastle" % "bcpkix-jdk15on" % bouncyCastleV,

    "org.scala-lang.modules" %% "scala-async" % "0.9.6",

    "org.mariadb.jdbc" % "mariadb-java-client" % "2.2.1"
  )
}

scalacOptions in Compile ++= Seq(
  "-deprecation",
    "-feature",
  "-Xlog-reflective-calls",
  "-Yno-adapted-args",
  "-Ypartial-unification"
)

testOptions in Test ++= Seq(
  Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"),
  Tests.Argument(TestFrameworks.ScalaTest, "-oDS")
)

enablePlugins(BuildInfoPlugin, GitVersioning, JavaAppPackaging)

buildInfoOptions += BuildInfoOption.ToMap

buildInfoOptions += BuildInfoOption.BuildTime

mainClass in Compile := Some("com.advancedtelematic.director.Boot")

import com.typesafe.sbt.packager.docker._
import sbt.Keys._
import com.typesafe.sbt.SbtNativePackager.Docker
import DockerPlugin.autoImport._
import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._

dockerRepository in Docker := Some("advancedtelematic")

packageName in Docker := packageName.value

dockerUpdateLatest := true

dockerAliases ++= Seq(dockerAlias.value.withTag(git.gitHeadCommit.value))

defaultLinuxInstallLocation in Docker := s"/opt/${moduleName.value}"

dockerCommands := Seq(
  Cmd("FROM", "advancedtelematic/alpine-jre:adoptopenjdk-jre8u262-b10"),
  ExecCmd("RUN", "mkdir", "-p", s"/var/log/${moduleName.value}"),
  Cmd("ADD", "opt /opt"),
  Cmd("WORKDIR", s"/opt/${moduleName.value}"),
  ExecCmd("ENTRYPOINT", s"/opt/${moduleName.value}/bin/${moduleName.value}"),
  Cmd("RUN", s"chown -R daemon:daemon /opt/${moduleName.value}"),
  Cmd("RUN", s"chown -R daemon:daemon /var/log/${moduleName.value}"),
  Cmd("USER", "daemon")
)

fork := true

sonarProperties ++= Map(
  "sonar.projectName" -> "OTA Connect Director",
  "sonar.projectKey" -> "ota-connect-director",
  "sonar.host.url" -> "http://sonar.in.here.com",
  "sonar.links.issue" -> "https://saeljira.it.here.com/projects/OTA/issues",
  "sonar.links.scm" -> "https://main.gitlab.in.here.com/olp/edge/ota/connect/back-end/director",
  "sonar.links.ci" -> "https://main.gitlab.in.here.com/olp/edge/ota/connect/back-end/director/pipelines",
  "sonar.projectVersion" -> version.value,
  "sonar.language" -> "scala")

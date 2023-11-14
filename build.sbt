name := "director-v2"
organization := "io.github.uptane"
scalaVersion := "2.13.10"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature", "-Xlog-reflective-calls", "-Xasync", "-Xsource:3", "-Ywarn-unused")

resolvers += "sonatype-snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
resolvers += "sonatype-releases" at "https://s01.oss.sonatype.org/content/repositories/releases"

Global / bloopAggregateSourceDependencies := true

Compile / unmanagedSourceDirectories += baseDirectory.value / "device-registry" / "src" / "main" / "scala"

Test / unmanagedSourceDirectories += baseDirectory.value / "device-registry" / "src" / "test" / "scala"

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test"

lazy val library =
  new {
    object Version {
      val attoCore = "0.9.5"
      val scalaTest  = "3.2.12"
      val libAts     = "2.5.0"
      val libTuf = "3.1.3"
      val akka = "2.8.5"
      val akkaHttp = "10.5.2"
      val alpakkaCsv = "2.0.0"
      val mariaDb = "2.7.3"
      val circe = "0.14.1"
      val toml = "0.2.2"
    }

    val scalaTest  = "org.scalatest"  %% "scalatest"  % Version.scalaTest
    val libAts = Seq(
      "libats-messaging",
      "libats-messaging-datatype",
      "libats-slick",
      "libats-http",
      "libats-metrics",
      "libats-metrics-akka",
      "libats-metrics-prometheus",
      "libats-http-tracing",
      "libats-logging"
    ).map("io.github.uptane" %% _ % Version.libAts)
    val libTuf = "io.github.uptane" %% "libtuf" % Version.libTuf
    val akkaHttpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % Version.akkaHttp
    val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % Version.akka
    val akkaAlpakkaCsv = "com.lightbend.akka" %% "akka-stream-alpakka-csv" % Version.alpakkaCsv
    val mariaDb = "org.mariadb.jdbc" % "mariadb-java-client" % Version.mariaDb
    val circeTesting = "io.circe" %% "circe-testing" % Version.circe
    val akkaSlf4J = "com.typesafe.akka" %% "akka-slf4j" % Version.akka
    val toml = "tech.sparse" %% "toml-scala" % Version.toml
    val attoCore = "org.tpolecat" %% "atto-core" % Version.attoCore
  }


libraryDependencies ++= Seq(
  library.akkaAlpakkaCsv,
  library.akkaHttpTestKit % Test,
  library.akkaSlf4J,
  library.akkaStreamTestKit % Test,
  library.attoCore,
  library.circeTesting % Test,
  library.libTuf,
  library.mariaDb,
  library.scalaTest % Test,
  library.toml,
)

libraryDependencies ++= {
  val akkaV = "2.8.5"
  val akkaHttpV = "10.5.2"
  val tufV = "3.1.3"
  val scalaTestV = "3.2.17"
  val bouncyCastleV = "1.76"
  val libatsV = "2.5.1-SNAPSHOT"

  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "org.scalatest"     %% "scalatest" % scalaTestV % Test,
    "org.scalacheck" %% "scalacheck" % "1.17.0" % Test,

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

    "org.bouncycastle" % "bcprov-jdk18on" % bouncyCastleV,
    "org.bouncycastle" % "bcpkix-jdk18on" % bouncyCastleV,

    "org.scala-lang.modules" %% "scala-async" % "1.0.1",
    "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided,

    "org.mariadb.jdbc" % "mariadb-java-client" % "3.2.0"
  )
}

Test / testOptions ++= Seq(
  Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"),
  Tests.Argument(TestFrameworks.ScalaTest, "-oDS")
)

buildInfoObject := "AppBuildInfo"
buildInfoPackage := "com.advancedtelematic.director"
buildInfoUsePackageAsPath := true
buildInfoOptions += BuildInfoOption.Traits("com.advancedtelematic.libats.boot.VersionInfoProvider")
buildInfoOptions += BuildInfoOption.ToMap
buildInfoOptions += BuildInfoOption.BuildTime

enablePlugins(BuildInfoPlugin, GitVersioning, JavaAppPackaging)

Compile / mainClass := Some("com.advancedtelematic.director.Boot")

dockerRepository := Some("advancedtelematic")

Docker / packageName := packageName.value

dockerUpdateLatest := true

dockerAliases ++= Seq(dockerAlias.value.withTag(git.gitHeadCommit.value))

Docker / defaultLinuxInstallLocation := s"/opt/${moduleName.value}"

dockerBaseImage := "eclipse-temurin:17.0.3_7-jre-jammy"

Docker / daemonUser := "daemon"

fork := true

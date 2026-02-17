ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18" // todo: use 2.12.11

lazy val root = (project in file("."))
  .settings(
    name := "Earthquakes",
    idePackagePrefix := Some("it.matteogaliazzo.spark")
  )

val sparkVersion = "4.1.1" // todo: use 3.5.3

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
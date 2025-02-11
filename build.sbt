ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark_handson",
    idePackagePrefix := Some("de.spark_handson")
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.xerial" % "sqlite-jdbc" % "3.45.1.0",
  "org.scala-lang" % "scala-reflect" % "2.13.12"
)


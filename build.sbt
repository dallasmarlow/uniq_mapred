import AssemblyKeys._

assemblySettings

name := "uniq_mapred"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1" % "provided",
  "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.0.1" % "provided"
)

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases"
)



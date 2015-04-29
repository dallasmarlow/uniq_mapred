import AssemblyKeys._

assemblySettings

jarName in assembly := "uniq_mapred.jar"

name := "distinct_line"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1" % "provided",
  "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.0.1" % "provided"
)

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases"
)



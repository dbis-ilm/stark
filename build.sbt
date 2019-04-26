name := "stark"

scalaVersion := "2.11.12"

lazy val stark = (project in file("."))

libraryDependencies ++= Seq(
   //"com.vividsolutions" % "jts" % "1.13" withSources() withJavadoc(),
   "org.locationtech.jts" % "jts-core" % "1.16.0",
   "org.apache.spark" %% "spark-core" % "2.4.0" % "provided" withSources() withJavadoc(),
   "org.apache.spark"  %% "spark-mllib" % "2.4.0" % "provided",
   "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
   //"fm.void.jetm" % "jetm" % "1.2.3",
   "org.scalatest" %% "scalatest" % "3.0.5" % "test" withSources(),
   "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
   //"com.assembla.scala-incubator" %% "graph-core" % "1.11.0",
   "org.scala-graph" %% "graph-core" % "1.12.5",
   "com.github.scopt" %% "scopt" % "3.7.1"
)

test in assembly := {}

logBuffered in Test := false

parallelExecution in Test := false

assemblyJarName in assembly := "stark.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

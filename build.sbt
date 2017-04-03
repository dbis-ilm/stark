name := "stark"

scalaVersion := "2.12.1"

lazy val stark = (project in file("."))

libraryDependencies ++= Seq(
   "com.vividsolutions" % "jts" % "1.13" withSources() withJavadoc(),
   "org.apache.spark" %% "spark-core" % "2.1.0" % "provided" withSources() withJavadoc(),
   "org.apache.spark"  %% "spark-mllib" % "2.1.0" % "provided",
   "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
   //"fm.void.jetm" % "jetm" % "1.2.3",
   "org.scalatest" %% "scalatest" % "3.0.1" % "test" withSources(),
   "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
   //"com.assembla.scala-incubator" %% "graph-core" % "1.11.0",
   "org.scala-graph" %% "graph-core" % "1.11.4",
   "com.github.scopt" %% "scopt" % "3.5.0"
)

test in assembly := {}

logBuffered in Test := false

parallelExecution in Test := false

assemblyJarName in assembly := "stark.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

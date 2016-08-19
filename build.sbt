name := "spatialspark"

scalaVersion := "2.11.8"

lazy val root = (project in file(".")) 

libraryDependencies ++= Seq(
   "com.vividsolutions" % "jts" % "1.13" withSources() withJavadoc(),
   "org.apache.spark" %% "spark-core" % "2.0.0" % "provided" withSources() withJavadoc(),
   "org.apache.spark"  %% "spark-mllib" % "2.0.0" % "provided",
   "fm.void.jetm" % "jetm" % "1.2.3",
   "org.scalatest" %% "scalatest" % "3.0.0" % "test" withSources(),
   "com.assembla.scala-incubator" %% "graph-core" % "1.11.0"
)

parallelExecution in Test := false

assemblyJarName in assembly := "spatialspark.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

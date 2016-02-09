name := "spatialspark"

scalaVersion := "2.11.7"

lazy val root = (project in file(".")).dependsOn(parti)

lazy val parti = project.in(file("spatialpartitioner"))

libraryDependencies ++= Seq(
   "com.vividsolutions" % "jts" % "1.13" withSources() withJavadoc(),
   "org.apache.spark" % "spark-core_2.11" % "1.5.2" % "provided" withSources() withJavadoc(),
   "fm.void.jetm" % "jetm" % "1.2.3",
   "org.scalatest" %% "scalatest" % "3.0.0-M15" % "test" withSources()
)

parallelExecution in Test := false

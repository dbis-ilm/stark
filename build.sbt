name := "SpatialSpark"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
   "com.vividsolutions" % "jts" % "1.13" withSources() withJavadoc(),
   "org.apache.spark" % "spark-core_2.11" % "1.5.2" % "provided" withSources() withJavadoc()
)

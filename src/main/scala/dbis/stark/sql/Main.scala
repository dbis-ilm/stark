package dbis.stark.sql

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import dbis.stark._
import dbis.stark.spatial.SpatialRDD._
import org.apache.hadoop.yarn.util.RackResolver

object Main {
  def main(args: Array[String]) {

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val spark = SparkSession
      .builder()
      .appName("Spark SQL for Stark")
      // .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    org.apache.spark.sql.raster.registerUDTs()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.json("file:///Users/kai/Projects/stark/data.json")
    df.printSchema()

    val containedBy = (self: STObject, other: STObject) => self.containedBySpatial(other) : Boolean
    val toSTObject: String => STObject = STObject.apply(_)

    import org.apache.spark.sql.functions.udf

    val toSTObjectUDF = udf(toSTObject)
    val containedByUDF = udf(containedBy)

    spark.udf.register("containedBy", containedBy)
    spark.udf.register("toSTObject", toSTObject)

    val df2 = df
      .withColumn("location", toSTObjectUDF(df("column1")))
    df2.printSchema()

    // Register the DataFrame as a SQL temporary view
    df2.createOrReplaceTempView("myData")

    val sqlDF = spark.sql("SELECT location FROM myData WHERE containedBy(location, toSTObject('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'))")

    sqlDF.show()
  }
}

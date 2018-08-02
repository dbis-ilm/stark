package dbis.stark.sql

import dbis.stark.sql.Functions._
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * A sample program that shows how to use SparkSQL with STARK functions
  *
  * When stark.jar is available, start this example program with:
  *
  * spark-submit --master local --class dbis.stark.sql.SQLExample stark.jar
  */
object SQLExample {
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

    // IMPORTANT: initializes the SQL types and functions!
    dbis.stark.sql.Functions.register(spark)

    // read JSON file
    val df = spark.read.json("file:///tmp/data.json")
    df.printSchema()

    // add column with STObject type for spatio-temporal calculations
    val df2 = df
      .withColumn("location", fromWKT(df("column1")))

    df2.printSchema()

    // Register the DataFrame as a SQL temporary view
    df2.createOrReplaceTempView("myData")

    // run query
    val sqlDF = spark.sql("SELECT location, column2 FROM myData WHERE containedBy(location, fromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'))")

    // show result
    sqlDF.show(truncate = false)
  }
}

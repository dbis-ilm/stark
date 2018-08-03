package dbis.stark.sql


import dbis.stark.sql.Functions._
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}


/**
  * A sample program that shows how to use SparkSQL with STARK functions
  *
  * Assuming we are in the STARK project diretory, e.g. /home/hage/stark then start this example program with:
  *
  * spark-submit --master local --class dbis.stark.sql.SQLExample target/scala-2.11/stark.jar  file:///home/hage/stark/src/test/resources/spatialdata.json
  */
object SQLExample {
  def main(args: Array[String]) {

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val spark = dbis.stark.sql.STARKSession
      .builder()
      .appName("Spark SQL for Stark")
      .getOrCreate()

    // Alternative 1: SparkSession with implicits
    /*
    import org.apache.spark.sql.SparkSession
    import dbis.stark.sql.STARKSession._
    val spark = SparkSession
        .builder()
        .appName("Sark SQL for STARK")
        .enableSTARKSupport()
        .getOrCreate()
    */




    // Alternative 2: SparkSession with explicit Function registering
    /*
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
        .builder()
        .appName("Spark SQL for STARK")
        .getOrCreate()
    dbis.stark.sql.Functions.register(spark)
     */

    spark.sparkContext.setLogLevel("ERROR")

    // read JSON file
    if(args.length != 1) {
      sys.error("please provide path to a JSON file to process")
    }
    val theFile = args(0)
    val df = spark.read.json(theFile)
    df.printSchema()

    // add column with STObject type for spatio-temporal calculations
    val df2 = df
      .withColumn("location", fromWKT(df("column1")))

    df2.printSchema()

    // Register the DataFrame as a SQL temporary view
    df2.createOrReplaceTempView("myData")

    // run query
    val sqlDF = spark.sql("SELECT asString(location), column2 FROM myData WHERE containedBy(location, fromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'))")

    // show result
    sqlDF.show(truncate = false)
  }
}

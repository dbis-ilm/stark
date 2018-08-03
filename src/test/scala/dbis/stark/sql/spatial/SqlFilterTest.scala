package dbis.stark.sql.spatial

import dbis.stark.STObject
import dbis.stark.sql.Functions.fromWKT
import dbis.stark.sql.STARKSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SqlFilterTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = STARKSession.builder().master("local").appName("sqltest filter").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    if(spark != null)
      spark.close()
  }

  "A SQL spatial filter" should "be correct for contains" in {

    val sparkSession: SparkSession = spark
    import sparkSession.implicits._

    val s = Seq("""{ "column1": "POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))", "column2": 42 }""",
      """{ "column1": "POINT (25 20)", "column2": 69 }""")

    val df = spark.read.json(s.toDS())

    val df2 = df.withColumn("location", fromWKT(df("column1")))

    // Register the DataFrame as a SQL temporary view
    df2.createOrReplaceTempView("myData")

    // run query
    val sqlDF = spark.sql("SELECT location, column2 FROM myData WHERE containedBy(location, fromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'))")


    val result = sqlDF.collect()
    result.length shouldBe 1

    val row = result(0)
    row.getAs[STObject](0) shouldBe STObject("POINT (25 20)")
    row.getLong(1) shouldBe 69
  }



}

package dbis.stark.sql.spatial

import dbis.stark.sql.Functions.fromWKT
import dbis.stark.sql.STARKSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SqlJoinTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = STARKSession.builder().master("local").appName("sqltest filter").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    if(spark != null)
      spark.close()
  }

  "A SQL spatial join" should "be correct for intersects self join" in {

    val s = Seq("""{ "column1": "POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))", "column2": 42 }""",
      """{ "column1": "POINT (25 20)", "column2": 69 }""")

    val l = spark.read.json(spark.sparkContext.parallelize(s))
    val r = spark.read.json(spark.sparkContext.parallelize(s))

    val left =  l.withColumn("location", fromWKT(l("column1")))
    val right = r.withColumn("location", fromWKT(r("column1")))


    // Register the DataFrame as a SQL temporary view
    left.createOrReplaceTempView("left")
    right.createOrReplaceTempView("right")

    // run query
    val sqlDF = spark.sql(
      """SELECT asString(left.location) as left_loc, left.column2, asString(right.location) as right_loc, right.column2
        | FROM left l, right r
        | WHERE intersects(l.location, r.location)""".stripMargin)

    val result = sqlDF.collect()
    result.length shouldBe 2


    result(0).getString(0) shouldBe "STObject(POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5)),None)"
    result(0).getLong(1) shouldBe 42
    result(0).getString(2) shouldBe "STObject(POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5)),None)"
    result(0).getLong(3) shouldBe 42

    result(1).getString(0) shouldBe "STObject(POINT (25 20),None)"
    result(1).getLong(1) shouldBe 69
    result(1).getString(2) shouldBe "STObject(POINT (25 20),None)"
    result(1).getLong(3) shouldBe 69

  }



}

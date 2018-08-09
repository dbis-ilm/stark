package dbis.stark.sql.spatial

import dbis.stark.STObject
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

  private def prepare(qry: String) = {
    val s = Seq(
      """{ "column1": "POINT (1 1)", "column2": 23}""",
      """{ "column1": "POINT (25 20)", "column2": 69 }""",
      """{ "column1": "POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5))", "column2": 42 }"""
      )
//

    val s2 = Seq(
      """{ "column1": "POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5))", "column2": 42 }""",
      """{ "column1": "POINT (25 20)", "column2": 69 }""",
      """{ "column1": "POINT (1 1)", "column2": 23}"""

    )

    val sparkSession = spark
    import sparkSession.implicits._


    val l = spark.read.json(s.toDS())
    val r = spark.read.json(s2.toDS())

    val left =  l.withColumn("locationL", fromWKT(l("column1")))
    val right = r.withColumn("locationR", fromWKT(r("column1")))


    // Register the DataFrame as a SQL temporary view
    left.createOrReplaceTempView("left")
    right.createOrReplaceTempView("right")

    // run query
    val sqlDF = spark.sql(qry)

    sqlDF.collect()
  }

  "A SQL spatial join" should "be correct for intersects self join" in {

    val qry =
      """SELECT left.locationL as left_loc, left.column2, right.locationR as right_loc, right.column2
        | FROM left , right
        | WHERE st_intersects(left.locationL, right.locationR)""".stripMargin

    val result = prepare(qry)
    result.length shouldBe 3

    result.foreach(println)

    val stringRes = result.map(row => s"${row.get(0).toString}|${row.getLong(1)}|${row.get(2).toString}|${row.getLong(3)}")

    stringRes should contain allElementsOf Seq(
      "STObject(POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5)),None)|42|STObject(POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5)),None)|42",
      "STObject(POINT (25 20),None)|69|STObject(POINT (25 20),None)|69",
      "STObject(POINT (1 1),None)|23|STObject(POINT (1 1),None)|23")
  }

  it should "be correct for intersects self join with contructor udf used" in {

    val qry =
      """SELECT left.locationL as left_loc, left.column2, right.locationR as right_loc, right.column2
        | FROM left , right
        | WHERE st_intersects(st_geomfromwkt(left.column1), st_geomfromwkt(right.column1))""".stripMargin

    val result = prepare(qry)
    result.length shouldBe 3

    result.foreach(println)

    val stringRes = result.map(row => s"${row.get(0).toString}|${row.getLong(1)}|${row.get(2).toString}|${row.getLong(3)}")

    stringRes should contain allElementsOf Seq(
      "STObject(POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5)),None)|42|STObject(POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5)),None)|42",
      "STObject(POINT (25 20),None)|69|STObject(POINT (25 20),None)|69",
      "STObject(POINT (1 1),None)|23|STObject(POINT (1 1),None)|23")
  }

  // TODO: create parser rule and inject into spark session
  ignore should "perform an explicit join" in {

    val qry =
      """SELECT asString(left.location) as left_loc, left.column2, asString(right.location) as right_loc, right.column2
        | FROM left SPATIAL_JOIN right ON intersects(left.location, right.location)""".stripMargin

    val result = prepare(qry)
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

  ignore should "compute join with programmatic API" in {
    val s = Seq("""{ "column1": "POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))", "column2": 42 }""",
      """{ "column1": "POINT (25 20)", "column2": 69 }""")

    val sparkSession = spark
    import sparkSession.implicits._


    val l = spark.read.json(s.toDS())
    val r = spark.read.json(s.toDS())

    val left =  l.withColumn("location1", fromWKT(l("column1"))).withColumnRenamed("column2","leftCol2")
    val right = r.withColumn("location2", fromWKT(r("column1"))).withColumnRenamed("column2","rightCol2")

    val resultDF = left.join(right, $"location1" === $"location2").select("location1", "leftCol2", "location2", "rightCol2")

//    resultDF.show(truncate = false)

    val result = resultDF.collect()

    result.length shouldBe 2

    result(1).getAs[STObject]("location1") shouldBe STObject("POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))")
    result(1).getLong(1) shouldBe 42
    result(1).getAs[STObject]("location2") shouldBe STObject("POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))")
    result(1).getLong(3) shouldBe 42

    result(0).getAs[STObject]("location1") shouldBe STObject("POINT (25 20)")
    result(0).getLong(1) shouldBe 69
    result(0).getAs[STObject]("location2") shouldBe STObject("POINT (25 20)")
    result(0).getLong(3) shouldBe 69
  }

}

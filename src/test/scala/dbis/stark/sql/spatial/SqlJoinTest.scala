package dbis.stark.sql.spatial

import dbis.stark.STObject
import dbis.stark.sql.Functions.fromWKT
import dbis.stark.sql.STARKSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SqlJoinTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = STARKSession.builder()
      .master("local")
      .appName("sqltest filter")
      .getOrCreate()

  }

  override protected def afterAll(): Unit = {
    if(spark != null)
      spark.close()
  }

  private def prepareFromMemory(qry: String) = {
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

    left.createOrReplaceTempView("left")
    right.createOrReplaceTempView("right")

    // run query
    val sqlDF = spark.sql(qry)

    sqlDF

  }

  private def prepareFromFiles(qry: String) = {

    val l = spark.read.json("src/test/resources/spatialdata.json")
    val r = spark.read.json("src/test/resources/spatialdata2.json")

    val left =  l.withColumn("locationL", fromWKT(l("column1")))
    val right = r.withColumn("locationR", fromWKT(r("column1")))


    // Register the DataFrame as a SQL temporary view
    left.createOrReplaceTempView("left")
    right.createOrReplaceTempView("right")

    // run query
    val sqlDF = spark.sql(qry)

    sqlDF
  }

  "A SQL spatial join" should "be correct for intersects join" in {

//    locationL as left_loc
//    left.column1, left.column2, right.column1, right.column2, locationL, locationR
    val qry =

      """SELECT right.column1, right.column2, left.column1, left.column2
        | FROM right, left
        | WHERE st_intersects(left.locationL, right.locationR)""".stripMargin

    val result = prepareFromFiles(qry)
//    result.collect().length shouldBe 3

//    result.show(truncate = false)

    val stringRes = result.collect().map(row => s"${row.get(0).toString}|${row.getLong(1)}|${row.get(2).toString}|${row.getLong(3)}")

    stringRes should contain allElementsOf Seq(
      "POLYGON ((-73.1 40.6, -70 40.5, -72 41, -73.1 40.6))|43|POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))|42",
      "POLYGON ((-73.1 40.6, -70 40.5, -72 41, -73.1 40.6))|43|POINT (-72.5 40.75)|55")

  }

  it should "self join from file" in {
    val qry =

      """SELECT r.column1, r.column2, l.column1, l.column2
        | FROM left l, right r
        | WHERE st_intersects(l.locationL, r.locationR)""".stripMargin

    val result = prepareFromFiles(qry)
    //    result.collect().length shouldBe 3

    val stringRes = result.collect().map(row => s"${row.get(0).toString}|${row.getLong(1)}|${row.get(2).toString}|${row.getLong(3)}").mkString("\n")

//    stringRes should contain allElementsOf

    val ref = Seq(
      "POLYGON ((-73.1 40.6, -70 40.5, -72 41, -73.1 40.6))|43|POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))|42",
      "POLYGON ((-73.1 40.6, -70 40.5, -72 41, -73.1 40.6))|43|POINT (-72.5 40.75)|55").mkString("\n")

    stringRes shouldBe ref
  }

  it should "self join in memory" in {
    val qry =
      """SELECT left.column1, left.column2, right.column1, right.column2
        | FROM right, left
        | WHERE st_intersects(left.locationL, right.locationR)""".stripMargin

    val result = prepareFromMemory(qry)

    val stringRes = result.collect().map(row => s"${row.get(0).toString}|${row.getLong(1)}|${row.get(2).toString}|${row.getLong(3)}")

    stringRes should contain allElementsOf Seq(
      "POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5))|42|POLYGON ((-73.1 40.5, -70 40.5, -72 41, -73.1 40.5))|42",
      "POINT (25 20)|69|POINT (25 20)|69",
      "POINT (1 1)|23|POINT (1 1)|23")
  }

  it should "be correct for intersects self join with contructor udf used" in {

    val qry =
      """SELECT right.column1, right.column2, left.column1, left.column2
        | FROM left , right
        | WHERE st_intersects(st_geomfromwkt(left.column1), st_geomfromwkt(right.column1))""".stripMargin

    val result = prepareFromFiles(qry).collect()
    result.length shouldBe 2

    result.foreach(println)

    val stringRes = result.map(row => s"${row.get(0).toString}|${row.getLong(1)}|${row.get(2).toString}|${row.getLong(3)}")

    stringRes should contain allElementsOf Seq(
      "POLYGON ((-73.1 40.6, -70 40.5, -72 41, -73.1 40.6))|43|POLYGON ((-73.0 40.5, -70 40.5, -72 41, -73.0 40.5))|42",
      "POLYGON ((-73.1 40.6, -70 40.5, -72 41, -73.1 40.6))|43|POINT (-72.5 40.75)|55")
  }

  // TODO: create parser rule and inject into spark session
  ignore should "perform an explicit join" in {

    val qry =
      """SELECT asString(left.location) as left_loc, left.column2, asString(right.location) as right_loc, right.column2
        | FROM left SPATIAL_JOIN right ON st_intersects(left.location, right.location)""".stripMargin

    val result = prepareFromFiles(qry).collect()
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

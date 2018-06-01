package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RasterFilterVectorRDDTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    sc = new SparkContext(conf)
  }

  override protected def afterAll(): Unit = sc.stop()

//  "A RasterFilterRDD"
  ignore should " filter an unpartitioned RasterRDD" in {

    val width = 10
    val height = 10

    val tiles = for {
        w <- 0 until width
        h <- 0 until height

      } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

    val tileRDD = sc.parallelize(tiles)

    val num = tileRDD.filter(STObject("POLYGON((11 11, 89 11, 89 89, 11 89, 11 11))"), JoinPredicate.INTERSECTS).count()

    num shouldBe 100-36

  }

  ignore should "filter a partitioned RasterRDD for a large query polygon" in {

    val width = 10
    val height = 10

    val tiles = for {
      w <- 0 until width
      h <- 0 until height

    } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

    val tileRDD = sc.parallelize(tiles)

    val gridPartitioner = new RasterGridPartitioner(9,9, 0, 100, 0, 100)

    val filterRes = tileRDD.partitionBy(gridPartitioner).filter(STObject("POLYGON((11 11, 89 11, 89 89, 11 89, 11 11))")).cache()

    val num = filterRes.count()

    num shouldBe 100-36
  }

  ignore should "filter a partitioned RasterRDD with a query point" in {
    val width = 10
    val height = 10

    val tiles = for {
      w <- 0 until width
      h <- 0 until height

    } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

    val tileRDD = sc.parallelize(tiles)

    val gridPartitioner = new RasterGridPartitioner(9,9, 0, 100, 0, 100)

    val filterRes = tileRDD.partitionBy(gridPartitioner).filter(STObject("POINT(51 51)"))

    val num = filterRes.count()

    num shouldBe 1
  }

  it should "return only matching pixels" in {
    val width = 11
    val height = 7

    val arr = Array.tabulate(width * height)(identity)

    val tile = Tile(ulx = 0, uly = height, width,height, arr)

    val rdd = sc.parallelize(Seq(tile))

    val qry = STObject("POLYGON((5 -1,  7.5 3.5, 13 5.5, 13 -1, 5 -1))")

    val result = rdd.filter(qry, JoinPredicate.INTERSECTS)

    result.collect().foreach(t => println(t.matrix))

  }
}

package dbis.stark.raster

import dbis.stark.STObject
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RasterFilterVectorRDDTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    sc = new SparkContext(conf)
  }

  "A RasterFilterRDD" should " filter an unpartitioned RasterRDD" in {

    val width = 10
    val height = 10

    val tiles = for {
        w <- 0 until width
        h <- 0 until height

      } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

    val tileRDD = sc.parallelize(tiles)

    val num = tileRDD.filter(STObject("POLYGON((11 11, 89 11, 89 89, 11 89, 11 11))")).count()

    num shouldBe 100-36

  }

  it should "filter a partitioned RasterRDD for a large query polygon" in {

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

  it should "filter a partitioned RasterRDD with a query point" in {
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
}

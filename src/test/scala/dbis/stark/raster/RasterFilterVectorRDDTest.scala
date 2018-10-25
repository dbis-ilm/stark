package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate
import dbis.stark.raster.RasterRDD._

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
  it should " filter an unpartitioned RasterRDD" in {

    val width = 10
    val height = 10

    val tiles = for {
        w <- 0 until width
        h <- 0 until height

      } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

    val tileRDD = sc.parallelize(tiles)

    val num = tileRDD.filter(STObject("POLYGON((11 11, 89 11, 89 89, 11 89, 11 11))"), Byte.MinValue, JoinPredicate.INTERSECTS).count()

    num shouldBe 100-36

  }

  it should "filter a partitioned RasterRDD for a large query polygon" in {

    val width = 10
    val height = 10

    val tiles = for {
      w <- 0 until width
      h <- (1 until height+1).reverse

    } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

    val tileRDD = sc.parallelize(tiles)

    val gridPartitioner = new RasterGridPartitioner(9,9, 0, 101, 0, 101 )

    val filterRes = tileRDD.partitionBy(gridPartitioner).filter(STObject("POLYGON((11 11, 89 11, 89 89, 11 89, 11 11))"), Byte.MinValue).cache()

    val num = filterRes.count()

    num shouldBe 100-36
  }

  it should "filter a partitioned RasterRDD with a query point" in {
    val width = 10
    val height = 10

    val tiles = for {
      w <- 0 until width
      h <- (1 until height+1).reverse

    } yield Tile(w * width,h * height,width,height, Array.fill(width * height){  (w * width + h * height).toByte})

//    println(tiles.map(_.wkt).mkString("\n"))

    val tileRDD = sc.parallelize(tiles)

    val gridPartitioner = new RasterGridPartitioner(4,4, 0, 100, 0, 100)

    val parted = tileRDD.partitionBy(gridPartitioner)

    val filterRes = parted.filter(STObject("POINT(51 51)"), Byte.MinValue)

    val num = filterRes.count()

    num shouldBe 1
  }

  it should "return only matching pixels for intersecting poly" in {
    val width = 11
    val height = 7

    val arr = Array.tabulate(width * height)(identity)

    val tile = Tile(ulx = 0, uly = height, width,height, arr)

    val qry = STObject("POLYGON((5 -1,  7.5 3.5, 13 5.5, 13 -1, 5 -1))")

    val rdd = sc.parallelize(Seq(tile))

    val result = rdd.filter(qry, Integer.MIN_VALUE, JoinPredicate.INTERSECTS)

    val matchedTile = result.collect().head

    withClue("height does not match") { matchedTile.height shouldBe 5 }
    withClue("width does not match") { matchedTile.width shouldBe 6 }

    val refValues = Array(
      Seq(Integer.MIN_VALUE, Integer.MIN_VALUE),
      29 to 32,
      Seq(Integer.MIN_VALUE),
      39 to 43,
      49 to 54,
      60 to 65,
      71 to 76
    ).flatten

    matchedTile.data should contain theSameElementsAs refValues
  }

  it should "return only matching pixels for bigger polygon" in {
    val width = 11
    val height = 7

    val arr = Array.tabulate(width * height)(identity)

    val tile = Tile(ulx = 0, uly = height, width,height, arr)

    val qry = STObject("POLYGON((-1 -1,  100 -1, 100 100, -1 100, -1 -1))")

    val rdd = sc.parallelize(Seq(tile))

    val result = rdd.filter(qry, Integer.MIN_VALUE, JoinPredicate.INTERSECTS)

    val matchedTile = result.collect().head

    withClue("height does not match") { matchedTile.height shouldBe tile.height }
    withClue("width does not match") { matchedTile.width shouldBe tile.width }

    matchedTile.data should contain theSameElementsAs tile.data

    matchedTile.ulx shouldBe tile.ulx
    matchedTile.uly shouldBe tile.uly
    matchedTile.width shouldBe tile.width
    matchedTile.height shouldBe tile.height

    matchedTile.data should contain theSameElementsAs tile.data
  }

  ignore should "return only matching pixels for completely contained polygon" in {
    val width = 11
    val height = 7

    val arr = Array.tabulate(width * height)(identity)

    val tile = Tile(ulx = 0, uly = height, width,height, arr)

    val qry = STObject("POLYGON((7.5 5.5, 8.5 5.5, 8.5 6.5, 7.5 6.5, 7.5 5.5))")

    val rdd = sc.parallelize(Seq(tile))

    val result = rdd.filter(qry, Integer.MIN_VALUE, JoinPredicate.INTERSECTS)

    val matchedTile = result.collect().head

    withClue("height does not match") { matchedTile.height shouldBe 1 }
    withClue("width does not match") { matchedTile.width shouldBe 1 }

    val refValues = Array(
      7 to 8,
      18 to 19
    ).flatten

    matchedTile.data should contain theSameElementsAs refValues
  }
}

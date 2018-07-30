package dbis.stark.visualization

import java.awt.Color
import java.nio.file.{Files, Paths}

import dbis.stark.TestUtils.makeTimeStamp
import dbis.stark.{STObject, TestUtils}
import dbis.stark.raster.Tile
import dbis.stark.spatial.SpatialRDD._
import org.apache.spark.SparkContext
import org.locationtech.jts.geom.Envelope
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}



class VisualizationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll() {
    sc = TestUtils.createSparkContext("spatialrddtestcase")
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

//  "The visualization"
  ignore should "create a PNG file" in {
    val rdd = TestUtils.createRDD(sc)

    val path = "/tmp/testimg"

    rdd.visualize(800,600,path, fileExt = "png")

    Files.exists(Paths.get(s"$path.png")) shouldBe true

  }

  it should "create a PNG file on a world map" in {
    val rdd = TestUtils.createPolyRDD(sc)
    //val rdd = TestUtils.createPointRDD(sc)

    val path = "/tmp/testimg"

    rdd.visualize(4096,2048, path = path, fileExt = "png", range = (-90, 90, -180, 180),
      flipImageVert = true, worldProj = true, fillPolygon = true, bgImagePath = "src/test/resources/mercator.jpg")

    Files.exists(Paths.get(s"$path.png")) shouldBe true

  }

  ignore should "create a jpg file" in {
    val rdd = TestUtils.createRDD(sc)

    val path = "/tmp/testimg"

    rdd.visualize(800,600,path, fileExt = "jpg")

    Files.exists(Paths.get(s"$path.jpg")) shouldBe true
  }

  ignore should "create a raster png file" in {

    case class MyData(x: Int, y: Int, c: Int)

    val colors = Array( MyData(0,0, Color.GRAY.getRGB), MyData(0, 1, Color.BLUE.getRGB), MyData(0,2, Color.BLACK.getRGB),
      MyData(1,0, Color.ORANGE.getRGB), MyData(1,1,Color.YELLOW.getRGB), MyData(1,2, Color.RED.getRGB))


    val t1 = new Tile[Int](0, 0, 3, 1) //, Array(MyData(0,0, Color.RED.getRGB), MyData(1,0, Color.GREEN.getRGB), MyData(2,0, Color.BLUE.getRGB))
    t1.set(0,0, Color.RED.getRGB)
    t1.set(1,0, Color.GREEN.getRGB)
    t1.set(2,0, Color.BLUE.getRGB)


    val t2 = new Tile[MyData](3, 0, 3, 1) //, Array(MyData(0,0, Color.RED.getRGB), MyData(1,0, Color.GREEN.getRGB), MyData(2,0, Color.BLUE.getRGB))
    t2.set(3,0, MyData(0,0, Color.BLUE.getRGB))
    t2.set(4,0, MyData(1,0, Color.GREEN.getRGB))
    t2.set(5,0, MyData(2,0, Color.RED.getRGB))

    val tiles = Seq(t1)
    val rdd = sc.parallelize(tiles,2)

//    println(rdd.count())

    import dbis.stark.raster.RasterRDD._

    rdd.visualize("/tmp/raster.png", 3, 1)
  }


}

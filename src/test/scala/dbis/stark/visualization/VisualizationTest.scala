package dbis.stark.visualization

import java.nio.file.{Files, Paths}

import dbis.stark.TestUtils
import dbis.stark.spatial.SpatialRDD._
import org.apache.spark.SparkContext
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

  "The visualization" should "create a PNG file" in {
    val rdd = TestUtils.createRDD(sc)

    val path = "/tmp/testimg"

    rdd.visualize(800,600,path, fileExt = "png")

    Files.exists(Paths.get(s"$path.png")) shouldBe true

  }

  it should "create a jpg file" in {
    val rdd = TestUtils.createRDD(sc)

    val path = "/tmp/testimg"

    rdd.visualize(800,600,path, fileExt = "jpg")

    Files.exists(Paths.get(s"$path.jpg")) shouldBe true

  }


}

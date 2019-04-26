package dbis.stark.spatial

import dbis.stark.{Distance, STObject, StarkTestUtils}
import dbis.stark.spatial.indexed.RTreeConfig
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.SpatialRDD._

class SpatialKnnJoinRDDTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll() {
    sc = StarkTestUtils.createSparkContext("spatialKNNjoinrddtestcase", 1)
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

  "A SpatiakKnnJoinRDDTest" should "find 3 1nn" in {
    val start = System.currentTimeMillis()

    val right = StarkTestUtils.createPointRDD(sc).index(None, RTreeConfig(order = 5))

    val three = sc.parallelize(Seq(
      (STObject(-88.331492,32.324142,0L), 1),
      (STObject(-88.175933,32.360763,0L), 2),
      (STObject(-88.388954,32.357073,0L), 3)
    ))

    val result = three.knnJoin(right, 1, Distance.seuclid).collect()

    result.length shouldBe 3

    result should contain theSameElementsAs Seq((1,(STObject(-88.331492,32.324142,0L), -88.331492)),
                                                (2,(STObject(-88.175933,32.360763,0L), -88.175933)),
                                                (3,(STObject(-88.388954,32.357073,0L), -88.388954)))


    val end = System.currentTimeMillis()
    println(s"3 1nn join took: ${end -start} ms")

  }

}

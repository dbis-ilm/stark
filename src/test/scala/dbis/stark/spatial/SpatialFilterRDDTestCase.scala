package dbis.stark.spatial

import dbis.stark.{STObject, TestUtils}
import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SpatialFilterRDDTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll() {
    sc = TestUtils.createSparkContext("spatialfilterrddtestcase")
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

  "A SpatialFilterRDD" should "return all partitions if no spatial partitioner was applied" in {

    val raw = TestUtils.createRDD(sc)

    val q: STObject = "POINT (53.483437 -2.2040706)"

    val filter = new SpatialFilterRDD(raw, q, JoinPredicate.INTERSECTS)

    filter.getPartitions.length shouldBe raw.getNumPartitions
  }

  it should "prune partitions when GRID partitioner was applied" in {

    val raw = TestUtils.createRDD(sc)

    val grid = new SpatialGridPartitioner(raw, 10, false)

    val gridRdd = raw.partitionBy(grid)
    val q: STObject = "POINT (53.483437 -2.2040706)"

    val filter = new SpatialFilterRDD(gridRdd, q, JoinPredicate.INTERSECTS)

    gridRdd.partitioner should not be None
    gridRdd.partitioner.get shouldBe an[SpatialGridPartitioner[STObject, (String, Long, String, STObject)]]
    gridRdd.partitioner.get shouldBe grid

    filter.partitioner should not be None
    filter.partitioner.get shouldBe grid
    filter.partitioner shouldBe gridRdd.partitioner

    filter.getPartitions.length shouldBe 1
  }

  it should "prune partitions when BSP partitioner was applied" in {

    val raw = TestUtils.createRDD(sc)

    val bsp = new BSPartitioner(raw, 1, 50, false)

    val bspRdd = raw.partitionBy(bsp)
    val q: STObject = "POINT (53.483437 -2.2040706)"

    val filter = new SpatialFilterRDD(bspRdd, q, JoinPredicate.INTERSECTS)

    bspRdd.partitioner should not be None
    bspRdd.partitioner.get shouldBe an[BSPartitioner[STObject, (String, Long, String, STObject)]]
    bspRdd.partitioner.get shouldBe bsp

    filter.partitioner should not be None
    filter.partitioner.get shouldBe bsp
    filter.partitioner shouldBe bspRdd.partitioner

    filter.getPartitions.length shouldBe 1
  }

}
package org.apache.spark

import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner}
import dbis.stark.{STObject, StarkTestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SpatialFilterRDDTestCase extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll() {
    sc = StarkTestUtils.createSparkContext("spatialfilterrddtestcase")
  }

  override def afterAll() {
    if(sc != null)
      sc.stop()
  }

  "A SpatialFilterRDD" should "return all partitions if no spatial partitioner was applied" in {

    val raw = StarkTestUtils.createRDD(sc)

    val q: STObject = "POINT (53.483437 -2.2040706)"

    val filter = new SpatialFilterRDD(raw, q, JoinPredicate.INTERSECTS)

    filter.getPartitions.length shouldBe raw.getNumPartitions
  }

  it should "prune partitions when GRID partitioner was applied" in {

    val raw = StarkTestUtils.createRDD(sc)

    val grid = SpatialGridPartitioner(raw, 10, pointsOnly = true)

    val gridRdd = raw.partitionBy(grid)
    val q: STObject = "POINT (53.483437 -2.2040706)"

    val filter = new SpatialFilterRDD(gridRdd, q, JoinPredicate.INTERSECTS)

    gridRdd.partitioner should not be None
    gridRdd.partitioner.get shouldBe an[SpatialGridPartitioner[STObject]]
    gridRdd.partitioner.get shouldBe grid

    filter.partitioner should not be None
    filter.partitioner.get shouldBe grid
    filter.partitioner shouldBe gridRdd.partitioner

    filter.getPartitions.length shouldBe 1
  }

  it should "prune partitions when BSP partitioner was applied" in {

    val raw = StarkTestUtils.createRDD(sc)

    val bsp = BSPartitioner(raw, 1, 50, pointsOnly = true)

    val bspRdd = raw.partitionBy(bsp)
    val q: STObject = "POINT (53.483437 -2.2040706)"

    val filter = new SpatialFilterRDD(bspRdd, q, JoinPredicate.INTERSECTS)

    bspRdd.partitioner should not be None
    bspRdd.partitioner.get shouldBe an[BSPartitioner[STObject]]
    bspRdd.partitioner.get shouldBe bsp

    filter.partitioner should not be None
    filter.partitioner.get shouldBe bsp
    filter.partitioner shouldBe bspRdd.partitioner

    filter.getPartitions.length shouldBe 1
  }

}
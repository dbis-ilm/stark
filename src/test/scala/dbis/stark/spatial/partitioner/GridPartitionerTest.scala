package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

class GridPartitionerTest extends FlatSpec with Matchers with BeforeAndAfter {


  def makeHisto(x: Int, y: Int) = {
    val buckets = mutable.Map.empty[Int, (Cell, Int)]

    var i = -1
    for(y <- 0 to 3;
        x <- 0 to 3) {
      i += 1
      buckets += i -> (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    CellHistogram(buckets)
  }

  "A BSP async" should "compute correct cells in range covering complete space" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    GridPartitioner.getCellsIn(startRange,1,startRange,4) should contain only (0 to 15 :_*)

  }

  ignore should "compute correct cells in range covering greater space" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val theRange = NRectRange(NPoint(0, 0),NPoint(20,20))

    GridPartitioner.getCellsIn(theRange,1,startRange,4) should contain only (0 to 15 :_*)
  }

  it should "compute correct cells in range covering a single cell" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val theRange = NRectRange(NPoint(2, 2),NPoint(3,3))

    GridPartitioner.getCellsIn(theRange,1,startRange,4) should contain only 10
  }

  it should "compute correct cells in range covering multiple cells" in {

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    GridPartitioner.getCellsIn(theRange,1,startRange,4) should contain only (Seq(6,7,10,11):_*)
  }

  it should "get the correct extent even for points" in {
    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val sideLength = 1
    val numXCells = 4

    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    val extent = CostBasedPartitioner.extentForRange(theRange,sideLength ,startRange,numXCells ,histo)

    extent shouldBe theRange

  }

  it should "estimate costs correctly" in {
    val histo = makeHisto(3,3)

//    println(histo.buckets.values.mkString("\n"))

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))
    val sideLength = 1.0
//    val bspTask = new SplitTask(startRange, histo,
//      sideLength, maxCostPerPartition = 3, pointsOnly = false )

    histo.buckets.foreach{ case (_,(cell,_)) =>
      CostBasedPartitioner.costEstimation(cell.range,sideLength, startRange,4,histo) shouldBe 1
    }

    CostBasedPartitioner.costEstimation(startRange,sideLength,startRange,4,histo) shouldBe 16

    CostBasedPartitioner.costEstimation(NRectRange(NPoint(0.5, 0.5), NPoint(1,1)),sideLength,startRange,4,histo) shouldBe 1

  }

}

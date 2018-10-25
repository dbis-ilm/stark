package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class BSPBinaryAsyncTest extends FlatSpec with Matchers {

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

    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )

    bspTask.getCellsIn(startRange) should contain only (0 to 15 :_*)

  }

  ignore should "compute correct cells in range covering greater space" in {

    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )


    val theRange = NRectRange(NPoint(0, 0),NPoint(20,20))

    bspTask.getCellsIn(theRange) should contain only (0 to 15 :_*)
  }

  it should "compute correct cells in range covering a single cell" in {

    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )


    val theRange = NRectRange(NPoint(2, 2),NPoint(3,3))

    bspTask.getCellsIn(theRange) should contain only 10
  }

  it should "compute correct cells in range covering multiple cells" in {

    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )

    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    bspTask.getCellsIn(theRange) should contain only (Seq(6,7,10,11):_*)
  }

  it should "get the correct extent even for points" in {
    val histo = makeHisto(3,3)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = false )

    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    val extent = bspTask.extentForPart(theRange)

    extent shouldBe theRange


  }

  it should "estimate costs correctly" in {
    val histo = makeHisto(3,3)

    println(histo.buckets.values.mkString("\n"))

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = false )

    histo.buckets.foreach{ case (_,(cell,_)) =>
      bspTask.costEstimation(cell.range) shouldBe 1
    }

    bspTask.costEstimation(startRange) shouldBe 16

    bspTask.costEstimation(NRectRange(NPoint(0.5, 0.5), NPoint(1,1))) shouldBe 1

  }

}

package dbis.stark.spatial.partitioner

import dbis.stark.spatial
import dbis.stark.spatial.{Cell, NPoint, NRectRange}
import org.scalatest.{FlatSpec, Matchers}

class BSPBinaryAsyncTest extends FlatSpec with Matchers {

  "A BSP async" should "compute correct cells in range covering complete space" in {

    var i = -1
    val histoSeq = for(y <- 0 to 3;
        x <- 0 to 3) yield {
      i += 1
      (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    val histo = Array(histoSeq: _*)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )



    bspTask.getCellsIn(startRange) should contain only (0 to 15 :_*)

  }

  ignore should "compute correct cells in range covering greater space" in {

    var i = -1
    val histoSeq = for(y <- 0 to 3;
                       x <- 0 to 3) yield {
      i += 1
      (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    val histo = Array(histoSeq: _*)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )


    val theRange = NRectRange(NPoint(0, 0),NPoint(20,20))

    bspTask.getCellsIn(theRange) should contain only (0 to 15 :_*)
  }

  it should "compute correct cells in range covering a single cell" in {

    var i = -1
    val histoSeq = for(y <- 0 to 3;
                       x <- 0 to 3) yield {
      i += 1
      (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    val histo = Array(histoSeq: _*)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )


    val theRange = NRectRange(NPoint(2, 2),NPoint(3,3))

    bspTask.getCellsIn(theRange) should contain only 10
  }

  it should "compute correct cells in range covering multiple cells" in {

    var i = -1
    val histoSeq = for(y <- 0 to 3;
                       x <- 0 to 3) yield {
      i += 1
      (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    val histo = Array(histoSeq: _*)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = true )

    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    bspTask.getCellsIn(theRange) should contain only (Seq(6,7,10,11):_*)
  }

  it should "get the correct extent even for points" in {
    var i = -1
    val histoSeq = for(y <- 0 to 3;
                       x <- 0 to 3) yield {
      i += 1
      (Cell(i, NRectRange(NPoint(x, y), NPoint(x+1, y+1))), 1)
    }

    val histo = Array(histoSeq: _*)

    val startRange = NRectRange(NPoint(0, 0), NPoint(4,4))

    val bspTask = new SplitTask(startRange, histo,
      sideLength = 1.0, maxCostPerPartition = 3, pointsOnly = false )

    val theRange = NRectRange(NPoint(2, 1),NPoint(4,3))

    val extent = bspTask.extentForRange(theRange)

    extent shouldBe theRange


  }

}

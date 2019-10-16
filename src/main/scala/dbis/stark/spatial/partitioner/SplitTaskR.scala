package dbis.stark.spatial.partitioner

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, ExecutorService, Future}

import dbis.stark.spatial.{Cell, NRectRange}

object SplitTaskR {
  /*
   * Find the best split for in a given dimension
   * @param dim The dimension
   * @param part the partition (candidate) to process
   * @return Returns the two created partitions along with their cost difference
   */
  private def bestSplitInDimension(dim: Int, part: NRectRange, sideLength: Double, range: NRectRange, numXCells: Int,
                                   cellHistogram: CellHistogram, maxCostPerPartition: Double): (Option[(NRectRange, Int)], Option[(NRectRange,Int)], Int) = {

    val numCells = GridPartitioner.cellsPerDimension(part, sideLength)(dim)

    // there are fewer than 2 cells in the requested dimension (i.e. 0 or 1) -- we cannot further split this!
    val cost = CostBasedPartitioner.costEstimation(part,sideLength,range,numXCells,cellHistogram)
    if(numCells < 2) {
      return (Some(part, cost), None, cost)
    }

    // will store the two partitions with the minimal costDiff
    var r1,r2: Option[(NRectRange,Int)] = None
    var minDiff = Int.MaxValue


    var low = part.ll(dim)
    var up = part.ur(dim)


    var diff = 1.0
//    var prevCostDiff:Option[Int] = None
//    breakable {
      while (diff > 0 && low <= up /*&& (minDiff > 0.1 * maxCostPerPartition)*/) {

        // TODO: make sure to align at cells
        diff = ((up - low) / sideLength).toInt / 2
        val splitPos = low + diff * sideLength

        // create the two resulting partitions
        val rect1 = NRectRange(part.ll, part.ur.withValue(dim, splitPos))
        val rect2 = NRectRange(part.ll.withValue(dim, splitPos), part.ur)

        // compute costs for each partitions
        val cost1 = CostBasedPartitioner.costEstimation(rect1, sideLength, range, numXCells, cellHistogram)
        val cost2 = CostBasedPartitioner.costEstimation(rect2, sideLength, range, numXCells, cellHistogram)

        require(cost1+cost2 == cost, s"cost do not match up: $cost1 + $cost2 != $cost")

        val costDiff = cost1 - cost2
        val absCostDiff = math.abs(costDiff)
//        if (prevCostDiff.isDefined && prevCostDiff.get < costDiff) {
//          r1 = Some((rect1, cost1))
//          r2 = Some((rect2, cost2))
//          minDiff = absCostDiff
//          break
//        }
//        prevCostDiff = Some(costDiff)



        // set as new minimal cost diff --> could be our final result here
        if (absCostDiff < minDiff) {
          r1 = Some((rect1,cost1))
          r2 = Some((rect2,cost2))
          minDiff = absCostDiff
        }

        // prepare for next iteration
        if (cost1 > cost2) {
          up = splitPos
        } else if (cost1 < cost2) {
          low = splitPos + sideLength
        } else
          diff = 0
      }
//    }

    if(r1.isEmpty && r2.isEmpty) {
      r1 = Some((part,cost))
    } else if(r1.isDefined && r1.get._2 <= 0)
      r1 = None

    if(r2.isDefined && r2.get._2 <= 0)
      r2 = None

    //    val partCost = costEstimation(part)
    //    println(s"split ($dim) $part ($partCost) into \n ${r1.map(_.wkt).getOrElse("-")} ($cost1)\n ${r2.map(_.wkt).getOrElse("-")} ($cost2)")
    //    println("")

    //    require(cost1 + cost2 == partCost, s"costs do not match partCost=$partCost != cost1=$cost1 + cost2=$cost2")

    (r1, r2, minDiff)

  }


  /**
    * Find the best split for a partition
    *
    * Will check partitioning in each dimension and take the one with minimal cost difference,
    * i.e. try to create two balanced partitions
    * @param part The partition to split
    * @return Returns the two resulting partitions with the optimal split
    */
  def findBestSplit(part: NRectRange, sideLength: Double, range: NRectRange, numXCells: Int,
                    cellHistogram: CellHistogram, maxCostPerPartition: Double): (Option[(NRectRange,Int)], Option[(NRectRange,Int)]) = {

    val splitWithMinDiff = (0 until part.dim).iterator
      //                              .par // parallel processing of each dimension
      .map(dim => SplitTaskR.bestSplitInDimension(dim, part,sideLength,range,numXCells,cellHistogram,maxCostPerPartition)) // find best split for that dimension
      // results in one candidate split per dimension
      .minBy(_._3) // take best of all candidate split

    (splitWithMinDiff._1, splitWithMinDiff._2) // return only the generated two partitions
  }
}

class SplitTaskR(range: NRectRange, universe:NRectRange, sideLength: Double, cellHistogram: CellHistogram,
                 maxCostPerPartition: Double, pointsOnly: Boolean, universeNumXCells: Int,
                 running: AtomicInteger, result: ConcurrentLinkedQueue[Cell], ex: ExecutorService, mutex: Object,
                 active:ConcurrentLinkedQueue[Future[_]]) extends Runnable {

  override def run(): Unit = {
    try {
      running.incrementAndGet()

      val numCells = GridPartitioner.cellsPerDimension(range, sideLength)
      //      val numXCells = numCells(0)
      /*
       if the partition to split does not exceed the maximum cost or is a single cell,
       return this as result partition
       Note: it may happen that the cost == 0, i.e. we think the partition is empty. However, when we are sampling,
       the partition might be empty only for the sample. To avoid expensive calculations to assign a point to its
       closest partition, we add empty partitions here too.
       */
      val currCost = CostBasedPartitioner.costEstimation(range, sideLength, range, universeNumXCells, cellHistogram)
      if (( currCost <= maxCostPerPartition) || !numCells.exists(_ > 1)) {
        if (pointsOnly) {
          result.add(Cell(range))
        }
        else {
          result.add(Cell(range, CostBasedPartitioner.extentForRange(range,sideLength,universe,universeNumXCells,cellHistogram)))
        }
      } else { // need to cRmpute the split

        // find the best split and ...
        val (s1, s2) = SplitTaskR.findBestSplit(range, sideLength, universe, universeNumXCells, cellHistogram, maxCostPerPartition)

        Iterator(s1, s2).flatten.foreach { case (r, _) =>
          val f = ex.submit(new SplitTaskR(r, universe, sideLength, cellHistogram,maxCostPerPartition,pointsOnly,
            universeNumXCells,running,result,ex, mutex,active))
          active.add(f)
        }
      }
    } finally {
      running.decrementAndGet()

      mutex.synchronized(mutex.notify())
    }

  }
}
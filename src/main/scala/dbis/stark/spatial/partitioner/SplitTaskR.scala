package dbis.stark.spatial.partitioner

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, ExecutorService, Future}

import dbis.stark.spatial.{Cell, NRectRange}


object SplitTaskR {
  /*
   * Find the best split for in a given dimension
   * @param dim The dimension
   * @param range the partition (candidate) to process
   * @return Returns the two created partitions along with their cost difference
   */
  private def bestSplitInDimension(dim: Int, range: NRectRange, rangeCost: Int, sideLength: Double, universe: NRectRange, numXCells: Int,
                                   cellHistogram: CellHistogram, maxCostPerPartition: Double): (Option[(NRectRange, Int)], Option[(NRectRange,Int)], Int) = {

    val numCells = GridPartitioner.cellsPerDimension(range, sideLength)(dim)

    // there are fewer than 2 cells in the requested dimension (i.e. 0 or 1) -- we cannot further split this!
//    val cost = CostBasedPartitioner.costEstimation(range,sideLength,universe,numXCells,cellHistogram)
    if(numCells < 2) {
      return (Some(range, rangeCost), None, rangeCost)
    }

    // will store the two partitions with the minimal costDiff
    var r1,r2: Option[(NRectRange,Int)] = None
    var minDiff = Int.MaxValue

    var low = range.ll(dim)
    var up = range.ur(dim)

    var diff = 1.0
//    var prevCostDiff:Option[Int] = None
//    breakable {
      while (diff > 0 && low <= up && minDiff > 1 /*&& (minDiff > 0.1 * maxCostPerPartition)*/) {

        // TODO: make sure to align at cells
        diff = ((up - low) / sideLength).toInt / 2
        val splitPos = low + diff * sideLength

        // create the two resulting partitions
        val rect1 = NRectRange(range.ll, range.ur.withValue(dim, splitPos))
        val rect2 = NRectRange(range.ll.withValue(dim, splitPos), range.ur)

//        require(rect1.extend(rect2) == range, "rects must extend to range")

        // compute costs for each partitions
        val cost1 = CostBasedPartitioner.costEstimation(rect1, sideLength, universe, numXCells, cellHistogram)
        val cost2 = CostBasedPartitioner.costEstimation(rect2, sideLength, universe, numXCells, cellHistogram)



//        require(cost1+cost2 == rangeCost, s"cost do not match up: $cost1 + $cost2 != $rangeCost")

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
          low = splitPos // + sideLength
        } else
          diff = 0
      }
//    }

    if(r1.isEmpty && r2.isEmpty) {
      r1 = Some((range,rangeCost))
      minDiff = rangeCost
    }

    /*else if(r1.isDefined && r1.get._2 <= 0)
      r1 = None

    if(r2.isDefined && r2.get._2 <= 0)
      r2 = None
*/

    (r1, r2, minDiff)

  }


  /**
    * Find the best split for a partition
    *
    * Will check partitioning in each dimension and take the one with minimal cost difference,
    * i.e. try to create two balanced partitions
    * @param range The partition to split
    * @return Returns the two resulting partitions with the optimal split
    */
  def findBestSplit(range: NRectRange, rangeCost: Int, sideLength: Double, universe: NRectRange, numXCells: Int,
                    cellHistogram: CellHistogram, maxCostPerPartition: Double): (Option[(NRectRange,Int)], Option[(NRectRange,Int)]) = {

    val splitWithMinDiff = (0 until range.dim).iterator
      //                              .par // parallel processing of each dimension
      .map(dim => SplitTaskR.bestSplitInDimension(dim, range, rangeCost, sideLength,universe,numXCells,cellHistogram,maxCostPerPartition)) // find best split for that dimension
      // results in one candidate split per dimension
      .minBy(_._3) // take best of all candidate split

    (splitWithMinDiff._1, splitWithMinDiff._2) // return only the generated two partitions
  }
}

class SplitTaskR(range: NRectRange, rangeCost: Int, universe:NRectRange, sideLength: Double, cellHistogram: CellHistogram,
                 maxCostPerPartition: Double, pointsOnly: Boolean, universeNumXCells: Int,
                 running: AtomicInteger, result: ConcurrentLinkedQueue[Cell], ex: ExecutorService, mutex: Object,
                 active:ConcurrentLinkedQueue[Future[_]]/*, todo: mutable.Queue[SplitTaskR]*/) extends Runnable {

  override def run(): Unit = {
    try {
      running.incrementAndGet()

      val numCells = GridPartitioner.cellsPerDimension(range, sideLength)
      /*
       if the partition to split does not exceed the maximum cost or is a single cell,
       return this as result partition
       Note: it may happen that the cost == 0, i.e. we think the partition is empty. However, when we are sampling,
       the partition might be empty only for the sample. To avoid expensive calculations to assign a point to its
       closest partition, we add empty partitions here too.
       */

      if (( rangeCost <= maxCostPerPartition) || !numCells.exists(_ > 1)) {
//        if(result.toList.exists(c => c.range == range)) {
//          println(s"tried to add duplicate 1 $range")
//        }
        if (pointsOnly) {
          result.add(Cell(range))
        }
        else {
          result.add(Cell(range, CostBasedPartitioner.extentForRange(range,sideLength,universe,universeNumXCells,cellHistogram)))
        }
      } else { // need to cRmpute the split

        // find the best split and ...
        val (s1, s2) = SplitTaskR.findBestSplit(range, rangeCost, sideLength, universe, universeNumXCells, cellHistogram, maxCostPerPartition)


        require(Iterator(s1,s2).flatten.map(_._1).reduce(_.extend(_)) == range, "splits must equal parent region")

        require(s1.isDefined || s2.isDefined, "At least one split should have been defined")

        if(s1.isDefined && s2.isEmpty && s1.get._1 == range) {
//          if(result.toList.exists(c => c.range == s1.get._1)) {
//            println(s"tried to add duplicate 2 ${s1.get._1}")
//          }
          if (pointsOnly) {
            result.add(Cell(s1.get._1))
          }
          else {
            result.add(Cell(s1.get._1, CostBasedPartitioner.extentForRange(s1.get._1,sideLength,universe,universeNumXCells,cellHistogram)))
          }
        } else {
          Iterator(s1, s2).flatten.filter{case (_,cost) => cost <= 0}.foreach { case (r,_) =>
//            if(result.toList.exists(c => c.range == r)) {
//              println(s"tried to add duplicate 3 $r")
//            }
            if (pointsOnly) {
              result.add(Cell(r))
            }
            else {
              result.add(Cell(r, CostBasedPartitioner.extentForRange(r,sideLength,universe,universeNumXCells,cellHistogram)))
            }
          }
          Iterator(s1, s2).flatten.filter{case (_,cost) => cost > 0}.foreach { case (r, c) =>

            val task = new SplitTaskR(r, c,universe, sideLength, cellHistogram, maxCostPerPartition, pointsOnly,
              universeNumXCells, running, result, ex, mutex, active /*, todo*/)
            val f = ex.submit(task)
            active.add(f)
//            todo += task
          }
        }
      }
    } finally {
      running.decrementAndGet()

      mutex.synchronized(mutex.notify())
    }

  }
}
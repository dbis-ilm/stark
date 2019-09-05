package dbis.stark.spatial.partitioner

import java.util.concurrent.{ForkJoinPool, RecursiveTask}

import dbis.stark.spatial.{Cell, NRectRange}

/**
  * A binary space partitioning algorithm implementation based on
  *
  * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
  * by He, Tan, Luo, Feng, Fan
  *
  * @param _cellHistogram A list of all cells and the number of points in them. Empty cells can be left out
  * @param _sideLength The side length of the (quadratic) cell
  * @param _maxCostPerPartition The maximum cost that one partition should have to read (currently: number of points).
  * This cannot be guaranteed as there may be more points in a cell than <code>maxCostPerPartition</code>, but a cell
  * cannot be further split.
  */
class BSPBinaryAsync(_universe: NRectRange, _cellHistogram: CellHistogram, _sideLength: Double,
                     _maxCostPerPartition: Double, _pointsOnly: Boolean, _numXCells: Option[Int] = None,
                     _numCellThreshold: Int = -1) extends CostBasedPartitioner(_universe, _cellHistogram, _sideLength,
  _numXCells.getOrElse(GridPartitioner.cellsPerDimension(_universe,_sideLength)(0)),
  _maxCostPerPartition,_pointsOnly,_numCellThreshold) with Serializable {

  def this(_universe: NRectRange,_cellHistogram: CellHistogram,_sideLength: Double,_numXCells: Int,
           _maxCostPerPartition: Double,_pointsOnly: Boolean,_numCellThreshold: Int) {
    this(_universe,_cellHistogram,_sideLength,_maxCostPerPartition,_pointsOnly,Some(_numXCells),_numCellThreshold)
  }



  /**
    * Compute the partitioning using the cost based BSP algorithm
    *
    * This is a lazy value
    */
  lazy val partitions: Array[Cell] = {

    //return the non-empty cells or compute the actual partitioning
    val resultPartitions = if(cellHistogram.nonEmptyCells.nonEmpty && cellHistogram.nonEmptyCells.size <= numCellThreshold) {
      cellHistogram.nonEmptyCells.map(_._1) //.map{ cellId => cellHistogram(cellId)._1}
    } else {

      /* The actual partitioning is done using a recursive split of a candidate partition
       * The SplitTask performs the recurive binary split of the given range  (if necessary)
       */

      // the start task covering the complete data space
      val baseTask = new SplitTask(universe, universe, cellHistogram, sideLength, numXCells, maxCostPerPartition, pointsOnly)

      // parallel execution is handled by Java's ForkJoinPool
      val pool = new ForkJoinPool()

      // invoke the initial task - which will spawn new subtasks
      val partitions = pool.invoke(baseTask)

      // return the final result
      partitions
    }

    //    println(s"# result partitions: ${resultPartitions.size}")


    resultPartitions.iterator.zipWithIndex.map{ case (cell, idx) =>
      cell.id = idx
      cell
    }.toArray
  }
}

object SplitTask {
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
    if(numCells < 2) {
      return (None, None, BSP.costEstimation(part,sideLength,range,numXCells,cellHistogram))
    }

    // will store the two partitions with the minimal costDiff
    var r1,r2: Option[(NRectRange,Int)] = None
    var minDiff = Int.MaxValue


    var low = part.ll(dim)
    var up = part.ur(dim)

    var cost1, cost2: Int = 0

    var diff = 1.0
    import util.control.Breaks._
    var prevCostDiff:Option[Int] = None
    breakable {
      while (diff > 0 && low <= up && (minDiff > 0.1 * maxCostPerPartition)) {

        diff = ((up - low) / sideLength).toInt / 2
        val splitPos = low + diff * sideLength

        // create the two resulting partitions
        val rect1 = NRectRange(part.ll, part.ur.withValue(dim, splitPos))
        val rect2 = NRectRange(part.ll.withValue(dim, splitPos), part.ur)

        // compute costs for each partitions
        cost1 = BSP.costEstimation(rect1, sideLength, range, numXCells, cellHistogram)
        cost2 = BSP.costEstimation(rect2, sideLength, range, numXCells, cellHistogram)
        val costDiff = cost1 - cost2
        val absCostDiff = math.abs(costDiff)
        if (prevCostDiff.isDefined && prevCostDiff.get < costDiff) {
          r1 = Some((rect1, cost1))
          r2 = Some((rect2, cost2))
          minDiff = absCostDiff
          break
        }
        prevCostDiff = Some(costDiff)



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
    }

    if(r1.isEmpty && r2.isEmpty) {
      r1 = Some((part,0))
    } else if(cost1 <= 0)
      r1 = None

    if(cost2 <= 0)
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
      .map(dim => SplitTask.bestSplitInDimension(dim, part,sideLength,range,numXCells,cellHistogram,maxCostPerPartition)) // find best split for that dimension
      // results in one candidate split per dimension
      .minBy(_._3) // take best of all candidate split

    (splitWithMinDiff._1, splitWithMinDiff._2) // return only the generated two partitions
  }

}

/**
  * A SplitTask represents the splitting of a partition into two partitions.
  * The generated two partitions are further processed by new SplitTasks in parallel
  * @param universe The global space to find partitions in
  * @param range The range to split
  * @param cellHistogram The histogram to use for cost estimation
  * @param sideLength The side length of a cell
  * @param maxCostPerPartition Cost threshold allowed per partition
  * @param pointsOnly True if the dataset contains only points
  */
class SplitTask(universe: NRectRange,
                range: NRectRange,
                protected[stark] val cellHistogram: CellHistogram,
                private val sideLength: Double,
                private val numXCells: Int,
                private val maxCostPerPartition: Double,
                private val pointsOnly: Boolean) extends RecursiveTask[List[Cell]] {

  private val numCells = GridPartitioner.cellsPerDimension(range,sideLength)





  /**
    * Compute the partitioning or start new recursive sub tasks
    * @return Returns the list of generated partitions
    */
  override def compute(): List[Cell] = {

    // if the partition to split does not exceed the maximum cost or is a single cell,
    // return this as result partition
    if(BSP.costEstimation(range,sideLength,range,numXCells,cellHistogram) <= maxCostPerPartition ||
      !numCells.exists(_ > 1)) {
      if(pointsOnly)
        List(Cell(range))
      else
        List(Cell(range, BSP.extentForRange(range,sideLength,numXCells,cellHistogram,range)))

    } else { // need to compute the split

      // find the best split and ...
      val (s1, s2) = SplitTask.findBestSplit(range,sideLength,range,numXCells,cellHistogram,maxCostPerPartition)

      // ... create new sub tasks
      val task1 = s1.filter(r => r._2 > 0)
        .map{p =>
          new SplitTask(universe, p._1, cellHistogram, sideLength, numXCells, maxCostPerPartition, pointsOnly)}

      val task2 = s2.filter(r => r._2 > 0)
        .map{p =>
          new SplitTask(universe, p._1, cellHistogram, sideLength, numXCells, maxCostPerPartition, pointsOnly)}

      // start the first task
      task1.foreach(_.fork())
      // compute second task in current thread
      val second = task2.map(_.compute())
      // wait for first task to complete
      val first = task1.map(_.join())

      // combine results of both sub tasks
      first.getOrElse(List.empty) ++ second.getOrElse(List.empty)

    }

  }
}

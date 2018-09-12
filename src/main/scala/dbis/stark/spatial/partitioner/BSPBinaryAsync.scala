package dbis.stark.spatial.partitioner

import java.util.concurrent.{ForkJoinPool, RecursiveTask}

import dbis.stark.spatial.{Cell, NPoint, NRectRange}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer


/**
 * A binary space partitioning algorithm implementation based on 
 * 
 * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
 * by He, Tan, Luo, Feng, Fan 
 * 
 * @param cellHistogram A list of all cells and the number of points in them. Empty cells can be left out
 * @param sideLength The side length of the (quadratic) cell
 * @param maxCostPerPartition The maximum cost that one partition should have to read (currently: number of points).
 * This cannot be guaranteed as there may be more points in a cell than <code>maxCostPerPartition</code>, but a cell
 * cannot be further split.
 */
class BSPBinaryAsync(private val _start: NRectRange,
          protected[stark] val cellHistogram: Array[(Cell, Int)],
          private val sideLength: Double,
          private val maxCostPerPartition: Double,
          private val pointsOnly: Boolean,
          private val numCellThreshold: Int = -1
            ) extends Serializable {

  require(cellHistogram.nonEmpty, "cell histogram must not be empty")
  require(maxCostPerPartition > 0, "max cost per partition must not be negative or zero")
  require(sideLength > 0, "cell side length must not be negative or zero")



  /**
   * Compute the partitioning using the cost based BSP algorithm
   *
   * This is a lazy value
   */
  lazy val partitions: Array[Cell] = {
    var i = 0
    /*
     * It may happen, that data is so dense that it relies only in very few cells
     * So it may save time to return only those cells instead of computing the
     * actual partitioning
     *
     * To do so, we check the histogram and find the non-empty cells. If these
     * are fewer than a given threshold, return them as a partitioning.
     */
    val nonempty = ListBuffer.empty[Cell]
    while(i < cellHistogram.length && nonempty.length <= numCellThreshold) {
      val cell = cellHistogram(i)._1

      if(cellHistogram(i)._2 > 0)
        nonempty += cell

      i += 1
    }

    //return the non-empty cells or compute the actual partitioning
    val resultPartitions = if(i == cellHistogram.length) {
      nonempty
    } else {

      /* The actual partitioning is done using a recursive split of a candidate partition
       * The SplitTask performs the recurive binary split of the given range  (if necessary)
       */

      // the start task covering the complete data space
      val baseTask = new SplitTask(_start, cellHistogram, sideLength, maxCostPerPartition, pointsOnly)

      // parallel execution is handled by Java's ForkJoinPool
      val pool = new ForkJoinPool()

      // invoke the initial task - which will spawn new subtasks
      val partitions = pool.invoke(baseTask)

      // return the final result
      partitions
    }


    resultPartitions.iterator.zipWithIndex.map{ case (cell, idx) =>
      cell.id = idx
      cell
    }.toArray
  }
}

/**
  * A SplitTask represents the splitting of a partition into two partitions.
  * The generated two partitions are further processed by new SplitTasks in parallel
  * @param range The range to split
  * @param cellHistogram The histogram to use for cost estimation
  * @param sideLength The side length of a cell
  * @param maxCostPerPartition Cost threshold allowed per partition
  * @param pointsOnly True if the dataset contains only points
  */
class SplitTask(range: NRectRange, protected[stark] val cellHistogram: Array[(Cell, Int)],
                private val sideLength: Double,
                private val maxCostPerPartition: Double,
                private val pointsOnly: Boolean) extends RecursiveTask[List[Cell]] {

  private val numXCells = cellsPerDimension(range)(0)

  // compute how many cells fit in each dimension
  protected[spatial] def cellsPerDimension(part: NRectRange): IndexedSeq[Int] = (0 until part.dim).map { dim =>
    math.ceil(part.lengths(dim) / sideLength).toInt
  }

  // compute the ID of the cell containing a given point
  protected[spatial] def cellId(p: NPoint): Int = {
    //TODO make multidimensional
    val x = math.floor(math.abs(p(0) - range.ll(0)) / sideLength).toInt
    val y = math.floor(math.abs(p(1) - range.ll(1)) / sideLength).toInt
    y * numXCells + x
  }

  /**
    * Determine the IDs of the cells that are contained by the given range
    * @param r The range
    * @return Returns the list of Cell IDs
    */
  def getCellsIn(r: NRectRange): Seq[Int] = {
    val numCells = cellsPerDimension(r)

    // the cellId of the lower left point of the given range
    val llCellId = cellId(r.ll)

    (0 until numCells(1)).flatMap { i =>
      llCellId + i * numXCells until llCellId + numCells(0) + i * numXCells
    }
  }


  /**
    * Compute the cost for a partition, i.e. sum the cost
    * for each cell in that partition.
    *
    * @param part The partition
    * @return Returns the cost, i.e. the number of points, of the given cell
    */
  def costEstimation(part: NRectRange): Int = {
    val cellIds = getCellsIn(part)

    var i = 0
    var sum = 0
    while (i < cellIds.size) {
      val id = cellIds(i)
      if (id >= 0 && id < cellHistogram.length) {
        sum += cellHistogram(id)._2
      }
      i += 1
    }
    sum

  }

  /*
   * Find the best split for in a given dimension
   * @param dim The dimension
   * @param part the partition (candidate) to process
   * @return Returns the two created partitions along with their cost difference
   */
  private def bestSplitInDimension(dim: Int, part: NRectRange): (Option[NRectRange], Option[NRectRange], Int) = {
    val numCells = cellsPerDimension(part)(dim)

    // there are fewer than 2 cells in the requested dimension (i.e. 0 or 1) -- we cannot further split this!
    if(numCells == 0) {
      throw new IllegalStateException(s"no cell in the given range $part in dimension $dim")
//      println(s"WARNING: NO cell is in this partition $part in dim $dim")
//      return (None, None, costEstimation(part))
    }

    if(numCells == 1) {
//      println(s"WARNING: ONLY ONE cell is in this partition $part in dim $dim")
      return (Some(part), None, costEstimation(part))
    }




    // will store the two partitions with the minimal costDiff
    var r1,r2: Option[NRectRange] = None
    var minDiff = Int.MaxValue



//    val dimLength = part.lengths(dim)
//    var middleCells = (dimLength / sideLength).toInt / 2
//    var diff: Int = middleCells - part.ll(dim).toInt

    var low = part.ll(dim)
    var up = part.ur(dim)

    var cost1, cost2: Int = 0

    var diff = 1.0

    while(diff > 0 && low <= up && (minDiff > 0.1 * maxCostPerPartition)) {

      diff = ((up - low) / sideLength).toInt / 2
      val splitPos = low + diff* sideLength

      // create the two resulting partitions
      val rect1 = NRectRange(part.ll, part.ur.withValue(dim, splitPos))
      val rect2 = NRectRange(part.ll.withValue(dim, splitPos), part.ur)

      // compute costs for each partitions
      cost1 = costEstimation(rect1)
      cost2 = costEstimation(rect2)
      val costDiff = math.abs(cost1 - cost2)

      // set as new minimal cost diff --> could be our final result here
      if (costDiff < minDiff) {
        r1 = Some(rect1)
        r2 = Some(rect2)
        minDiff = costDiff
      }

      // prepare for next iteration
      if(cost1 > cost2) {
        up = splitPos
      } else if (cost1 < cost2) {
        low = splitPos + sideLength
      } else
        diff = 0
    }


//    println(s"FINISH $dim  r1=${r1.getOrElse("--")}   r2=${r2.getOrElse("--")}")

    // Iterate through all possible partitionings in this dimension and calculated the cost diff
//    var i = 1
//    while(i < numCells && (minDiff > 0.1 * maxCostPerPartition)) {
//
//      // the value in the dimension at which to split
//      val splitPos = part.ll(dim) + i*sideLength
//
//      // create the two resulting partitions
//      val rect1 = NRectRange(part.ll, part.ur.withValue(dim, splitPos))
//      val rect2 = NRectRange(part.ll.withValue(dim, splitPos), part.ur)
//
//      // compute costs for each partitions
//      val cost1 = costEstimation(rect1)
//      val cost2 = costEstimation(rect2)
//      val costDiff = math.abs(cost1 - cost2)
//
//      // set as new minimal cost diff --> could be our final result here
//      if(costDiff < minDiff) {
//        r1 = rect1
//        r2 = rect2
//        minDiff = costDiff
//      }
//
//      i += 1
//    }

    if(r1.isEmpty && r2.isEmpty) {
      r1 = Some(part)
    } else if(cost1 <= 0)
      r1 = None

    if(cost2 <= 0)
      r2 = None

    val partCost = costEstimation(part)
//    println(s"split ($dim) $part ($partCost) into \n ${r1.map(_.wkt).getOrElse("-")} ($cost1)\n ${r2.map(_.wkt).getOrElse("-")} ($cost2)")
//    println("")

    require(cost1 + cost2 == partCost, s"costs do not match partCost=$partCost != cost1=$cost1 + cost2=$cost2")

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
  def findBestSplit(part: NRectRange): (Option[NRectRange], Option[NRectRange]) = {

    val splitWithMinDiff = (0 until part.dim)
//                              .par // parallel processing of each dimension
                              .map(dim => bestSplitInDimension(dim, part)) // find best split for that dimension
                                                                            // results in one candidate split per dimension
                              .minBy(_._3) // take best of all candidate split

    (splitWithMinDiff._1, splitWithMinDiff._2) // return only the generated two partitions
  }

  /**
    * Determine the extent of the given range. The extent is computed by combining the extents
    * of all cotnained elements
    * @param part The range to determine the extent fr
    * @return Returns the extent
    */
  protected[spatial] def extentForPart(part: NRectRange): NRectRange = {

    val cellIds = getCellsIn(part)

    var i = 0
    var extent = part

    while(i < cellIds.length) {
      val id = cellIds(i)
      if(id >= 0 && id < cellHistogram.length) {
        extent = extent.extend(cellHistogram(id)._1.extent)
      }
      i += 1
    }

    extent

  }

  /**
    * Compute the partitioning or start new recursive sub tasks
    * @return Returns the list of generated partitions
    */
  override def compute(): List[Cell] = {

    // if the partition to split does not exceed the maximum cost or is a single cell,
    // return this as result partition
    if(costEstimation(range) <= maxCostPerPartition || !cellsPerDimension(range).exists(_ > 1)) {
      if(pointsOnly)
        List(Cell(range))
      else
        List(Cell(range, extentForPart(range)))

    } else { // need to compute the split

//      print(s"split $range into .... ")

      // find the best split and ...
      val (s1, s2) = findBestSplit(range)

//      println(s"$s1  and $s2")

      // ... create new sub tasks
      val task1 = s1.filter(r => !r.equals(range))
                    .map(p => new SplitTask(p, cellHistogram, sideLength, maxCostPerPartition, pointsOnly))

      val task2 = s2.filter(r => !r.equals(range))
                    .map(p => new SplitTask(p, cellHistogram, sideLength, maxCostPerPartition, pointsOnly))

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
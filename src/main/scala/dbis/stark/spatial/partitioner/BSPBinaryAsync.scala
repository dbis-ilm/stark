package dbis.stark.spatial.partitioner

import java.util.concurrent.{ForkJoinPool, RecursiveTask}

import dbis.stark.spatial.{Cell, NPoint, NRectRange}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


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



//  private lazy val numXCells = cellsPerDimension(start.range)(0) //math.ceil(math.abs(ur.head - ll.head) / sideLength).toInt





//  /**
//    * Determine the extent of the given range. The extent is computed by combining the extents
//    * of all cotnained elements
//    * @param range The range to determine the extent fr
//    * @return Returns the extent
//    */
//  protected[spatial] def extentForRange(range: NRectRange): NRectRange = {
////    getCellsIn(range)
////      .filter { id => id >= 0 && id < _cellHistogram.length } // FIXME: we should actually make sure cellInRange produces always valid cells
////      .map { id => _cellHistogram(id)._1.extent } // get the extent for the cells
////      .foldLeft(range){ (e1,e2) => e1.extend(e2) } // combine all extents to the maximum extent
//
//    val cellIds = getCellsIn(range)
//
//    var i = 0
//    var extent = range
//
//    while(i < cellIds.length) {
//      val id = cellIds(i)
//      if(id >= 0 && id < cellHistogram.length) {
//        extent = extent.extend(cellHistogram(id)._1.extent)
//      }
//      i += 1
//    }
//
//    extent
//
//  }


  /**
   * Compute the partitioning using the cost based BSP algorithm
   *
   * This is a lazy value
   */
  lazy val partitions: Array[Cell] = {
    var i = 0

    val nonempty = ListBuffer.empty[Cell]
    while(i < cellHistogram.length && nonempty.length <= numCellThreshold) {
      val cell = cellHistogram(i)._1

      if(cellHistogram(i)._2 > 0)
        nonempty += cell

      i += 1
    }


    val resultPartitions = if(i <= cellHistogram.length) {
      nonempty.toArray
    } else {

      val baseTask = new SplitTask(_start, cellHistogram, sideLength, maxCostPerPartition)
      val pool = new ForkJoinPool()
      val partitionRanges = pool.invoke(baseTask)

      val resultCells = new Array[Cell](partitionRanges.length)

        var j = 0
        val iter = partitionRanges.iterator
        while(iter.hasNext) {
          val range = iter.next()
          resultCells(j) = if(pointsOnly) {
            Cell(range)
          } else {
            val extent = range //extentForRange(range)
            Cell(range, extent)
          }
          j += 1
        }


      Array.empty[Cell]
    }

    resultPartitions
  }
}


class SplitTask(range: NRectRange, protected[stark] val cellHistogram: Array[(Cell, Int)],
                private val sideLength: Double,
                private val maxCostPerPartition: Double) extends RecursiveTask[List[NRectRange]] {

  private val numXCells = cellsPerDimension(range)(0)

  protected[spatial] def cellsPerDimension(part: NRectRange): IndexedSeq[Int] = (0 until part.dim).map { dim =>
    math.ceil(part.lengths(dim) / sideLength).toInt
  }

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

  private def bestSplitInDimension(dim: Int, part: NRectRange): (Option[NRectRange], Option[NRectRange], Int) = {

    val numCells = cellsPerDimension(part)(dim)

    if(numCells < 2) {
      return (None, None, costEstimation(part))
    }

    var r1,r2: NRectRange = null
    var minDiff = Int.MaxValue

    var i = 1
    while(i < numCells && (minDiff > 0.1 * maxCostPerPartition)) {
      val splitPos = part.ll(dim) + i*sideLength

      val rect1 = NRectRange(part.ll, part.ur.withValue(dim, splitPos))
      val rect2 = NRectRange(part.ll.withValue(dim, splitPos), part.ur)

      val cost1 = costEstimation(rect1)
      val cost2 = costEstimation(rect2)
      val costDiff = math.abs(cost1 - cost2)

      if(costDiff < minDiff) {
        r1 = rect1
        r2 = rect2
        minDiff = costDiff
      }

      i += 1
    }

    (Some(r1), Some(r2), minDiff)

//    val start = range.ll(dim)
//
//    var p1, p2: Option[NRectRange] = None
//
//    val cellsInDim = cellsPerDimension(part)(dim)
//    var splitPos = start + (cellsInDim / 2) * sideLength
//
//
//    val splitVec = part.ur.withValue(dim, splitPos)
//    var r1 = NRectRange(part.ll, splitVec)
//    var r2 = NRectRange(splitVec, part.ur)
//
//
//    var cost1 = costEstimation(r1)
//    var cost2 = costEstimation(r2)
//
//    var costDiff = math.abs(cost1 - cost2)
//
////    var iter = 0
////    while(costDiff > 0.1 * maxCostPerPartition) {
////
////      splitPos = if(cost1 > cost2) {
////        start + (cellsInDim / (2 << iter)) * sideLength
////      } else {
////
////        ???
////
////      }
////
////
////    }
//
//    (Some(r1), Some(r2), costDiff)
  }

  def findBestSplit(range: NRectRange): (Option[NRectRange], Option[NRectRange]) = {
    val splitWithMinDiff = (0 until range.dim).map(dim => bestSplitInDimension(dim, range)).minBy(_._3)
    val a = (splitWithMinDiff._1, splitWithMinDiff._2)

//    println(s"divide ${range.wkt} into \n\t ${a._1.map(_.wkt)}  and \n\t ${a._2.map(_.wkt)}")

    a
  }


  override def compute(): List[NRectRange] = {

    if(costEstimation(range) <= maxCostPerPartition || !cellsPerDimension(range).exists(_ > 1)) {
      List(range)
    } else {


      val (s1, s2) = findBestSplit(range)

      if(s1.isEmpty && s2.isDefined) {
        List(range)
      } else {

        val task1 = s1.map(p => new SplitTask(p, cellHistogram, sideLength, maxCostPerPartition))
        val task2 = s2.map(p => new SplitTask(p, cellHistogram, sideLength, maxCostPerPartition))

        task1.foreach(_.fork())
        val second = task2.map(_.compute())
        val first = task1.map(_.join())

        first.getOrElse(List.empty) ++ second.getOrElse(List.empty)
      }
    }

  }
}
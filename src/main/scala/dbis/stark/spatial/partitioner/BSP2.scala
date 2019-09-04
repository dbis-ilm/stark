package dbis.stark.spatial.partitioner

import java.util.concurrent.{ConcurrentLinkedQueue, ForkJoinTask}

import dbis.stark.spatial.{Cell, NRectRange}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
class BSP2(private val universe: NRectRange,
          protected[stark] val cellHistogram: CellHistogram,
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

    //return the non-empty cells or compute the actual partitioning
    val resultPartitions = if(cellHistogram.nonEmptyCells.nonEmpty && cellHistogram.nonEmptyCells.size <= numCellThreshold) {
      cellHistogram.nonEmptyCells.map(_._1) //.map{ cellId => cellHistogram(cellId)._1}
    } else {

      /* The actual partitioning is done using a recursive split of a candidate partition
       * The SplitTask performs the recurive binary split of the given range  (if necessary)
       */

      val queue = new ConcurrentLinkedQueue[Future[Unit]]()
      val result = ListBuffer.empty[Cell]

      compute(universe, queue, result)

      val f = Future.sequence(queue.toList)

      Await.result(f, Duration.Inf)
      result
    }

//    println(s"# result partitions: ${resultPartitions.size}")


    resultPartitions.iterator.zipWithIndex.map{ case (cell, idx) =>
      cell.id = idx
      cell
    }.toArray
  }


  def compute(range: NRectRange, queue: ConcurrentLinkedQueue[Future[Unit]], result: ListBuffer[Cell]): Unit = {

    }
}

class SplitTaskR(range: NRectRange, sideLength: Double, cellHistogram: CellHistogram, maxCostPerPartition: Double,pointsOnly:Boolean) extends ForkJoinTask[List[Cell]] {

  private val result = ListBuffer.empty[Cell]

  override def getRawResult: List[Cell] = result.toList

  override def setRawResult(value: List[Cell]): Unit = {
    result ++= value
  }

  override def exec(): Boolean = {
    val numCells = GridPartitioner.cellsPerDimension(range,sideLength)
    val numXCells = numCells(0)
    // if the partition to split does not exceed the maximum cost or is a single cell,
    // return this as result partition
    if (BSP.costEstimation(range, sideLength, range, numXCells, cellHistogram) <= maxCostPerPartition ||
      !numCells.exists(_ > 1)) {
      if (pointsOnly) {
        setRawResult(List(Cell(range)))
        true
      }
      else {
        setRawResult(List(Cell(range, BSP.extentForRange(range, sideLength, numXCells, cellHistogram, range))))
        true
      }

    } else { // need to compute the split

      // find the best split and ...
      val (s1,s2) = SplitTask.findBestSplit(range,sideLength,range,numXCells,cellHistogram,maxCostPerPartition)

      // start async computation
      val subs = List(s1,s2).filter(_.isDefined)
        .filter(_.get._2 > 0)
        .map { case Some((r, _)) =>
          new SplitTaskR(r,sideLength,cellHistogram,maxCostPerPartition,pointsOnly)
        }

      subs.foreach(_.fork())

//      subs.foreach(j => setRawResult(j.join()))
      true
    }
  }
}
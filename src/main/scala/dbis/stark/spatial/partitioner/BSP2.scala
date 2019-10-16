package dbis.stark.spatial.partitioner

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, Future}

import dbis.stark.spatial.{Cell, NRectRange}

import scala.collection.JavaConversions._
import scala.collection.mutable

object BSP2 {
  val DEFAULT_PARTITION_BUFF_SIZE = 100
}

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
class BSP2(private val _universe: NRectRange, protected[stark] val _cellHistogram: CellHistogram,
           private val _sideLength: Double, private val _pointsOnly: Boolean, private val _maxCostPerPartition: Double,
           private val _numXCells: Option[Int] = None, private val _numCellThreshold: Int = -1)
  extends CostBasedPartitioner(_universe, _cellHistogram, _sideLength,
    _numXCells.getOrElse(GridPartitioner.cellsPerDimension(_universe,_sideLength)(0)),
    _maxCostPerPartition,_pointsOnly,_numCellThreshold) with Serializable {

  def this(_universe: NRectRange,_cellHistogram: CellHistogram,_sideLength: Double,_numXCells: Int,
           _maxCostPerPartition: Double,_pointsOnly: Boolean,_numCellThreshold: Int) {
    this(_universe,_cellHistogram,_sideLength,_pointsOnly, _maxCostPerPartition,Some(_numXCells),_numCellThreshold)
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

      val result = new ConcurrentLinkedQueue[Cell]()
      val running = new AtomicInteger(0)

      val active = new ConcurrentLinkedQueue[Future[_]]()
      val mutex = new Object()

      val todo = mutable.Queue.empty[SplitTaskR]

      val ex = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() ) //

      val universeCost = cellHistogram.totalCost

      val baseTask = new SplitTaskR(universe,universeCost,universe,sideLength,cellHistogram,maxCostPerPartition,pointsOnly,
        numXCells, running, result,ex,mutex,active /*, todo*/)


//      todo += baseTask
//      while(todo.nonEmpty) {
//        val nextTask = todo.dequeue()
//        nextTask.run()
//      }

      val f = ex.submit(baseTask)
      active.add(f)

      while(!f.isDone || running.get() > 0 /* || active.toList.exists(!_.isDone)*/) {
        mutex.synchronized(mutex.wait())
      }

      active.toList.foreach(_.get())
      ex.shutdown()

      result.toSet

    }

//    println(s"# result partitions: ${resultPartitions.size}")

    resultPartitions.iterator.zipWithIndex.map{ case (cell, idx) =>
      cell.id = idx
      cell
    }.toArray
  }
}
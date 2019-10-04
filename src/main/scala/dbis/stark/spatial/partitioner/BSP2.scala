package dbis.stark.spatial.partitioner

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, ExecutorService, Executors, Future}

import dbis.stark.spatial.{Cell, NRectRange}

import scala.collection.JavaConversions._

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

      val ex = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
      val baseTask = new SplitTaskR(universe,sideLength,cellHistogram,maxCostPerPartition,pointsOnly, running, result,ex,mutex,active)
      val f = ex.submit(baseTask)
      active.add(f)

      while(!f.isDone || running.get() > 0) {
        mutex.synchronized(mutex.wait())
      }

      active.toList.foreach(_.get())

      ex.shutdown()

      result.toList

    }

//    println(s"# result partitions: ${resultPartitions.size}")

    resultPartitions.iterator.zipWithIndex.map{ case (cell, idx) =>
      cell.id = idx
      cell
    }.toArray
  }
}

class SplitTaskR(range: NRectRange, sideLength: Double, cellHistogram: CellHistogram,maxCostPerPartition: Double, pointsOnly: Boolean, running: AtomicInteger, result: ConcurrentLinkedQueue[Cell], ex: ExecutorService, mutex: Object, active:ConcurrentLinkedQueue[Future[_]]) extends Runnable {
  override def run(): Unit = {
    try {
      running.incrementAndGet()

      val numCells = GridPartitioner.cellsPerDimension(range, sideLength)
      val numXCells = numCells(0)
      /*
       if the partition to split does not exceed the maximum cost or is a single cell,
       return this as result partition
       Note: it may happen that the cost == 0, i.e. we think the partition is empty. However, when we are sampling,
       the partition might be empty only for the sample. To avoid expensive calculations to assign a point to its
       closest partition, we add empty partitions here too.
       */
      val currCost = BSP.costEstimation(range, sideLength, range, numXCells, cellHistogram)
      if (( currCost <= maxCostPerPartition) || !numCells.exists(_ > 1)) {
        if (pointsOnly) {
          result.add(Cell(range))
        }
        else {
          result.add(Cell(range, BSP.extentForRange(range, sideLength, numXCells, cellHistogram, range)))
        }
      } else { // need to compute the split

        // find the best split and ...
        val (s1, s2) = SplitTask.findBestSplit(range, sideLength, range, numXCells, cellHistogram, maxCostPerPartition)

        Iterator(s1, s2).flatten.foreach { case (r, _) =>
          val f = ex.submit(new SplitTaskR(r, sideLength, cellHistogram,maxCostPerPartition,pointsOnly,running,result,ex, mutex,active))
          active.add(f)
        }
      }
    } finally {
      running.decrementAndGet()

      mutex.synchronized(mutex.notify())
    }

  }
}
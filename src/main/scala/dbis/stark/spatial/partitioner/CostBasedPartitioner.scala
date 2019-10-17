package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NRectRange}

object CostBasedPartitioner {
  /**
    * Compute the cost for a partition, i.e. sum the cost
    * for each cell in that partition.
    *
    * @param part The partition
    * @return Returns the cost, i.e. the number of points, of the given cell
    */
  def costEstimation(part: NRectRange, sideLength: Double, universe: NRectRange, numXCells: Int,
                     cellHistogram: CellHistogram): Int = {
    val cellIds = GridPartitioner.getCellsIn(part,sideLength,universe,numXCells)

    var i = 0

    var sum = 0
    while (i < cellIds.length) {
      val id = cellIds(i)
      if (id >= 0 && id < cellHistogram.length) {
        sum += cellHistogram(id)._2
      }
      i += 1
    }
    sum
  }

  /**
    * Determine the extent of the given range. The extent is computed by combining the extents
    * of all cotnained elements
    * @param range The range to determine the extent fr
    * @return Returns the extent
    */
  protected[spatial] def extentForRange(range: NRectRange,sideLength: Double, universe: NRectRange, numXCells: Int,
                                        cellHistogram: CellHistogram): NRectRange = {
    //    getCellsIn(range)
    //      .filter { id => id >= 0 && id < _cellHistogram.length } // FIXME: we should actually make sure cellInRange produces always valid cells
    //      .map { id => _cellHistogram(id)._1.extent } // get the extent for the cells
    //      .foldLeft(range){ (e1,e2) => e1.extend(e2) } // combine all extents to the maximum extent

    val cellIds = GridPartitioner.getCellsIn(range, sideLength, universe, numXCells)

    var i = 0
    var extent = range

    while(i < cellIds.length) {
      val id = cellIds(i)
      if(id >= 0 && id < cellHistogram.length) {
        extent = extent.extend(cellHistogram(id)._1.extent)
      }
      i += 1
    }

    extent
  }

}

abstract class CostBasedPartitioner(val universe: NRectRange,
                                    val cellHistogram: CellHistogram,
                                    val sideLength: Double,
                                    val numXCells: Int,
                                    val maxCostPerPartition: Double,
                                    val pointsOnly: Boolean,
                                    val numCellThreshold: Int = -1) extends Serializable {

  require(cellHistogram.nonEmpty, "cell histogram must not be empty")
  require(maxCostPerPartition > 0, "max cost per partition must not be negative or zero")
  require(sideLength > 0, "cell side length must not be negative or zero")

  val partitions: Array[Cell]
}
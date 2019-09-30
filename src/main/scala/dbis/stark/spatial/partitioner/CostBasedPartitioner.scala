package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NRectRange}

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
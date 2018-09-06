package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object PartitionStrategy extends Enumeration {
  type PartitionStrategy = Value
  val GRID, BSP, RTREE = Value
}
import PartitionStrategy._


abstract class PartitionerConfig(val strategy: PartitionStrategy,
                                 pointsOnly: Boolean,
                                 minmax: Option[(Double, Double, Double, Double)]
                                ) extends Serializable

case class BSPStrategy(cellSize: Double,
                       maxCost: Double,
                       pointsOnly: Boolean = false,
                       minmax: Option[(Double, Double, Double, Double)] = None,
                       sampleFactor: Double = 0
                      ) extends PartitionerConfig(PartitionStrategy.BSP, pointsOnly, minmax)

case class GridStrategy(partitionsPerDimensions: Int,
                        pointsOnly: Boolean = false,
                        minmax: Option[(Double, Double, Double, Double)] = None
                      ) extends PartitionerConfig(PartitionStrategy.GRID, pointsOnly, minmax)

case class RTreeStrategy(order: Int, pointsOnly: Boolean = false,
                         minmax: Option[(Double, Double, Double, Double)] = None,
                         sampleFactor: Double = 0) extends PartitionerConfig(PartitionStrategy.RTREE, pointsOnly, minmax)


object PartitionerFactory {
  def get[G <: STObject : ClassTag, V : ClassTag](strategy: PartitionerConfig, rdd: RDD[(G, V)]): SpatialPartitioner = strategy match {
    case BSPStrategy(cellSize, maxCost, pointsOnly, minmax, sampleFactor) => minmax match {
      case None => new BSPartitioner(rdd, cellSize, maxCost, pointsOnly)
      case Some(mm) => new BSPartitioner(rdd, cellSize, maxCost, pointsOnly, mm, sampleFactor)
    }

    case GridStrategy(partitionsPerDimensions, pointsOnly, minmax) => minmax match {
      case None => new SpatialGridPartitioner(rdd, partitionsPerDimensions, pointsOnly)
      case Some(mm) => new SpatialGridPartitioner(rdd, partitionsPerDimensions, pointsOnly, mm, dimensions = 2)
    }

    case RTreeStrategy(order, pointsOnly, minmax, sampleFactor) =>
      val sample = if(sampleFactor > 0) rdd.sample(withReplacement = false, sampleFactor).collect() else rdd.collect()
      minmax match {
        case None => new RTreePartitioner(sample, order, pointsOnly)
        case Some(mm) => new RTreePartitioner(sample, order, mm, pointsOnly)
      }


  }
}



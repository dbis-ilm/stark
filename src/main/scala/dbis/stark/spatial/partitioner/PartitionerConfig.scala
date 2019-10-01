package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object PartitionStrategy extends Enumeration {
  type PartitionStrategy = Value
  val NONE, GRID, BSP, RTREE = Value
}
import PartitionStrategy._


abstract class PartitionerConfig(val strategy: PartitionStrategy,
                                 pointsOnly: Boolean,
                                 minmax: Option[(Double, Double, Double, Double)],
                                 sampleFraction: Double = 0
                                ) extends Serializable

case class BSPStrategy(cellSize: Double,
                       maxCost: Double,
                       pointsOnly: Boolean = false,
                       minmax: Option[(Double, Double, Double, Double)] = None,
                       sampleFraction: Double = 0,
                       parallel: Boolean = true
                      ) extends PartitionerConfig(PartitionStrategy.BSP, pointsOnly, minmax, sampleFraction)

case class GridStrategy(partitionsPerDimensions: Int,
                        pointsOnly: Boolean = false,
                        minmax: Option[(Double, Double, Double, Double)] = None,
                        sampleFraction: Double
                      ) extends PartitionerConfig(PartitionStrategy.GRID, pointsOnly, minmax, sampleFraction)

case class RTreeStrategy(order: Int, pointsOnly: Boolean = false,
                         minmax: Option[(Double, Double, Double, Double)] = None,
                         sampleFraction: Double = 0) extends PartitionerConfig(PartitionStrategy.RTREE, pointsOnly, minmax,sampleFraction)

case class NoPartitionerStrategy() extends PartitionerConfig(PartitionStrategy.NONE,true,None,0)


object PartitionerFactory {
  def get[G <: STObject : ClassTag, V : ClassTag](strategy: PartitionerConfig, rdd: RDD[(G, V)]): Option[GridPartitioner] = strategy match {
    case BSPStrategy(cellSize, maxCost, pointsOnly, minmax, sampleFactor,parallel) => minmax match {
      case None => Some(new BSPartitioner(rdd, cellSize, maxCost, pointsOnly,sampleFraction = sampleFactor, parallel = parallel))
      case Some(mm) => Some(new BSPartitioner(rdd, cellSize, maxCost, pointsOnly, mm, sampleFactor,parallel))
    }

    case GridStrategy(partitionsPerDimensions, pointsOnly, minmax, sampleFraction) => minmax match {
      case None => Some(new SpatialGridPartitioner(rdd, partitionsPerDimensions, pointsOnly,sampleFraction=sampleFraction))
      case Some(mm) => Some(new SpatialGridPartitioner(rdd, partitionsPerDimensions, pointsOnly, mm, dimensions = 2, sampleFraction))
    }

    case RTreeStrategy(order, pointsOnly, minmax, sampleFactor) =>
      val sample = if(sampleFactor > 0) rdd.sample(withReplacement = false, sampleFactor).map(_._1).collect() else rdd.map(_._1).collect()
      minmax match {
        case None => Some(new RTreePartitioner(sample, order, pointsOnly))
        case Some(mm) => Some(new RTreePartitioner(sample, order, mm, pointsOnly))
      }

    case NoPartitionerStrategy() => None
  }
}



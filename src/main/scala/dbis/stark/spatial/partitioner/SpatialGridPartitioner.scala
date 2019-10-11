package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.STObject
import dbis.stark.spatial.{Cell, NRectRange, StarkUtils}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object SpatialGridPartitioner {

  def apply[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
           partitionsPerDimension: Int,
           pointsOnly: Boolean,
           minMax: (Double, Double, Double, Double),
           dimensions: Int,
           sampleFraction: Double): SpatialGridPartitioner[G] = {

    require(dimensions == 2, "Only 2 dimensions supported currently")

    val (minX, maxX, minY, maxY) = minMax

    val xLength: Double = math.abs(maxX - minX) / partitionsPerDimension
    val yLength: Double = math.abs(maxY - minY) / partitionsPerDimension

    val histo = if(pointsOnly) {
      GridPartitioner.buildGrid(partitionsPerDimension,partitionsPerDimension, xLength, yLength, minX,minY)
    } else {
      //      val theRDD = if(sampleFraction > 0) rdd.sample(withReplacement = false, sampleFraction) else rdd
      GridPartitioner.buildHistogram(rdd.map(_._1.getGeo.getEnvelopeInternal),pointsOnly,partitionsPerDimension,
        partitionsPerDimension,minX,minY,maxX,maxY,xLength,yLength)
    }

    val partitions = histo.buckets.values.map(_._1).toArray.sortBy(_.id)

    new SpatialGridPartitioner(partitions, pointsOnly, minMax._1, minMax._2, minMax._3, minMax._4, xLength, yLength, partitionsPerDimension)
  }

  def apply[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)], partitionsPerDimension: Int, pointsOnly: Boolean,
           dimensions: Int = 2, sampleFraction: Double = 0): SpatialGridPartitioner[G] =
    SpatialGridPartitioner(rdd, partitionsPerDimension, pointsOnly, GridPartitioner.getMinMax(rdd.map(_._1.getGeo.getEnvelopeInternal)), dimensions, sampleFraction)

}


/**
 * A grid partitioner that simply applies a grid to the data space.
 *
 * The grid is applied from the lower left point (xmin, ymin) to the (xmax + 1, ymax + 1)
 * so that we only have grid cells over potentially filled space.
 *
 * @author hage
 */
class SpatialGridPartitioner[G <: STObject : ClassTag]private[partitioner](_partitions: Array[Cell],
   protected val pointsOnly: Boolean,
   _minX: Double, _maxX: Double,_minY: Double,_maxY: Double,
   xLength: Double, yLength: Double, partitionsPerDimension: Int) extends GridPartitioner(_partitions, _minX, _maxX, _minY, _maxY) {

  require(partitions.nonEmpty, "need at least some partitions!")

  override def printPartitions(fName: Path): Unit = {
    val list2 = partitions.iterator.map { cell => s"${cell.id};${cell.range.wkt}" }.toList
    GridPartitioner.writeToFile(list2, fName)
  }

  override def partitionBounds(idx: Int): Cell = partitions(idx)
//    partitions.get(idx) match {
//    case None =>
//      val range = GridPartitioner.getCellBounds(idx, partitionsPerDimension, xLength, yLength, minX, minY)
//      Cell(idx, range)
//    case Some((cell, _)) => cell
//  }

//    partitions(idx)._1 //getCellBounds(idx)

  override def partitionExtent(idx: Int): NRectRange = partitions(idx).extent

  override def numPartitions: Int = partitions.length //Math.pow(partitionsPerDimension,dimensions).toInt

  /**
   * Compute the partition for an input key.
   * In fact, this is a Geometry for which we use its centroid for
   * the computation
   *
   * @param key The key geometry to compute the partition for
   * @return The Index of the partition
   */
  override def getPartitionId(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val center = StarkUtils.getCenter(g.getGeo)

    val id = GridPartitioner.getCellId(center.getX, center.getY, minX, minY, maxX, maxY, xLength, yLength, partitionsPerDimension)

    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id  for input $key ($g) with center $center")

    id
  }

//  override def getAllPartitions(key: Any): List[Int] = {
//    val g = key.asInstanceOf[G]
//    partitions.filter{ case (Cell(_,_,extent),_) =>
//      extent.intersects(StarkUtils.fromEnvelope(g.getGeo.getEnvelopeInternal))
//    }.map(_._1.id).toList
//  }

  override def equals(obj: scala.Any) = obj match {
    case sp: SpatialGridPartitioner[G] =>
      sp.partitions sameElements partitions
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Iterator(partitionsPerDimension, pointsOnly, minX, maxX,minY,maxY)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
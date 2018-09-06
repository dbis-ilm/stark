package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.STObject
import dbis.stark.spatial.{Cell, NRectRange, Utils}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A grid partitioner that simply applies a grid to the data space.
 *
 * The grid is applied from the lower left point (xmin, ymin) to the (xmax + 1, ymax + 1)
 * so that we only have grid cells over potentially filled space.
 *
 * @author hage
 * @param partitionsPerDimension The number of partitions per dimension. This results in ppD to the power of dimension partitions
 * @param rdd The [[org.apache.spark.rdd.RDD]] to partition
 * @param dimensions The dimensionality of the input data
 */
class SpatialGridPartitioner[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
                                                                    protected val partitionsPerDimension: Int,
                                                                    protected val pointsOnly: Boolean,
                                                                    _minX: Double,
                                                                    _maxX: Double,
                                                                    _minY: Double,
                                                                    _maxY: Double,
                                                                    dimensions: Int) extends SpatialPartitioner(_minX, _maxX, _minY, _maxY) {

  require(dimensions == 2, "Only 2 dimensions supported currently")

  def this(rdd: RDD[(G,V)],
           partitionsPerDimension: Int,
           pointsOnly: Boolean,
           minMax: (Double, Double, Double, Double),
           dimensions: Int) =
    this(rdd, partitionsPerDimension, pointsOnly, minMax._1, minMax._2, minMax._3, minMax._4, dimensions)

  def this(rdd: RDD[(G,V)],
           partitionsPerDimension: Int,
           pointsOnly: Boolean,
           dimensions: Int = 2) =
    this(rdd, partitionsPerDimension, pointsOnly, SpatialPartitioner.getMinMax(rdd), dimensions)


  protected[this] val xLength: Double = math.abs(maxX - minX) / partitionsPerDimension
  protected[this] val yLength: Double = math.abs(maxY - minY) / partitionsPerDimension

//  new Array[Cell](numPartitions) //Map.empty[Int, Cell]
  private val partitions: Array[(Cell,Int)] = {
    if(pointsOnly) {
      SpatialPartitioner.buildGrid(partitionsPerDimension,partitionsPerDimension, xLength, yLength, minX,minY)
    } else {
      SpatialPartitioner.buildHistogram(rdd,pointsOnly,partitionsPerDimension,partitionsPerDimension,minX,minY,maxX,maxY,xLength,yLength)

    }
  }

  override def printPartitions(fName: Path): Unit = {
    val list2 = partitions.map { case (cell, _) => s"${cell.id};${cell.range.wkt}" }.toList
    super.writeToFile(list2, fName)
  }

  override def partitionBounds(idx: Int): Cell = partitions(idx)._1 //getCellBounds(idx)

  override def partitionExtent(idx: Int): NRectRange = partitions(idx)._1.extent

  override def numPartitions: Int = Math.pow(partitionsPerDimension,dimensions).toInt

  /**
   * Compute the partition for an input key.
   * In fact, this is a Geometry for which we use its centroid for
   * the computation
   *
   * @param key The key geometry to compute the partition for
   * @return The Index of the partition
   */
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val center = Utils.getCenter(g.getGeo)

    val id = SpatialPartitioner.getCellId(center.getX, center.getY, minX, minY, maxX, maxY, xLength, yLength, partitionsPerDimension)

    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id")

    id
  }

  override def equals(obj: scala.Any) = obj match {
    case sp: SpatialGridPartitioner[G,_] =>
      sp.partitionsPerDimension == partitionsPerDimension &&
      sp.pointsOnly == pointsOnly &&
      sp.minX == minX && sp.maxX == maxX &&
      sp.minY == minY && sp.maxY == maxY
    case _ => false
  }



  override def hashCode(): Int = {
    val state = Iterator(partitionsPerDimension, pointsOnly, minX, maxX,minY,maxY)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
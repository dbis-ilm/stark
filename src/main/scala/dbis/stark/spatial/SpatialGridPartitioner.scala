package dbis.stark.spatial

import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import scala.collection.mutable.Map
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.Envelope

import dbis.stark.STObject

/**
 * A grid partitioner that simply applies a grid to the data space.
 * 
 * The grid is applied from the lower left point (xmin, ymin) to the (xmax + 1, ymax + 1)
 * so that we only have grid cells over potentially filled space.  
 * 
 * @author hage
 * 
 * @param partitionsPerDimension The number of partitions per dimension. This results in ppD^dimension partitions
 * @param rdd The [[org.apache.spark.RDD]] to partition
 * @param dimensions The dimensionality of the input data 
 */
class SpatialGridPartitioner[G <: STObject : ClassTag, V: ClassTag](
    @transient private val rdd: RDD[(G,V)],
    partitionsPerDimension: Int, 
    _minX: Double,
    _maxX: Double,
    _minY: Double,
    _maxY: Double,
    dimensions: Int = 2) extends SpatialPartitioner(rdd, _minX, _maxX, _minY, _maxY) {
  
  require(dimensions == 2, "Only 2 dimensions supported currently")
  
  def this(rdd: RDD[(G,V)],
      partitionsPerDimension: Int, 
      minMax: (Double, Double, Double, Double)) = 
    this(rdd, partitionsPerDimension, minMax._1, minMax._2, minMax._3, minMax._4)  
  
  def this(rdd: RDD[(G,V)],
      partitionsPerDimension: Int) = 
    this(rdd, partitionsPerDimension, SpatialPartitioner.getMinMax(rdd))
  
  
  protected[this] val xLength = math.abs(maxX - minX) / partitionsPerDimension
  protected[this] val yLength = math.abs(maxY - minY) / partitionsPerDimension
  
  
  private val partitions = Map.empty[Int, Cell]
  
  protected[spatial] def getCellBounds(id: Int): Cell = {
    
    require(id >= 0 && id < numPartitions, s"Invalid cell id (0 .. $numPartitions): $id")
    
    val dy = id / partitionsPerDimension
    val dx = id % partitionsPerDimension
    
    val llx = dx * xLength + minX
    val lly = dy * yLength + minY
    
    val urx = llx + xLength
    val ury = lly + yLength
      
    Cell(NRectRange(id, NPoint(llx, lly), NPoint(urx, ury)))
  }
  /**
   * Compute the cell id of a data point
   * 
   * @param point The point to compute the cell id for
   * @returns Returns the number (ID) of the cell the given point lies in
   */
  private def getCellId(p: NPoint): Int = {
    
    require(p(0) >= minX && p(0) <= maxX || p(1) >= minY || p(1) <= maxY, s"$p out of range!")
      
    val newX = math.abs(p(0) - minX)
    val newY = math.abs(p(1) - minY)
    
    val x = (newX.toInt / xLength).toInt
    val y = (newY.toInt / yLength).toInt
    
    val cellId = y * partitionsPerDimension + x
    
    cellId
  }
  
  override def partitionBounds(idx: Int) = getCellBounds(idx)//partitions(idx)
  override def partitionExtent(idx: Int) = partitions(idx).extent
  
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
    
    val center = g.getCentroid
    val p = NPoint(center.getX, center.getY)
    
    val id = getCellId(p)
    
    val env = g.getEnvelopeInternal
    val gExtent = NRectRange(NPoint(env.getMinX, env.getMinY), NPoint(env.getMaxX, env.getMaxY))
    
    if(partitions.contains(id)) {
      val old = partitions(id)
      val extent = old.extent.extend(gExtent)
      partitions.update(id, Cell(old.range, extent))
    } else {
      val bounds = getCellBounds(id)
      partitions.put(id, bounds)
    }
    
    
    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id")
    
    id
  }
  
}
package dbis.stark.spatial

import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
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
    partitionsPerDimension: Int, 
    rdd: RDD[(G,V)], 
    dimensions: Int = 2) extends SpatialPartitioner(rdd) {
  
  require(dimensions == 2, "Only 2 dimensions supported currently")
  
  protected[this] val xLength = (maxX - minX) / partitionsPerDimension
  protected[this] val yLength = (maxY - minY) / partitionsPerDimension
  
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
      
    val newX = p(0) - minX
    val newY = p(1) - minY
    
    val x = (newX.toInt / xLength).toInt
    val y = (newY.toInt / yLength).toInt
    
    val cellId = y * partitionsPerDimension + x
    
    cellId
  }
  
  override def partitionBounds(idx: Int) = getCellBounds(idx)
  
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
    val center = key.asInstanceOf[G].getCentroid
    
    val p = NPoint(center.getX, center.getY)
    
    val id = getCellId(p)
    
    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id")
    
    id
  }
  
}
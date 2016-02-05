package dbis.spark.spatial

import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.Envelope


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
class SpatialGridPartitioner[G <: Geometry : ClassTag, V: ClassTag](
    partitionsPerDimension: Int, 
    rdd: RDD[_ <: Product2[G,V]], 
    dimensions: Int = 2) extends SpatialPartitioner {
  
  require(dimensions == 2, "Only 2 dimensions supported currently")
  
  import SpatialGridPartitioner._
  
  override def numPartitions: Int = Math.pow(partitionsPerDimension,dimensions).toInt
  
  /*
   * Compute min and max values per dimension as well as grid cell sizes
   * 
   * TODO: Currently 2-dimensional only
   */
  protected[this] val (minX, maxX, minY, maxY) = {
    
    val coords = rdd.map{ case (g,v) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }.cache() // cache for re-use
    
    val minX = coords.map(_._1).min()
    val maxX = coords.map(_._2).max() + 1 // +1 to also include points that lie on the maxX value
    
    val minY = coords.map(_._3).min()
    val maxY = coords.map(_._4).max() + 1 // +1 to also include points that lie on the maxY value
    
    (minX, maxX, minY, maxY)
  }
  
  protected[this] val xLength = (maxX - minX) / partitionsPerDimension
  protected[this] val yLength = (maxY - minY) / partitionsPerDimension
  
  protected[spatial] def getCellBounds(id: Int): RectRange = {
    
    require(id >= 0 && id < numPartitions, s"Invalid cell id (0 .. $numPartitions): $id")
    
    val dy = id / partitionsPerDimension
    val dx = id % partitionsPerDimension
    
    val llx = dx * xLength + minX
    val lly = dy * yLength + minY
    
    val urx = llx + xLength
    val ury = lly + yLength
      
    RectRange(id, Point(llx, lly), Point(urx, ury))
  }
  /**
   * Compute the cell id of a data point
   * 
   * @param point The point to compute the cell id for
   * @returns Returns the number (ID) of the cell the given point lies in
   */
  private def getCellId(p: Point): Int = {
    
    require(p.x >= minX || p.x <= maxX || p.y >= minY || p.y <= maxY, s"$p out of range!")
      
    val newX = p.x - minX
    val newY = p.y - minY
    
    val x = (newX.toInt / xLength).toInt
    val y = (newY.toInt / yLength).toInt
    
    val cellId = y * partitionsPerDimension + x
    
    cellId
  }
  
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
    
    val p = Point(center.getX, center.getY)
    
    val id = getCellId(p)
    
    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id")
    
    id
  }
  
}

// Helper classes
object SpatialGridPartitioner {

  implicit def makePoint(t: (Double, Double)): Point = Point(t._1, t._2) 
  
  protected[spatial] case class Point(x: Double, y: Double) {
    protected[spatial] def this(p: com.vividsolutions.jts.geom.Point) = this(p.getX, p.getY)
    protected[spatial] def this(g: Geometry) = this(g.getCentroid)
  }
  
  protected[spatial] case class RectRange(id: Int, ll: Point, ur: Point) {
    
    def contains(p: Point): Boolean = p.x >= ll.x && p.y >= ll.y && p.x < ur.x && p.y < ur.y
    
    def contains(rect: RectRange): Boolean = ll.x <= rect.ll.x && ll.y <= rect.ll.y && ur.x >= rect.ur.x && ur.y >= rect.ur.y
    
    def toEnvelope: Envelope = {
      val s = s"""POLYGON ((${ll.x} ${ll.y}, ${ur.x} ${ll.y}, ${ur.x} ${ur.y}, ${ll.x} ${ur.y}, ${ll.x} ${ll.y}))"""
      new WKTReader().read(s).getEnvelopeInternal 
    }
    
    def area = (ur.x - ll.x) * (ur.y - ll.y)
  }
  
  protected[spatial] object RectRange {
    
    protected[spatial] def apply(ll: Point, ur: Point) = new RectRange(-1, ll, ur)
    
  }
}
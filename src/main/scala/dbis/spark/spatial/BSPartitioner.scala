package dbis.spark.spatial

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import etm.core.monitor.EtmMonitor
import dbis.spatial.{NPoint,NRectRange}
import dbis.spatial.partitioner.BSP

/**
 * A cost based binary space partitioner based on the paper
 * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
 * by He, Tan, Luo, Feng, Fan 
 * 
 * @param rdd The RDD to partition
 * @param sideLength side length of a quadratic cell - defines granularity
 * @param maxCostPerPartition Maximum cost a partition should have - here: number of elements  
 */
class BSPartitioner[G <: Geometry : ClassTag, V: ClassTag](
    @transient private val rdd: RDD[_ <: Product2[G,V]],
    sideLength: Double,
    maxCostPerPartition: Double = 1.0) extends SpatialPartitioner {
  
  
  /** 
   * The lower left and uppper right corner points
   * of the data space in the RDD
   */
  protected[spatial] lazy val (minX, maxX, minY, maxY) = {
    
    val coords = rdd.map{ case (g,v) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }.cache() // cache for re-use
    
    val minX = coords.map(_._1).min()
    val maxX = coords.map(_._2).max() + 1 // +1 to also include points that lie on the maxX value
    
    val minY = coords.map(_._3).min()
    val maxY = coords.map(_._4).max() + 1 // +1 to also include points that lie on the maxY value
    
//    println(s"$minX $minY  -- $maxX $maxY")
    
    (minX, maxX, minY, maxY)
  }
  
  
  val numXCells = Math.ceil((maxX - minX) / sideLength).toInt
  val numYCells = Math.ceil((maxY - minY) / sideLength).toInt

  
  /**
   * Compute the bounds of a cell with the given ID
   * @param id The ID of the cell to compute the bounds for
   */
  protected[spatial] def getCellBounds(id: Int): NRectRange = {
//    require(id >= 0 && id < numPartitions, s"Invalid cell id (0 .. $numPartitions): $id")
      
    val dy = id / numYCells
    val dx = id % numXCells
    
    val llx = dx * sideLength + minX
    val lly = dy * sideLength + minY
    
    val urx = llx + sideLength
    val ury = lly + sideLength
      
    NRectRange(id, NPoint(llx, lly), NPoint(urx, ury))
  }
  
  /**
   * The cells which contain elements and the number of elements
   * 
   * We iterate over all elements in the RDD, determine to which
   * cell it belongs and then simple aggregate by cell
   */
  protected[spatial] val cells = rdd.map { case (g,v) =>  
      val p = g.getCentroid
      
      val newX = p.getX - minX
      val newY = p.getY - minY
    
      val x = (newX.toInt / sideLength).toInt
      val y = (newY.toInt / sideLength).toInt
      
      val cellId = y * numXCells + x
      
      (getCellBounds(cellId),1)
    }.reduceByKey(_+_).collect()

  
  protected[spatial] val bsp = new BSP(Array(minX, minY), Array(maxX, maxY), cells, sideLength, maxCostPerPartition)  
    
  override def numPartitions: Int = bsp.partitions.size
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]
    /* XXX: This will throw an error if the geometry is outside of our initial data space
     * However, this should not happen, because the partitioner is specially for a given RDD
     * which by definition is immutable. 
     */
    val part = bsp.partitions.filter{ p =>
      val c = g.getCentroid
      p.contains(NPoint(c.getX, c.getY)) 
    }.head
    
    part.id
    
  }
}
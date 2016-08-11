package dbis.stark.spatial

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import etm.core.monitor.EtmMonitor
import dbis.spatial.{NPoint,NRectRange}
import dbis.spatial.partitioner.BSP
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption

import scala.collection.JavaConverters._
import dbis.stark.SpatialObject

/**
 * A cost based binary space partitioner based on the paper
 * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
 * by He, Tan, Luo, Feng, Fan 
 * 
 * @param rdd The RDD to partition
 * @param sideLength side length of a quadratic cell - defines granularity
 * @param maxCostPerPartition Maximum cost a partition should have - here: number of elements  
 */
class BSPartitioner[G <: SpatialObject : ClassTag, V: ClassTag](
    @transient private val rdd: RDD[(G,V)],
    sideLength: Double,
    maxCostPerPartition: Double = 1.0) extends SpatialPartitioner(rdd) {
  
  
  protected[spatial] val numXCells = Math.ceil((maxX - minX) / sideLength).toInt
  
  /**
   * Compute the bounds of a cell with the given ID
   * @param id The ID of the cell to compute the bounds for
   */
  protected[spatial] def getCellBounds(id: Int): NRectRange = {
      
    val dy = id / numXCells
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
   * cell it belongs and then simply aggregate by cell
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

  
  protected[spatial] val bsp = new BSP(
      NPoint(minX, minY), 
      NPoint(maxX, maxY), 
      cells, 
      sideLength, 
      maxCostPerPartition)  
    
  override def partitionBounds(idx: Int): NRectRange = bsp.partitions(idx)  
  
//  def printPartitions(fName: String) {
//    val list = bsp.partitions.map { p => s"${p.ll(0)},${p.ll(1)},${p.ur(0)},${p.ur(1)}" }.asJava    
//    Files.write(new File(fName).toPath(), list, StandardOpenOption.CREATE, StandardOpenOption.WRITE) 
//      
//  } 
//    
//  def printHistogram(fName: String) {
//    
//    println(s"num in hist: ${cells.map(_._2).sum}")
//    
//    
//    val list = cells.map { case (c,i) => s"${c.ll(0)},${c.ll(1)},${c.ur(0)},${c.ur(1)}" }.toList.asJava    
//    Files.write(new File(fName).toPath(), list, StandardOpenOption.CREATE, StandardOpenOption.WRITE) 
//      
//  }   
    
  override def numPartitions: Int = bsp.partitions.size
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]
//    println(s"get partition for key $g")

    /* XXX: This will throw an error if the geometry is outside of our initial data space
     * However, this should not happen, because the partitioner is specially for a given RDD
     * which by definition is immutable. 
     */
    val part = bsp.partitions.filter{ p =>
      val c = g.getCentroid
      p.contains(NPoint(c.getX, c.getY)) 
    }.headOption
    
    part match {
      case None => 
        println(bsp.partitions.mkString("\n"))
        println(bsp.partitionStats)
        throw new IllegalStateException("didum")
      case Some(part) =>  
        part.id
      
    }
    
    
  }
}
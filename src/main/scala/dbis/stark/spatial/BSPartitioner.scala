package dbis.stark.spatial

import scala.reflect.ClassTag
import scala.collection.mutable.Map

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD

import dbis.spatial.{NPoint,NRectRange}
import dbis.spatial.partitioner.BSP
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
    _sideLength: Double,
    _maxCostPerPartition: Double = 1.0) extends SpatialPartitioner(rdd) {
  
  lazy val maxCostPerPartition = _maxCostPerPartition
  lazy val sideLength = _sideLength
  
  protected[spatial] val numXCells = Math.ceil((maxX - minX) / _sideLength).toInt
  protected[spatial] val numYCells = Math.ceil((maxY - minY) / _sideLength).toInt
  
  /**
   * Compute the bounds of a cell with the given ID
   * @param id The ID of the cell to compute the bounds for
   */
  protected[spatial] def getCellBounds(id: Int): NRectRange = {
      
    val dy = id / numXCells
    val dx = id % numXCells
    
    val llx = dx * _sideLength + minX
    val lly = dy * _sideLength + minY
    
    val urx = llx + _sideLength
    val ury = lly + _sideLength
      
    NRectRange(id, NPoint(llx, lly), NPoint(urx, ury))
  }
  
  /**
   * The cells which contain elements and the number of elements
   * 
   * We iterate over all elements in the RDD, determine to which
   * cell it belongs and then simply aggregate by cell
   */
  protected[spatial] val cells: Array[(NRectRange, Int)] = {
    
    val themap = Map.empty[NRectRange, Int]
    (0 until numXCells * numYCells).map { i => 
      val cell = getCellBounds(i)
      
      themap += (cell -> 0)
    }
    
    rdd.map { case (g,v) =>
      val p = g.getCentroid
      
      val newX = p.getX - minX
      val newY = p.getY - minY
    
      val x = (newX.toInt / _sideLength).toInt
      val y = (newY.toInt / _sideLength).toInt
      
      val cellId = y * numXCells + x
      
      (cellId,1)
    }
    .reduceByKey(_ + _)
    .collect
    .map { case (id, cnt) => (getCellBounds(id), cnt) }
    .foreach { case (cell, cnt) => themap(cell) += cnt }
    
    themap.toArray
  }

  
  protected[spatial] val bsp = new BSP(
      NPoint(minX, minY), 
      NPoint(maxX, maxY), 
      cells, 
      _sideLength, 
      _maxCostPerPartition)  
    
  override def partitionBounds(idx: Int): NRectRange = bsp.partitions(idx)  
  
  def printPartitions(fName: String) {
    val list = bsp.partitions.map { p => s"${p.ll(0)},${p.ll(1)},${p.ur(0)},${p.ur(1)}" }.asJava    
    java.nio.file.Files.write(new java.io.File(fName).toPath(), list, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE) 
      
  } 
    
  def printHistogram(fName: String) {
    
    println(s"num in hist: ${cells.map(_._2).sum}")
    
    
    val list = cells.map { case (c,i) => s"${c.ll(0)},${c.ll(1)},${c.ur(0)},${c.ur(1)}" }.toList.asJava    
    java.nio.file.Files.write(new java.io.File(fName).toPath(), list, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE) 
      
  } 
  
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
        throw new IllegalStateException(s"$g is not in any partition!")
      case Some(part) =>  
        part.id
      
    }
    
    
  }
}
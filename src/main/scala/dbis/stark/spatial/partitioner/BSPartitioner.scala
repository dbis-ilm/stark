package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import dbis.stark.spatial._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


object BSPartitioner {
  
  def withGridPPD[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
    _gridPPD: Double,
    _maxCostPerPartition: Double,
    withExtent: Boolean,
    _minX: Double,
    _maxX: Double,
    _minY: Double,
    _maxY: Double): BSPartitioner[G, V] = {
    
    val _sideLength = math.min(math.abs(_maxX - _minX), math.abs(_maxY - _minY)) / _gridPPD
    
    new BSPartitioner(rdd, _sideLength, _maxCostPerPartition, withExtent, _minX, _maxX, _minY, _maxY)
    
  }
  
  var numCellThreshold: Int = -1
}

/**
  * * A cost based binary space partitioner based on the paper
  * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
  * by He, Tan, Luo, Feng, Fan
  *
  * @param rdd Th RDD
  * @param sideLength Length of a cell
  * @param maxCostPerPartition Maximum allowed cost/number of elements per parititon
  * @param withExtent Regard the element's extent
  * @param _minX Minimum x value
  * @param _maxX Maximum x value
  * @param _minY Minimum y value
  * @param _maxY Maximum y value
  * @tparam G Geometry type
  * @tparam V Payload data type
  */
class BSPartitioner[G <: STObject : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)],
    sideLength: Double,
    maxCostPerPartition: Double,
    withExtent: Boolean,
    _minX: Double,
    _maxX: Double,
    _minY: Double,
    _maxY: Double) extends SpatialPartitioner(_minX, _maxX, _minY, _maxY) {

  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           withExtent: Boolean,
           minMax: (Double, Double, Double, Double)) =
    this(rdd, sideLength, maxCostPerPartition, withExtent, minMax._1, minMax._2, minMax._3, minMax._4)
  
  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           withExtent: Boolean = false) =
    this(rdd, sideLength, maxCostPerPartition, withExtent, SpatialPartitioner.getMinMax(rdd))

  
//  lazy val maxCostPerPartition: Double = _maxCostPerPartition

  protected[spatial] var numXCells: Int = Math.ceil(math.abs(maxX - minX) / sideLength).toInt
  protected[spatial] var numYCells: Int = Math.ceil(math.abs(maxY - minY) / sideLength).toInt
  
  /**
   * The cells which contain elements and the number of elements
   * 
   * We iterate over all elements in the RDD, determine to which
   * cell it belongs and then simply aggregate by cell
   */
  protected[spatial] val cells: Array[(Cell, Int)] =
    SpatialPartitioner.buildHistogram(rdd,withExtent,numXCells,numYCells,minX,minY,maxX,maxY,sideLength,sideLength)

  protected[spatial] var bsp = new BSP(
      Array(minX, minY), 
      Array(maxX, maxY), 
      cells, // for BSP we only need calculated cell sizes and their respective counts 
      sideLength,
      maxCostPerPartition,
      withExtent,
      BSPartitioner.numCellThreshold)
  
//  protected[spatial] var bsp = new BSPBinary(
//      Array(minX, minY), 
//      Array(maxX, maxY), 
//      20,
//      cells, // for BSP we only need calculated cell sizes and their respective counts 
//      _sideLength, 
//      _maxCostPerPartition,
//      withExtent,
//      BSPartitioner.numCellThreshold)
    
  override def partitionBounds(idx: Int): Cell = bsp.partitions(idx)
  override def partitionExtent(idx: Int): NRectRange = bsp.partitions(idx).extent
  
  def printPartitions(fName: java.nio.file.Path) {
    val list = bsp.partitions.map(_.range).map { p => s"${p.ll(1)},${p.ll(0)},${p.ur(1)},${p.ur(0)}" }.toList.asJava
    java.nio.file.Files.write(fName, list, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING) 
    
    val list2 = bsp.partitions.map{ cell => s"${cell.id};${cell.range.wkt}"}.toList.asJava
    java.nio.file.Files.write(fName.getParent.resolve("partitions_wkt.csv"), list2, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
    
//    val list3 = bsp.partitions.map{ cell => s"${cell.range.id};${cell.extent.getWKTString()}"}.toList.asJava
//    java.nio.file.Files.write(fName.getParent.resolve("partitions_wkt_extent.csv"), list3, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
  } 
    
  def printHistogram(fName: java.nio.file.Path) {
    
//    println(s"num in hist: ${cells.map(_._2).sum}")
    val list = cells.map(_._1.range).map { c => s"${c.ll(1)},${c.ll(0)},${c.ur(1)},${c.ur(0)}" }.toList.asJava
    java.nio.file.Files.write(fName, list, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
    
    val list2 = cells.map{ case (cell,cnt) => s"${cell.id};${cell.range.wkt};$cnt"}.toList.asJava
    java.nio.file.Files.write(fName.getParent.resolve("histogram_wkt.csv"), list2, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
    
//    val list3 = cells.map{ case (cell,cnt) => s"${cell.range.id};${cell.extent.getWKTString()}"}.toList.asJava
//    java.nio.file.Files.write(fName.getParent.resolve("histo_wkt_extent.csv"), list3, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
      
  } 
  
  override def numPartitions: Int = bsp.partitions.length
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]

    /* XXX: This will throw an error if the geometry is outside of our partitions!
     * However, this should not happen, because the partitioner is specially for a given RDD
     * which by definition is immutable and the partitions should cover the complete data space 
     * of the RDD's content
     */
    val c = Utils.getCenter(g.getGeo)
    val part = bsp.partitions.find{ p =>
      p.range.contains(NPoint(c.getX, c.getY))
    }
    
    
    if(part.isDefined) {
      // return the partition ID
      part.get.id 
      
    } else {
//      println("error: no partition found")
//      println(bsp.partitions.mkString("\n"))
      val histoFile = java.nio.file.Paths.get(System.getProperty("user.home"), "stark_histogram")
      val partitionFile = java.nio.file.Paths.get(System.getProperty("user.home"), "stark_partitions")
      
//      println(s"saving historgram to $histoFile")
      printHistogram(histoFile)
      
//      println(s"saving partitions to $partitionFile")
      printPartitions(partitionFile)
      
//      println("you can use the script/plotpoints.py script to visualize points, cells, and partitions")
      
      throw new IllegalStateException(s"$g is not in any partition!")
        
    }
    
    
  }
}

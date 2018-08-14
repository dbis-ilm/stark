package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import dbis.stark.spatial._
import org.apache.spark.rdd.RDD

//import scala.collection.JavaConverters._
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
  * @param pointsOnly Regard the element's extent
  * @param _minX Minimum x value
  * @param _maxX Maximum x value
  * @param _minY Minimum y value
  * @param _maxY Maximum y value
  * @tparam G Geometry type
  * @tparam V Payload data type
  */
class BSPartitioner[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G,V)],
                                                           val sideLength: Double,
                                                           maxCostPerPartition: Double,
                                                           pointsOnly: Boolean,
                                                           _minX: Double,
                                                           _maxX: Double,
                                                           _minY: Double,
                                                           _maxY: Double) extends SpatialPartitioner(_minX, _maxX, _minY, _maxY) {

  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean,
           minMax: (Double, Double, Double, Double)) =
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, minMax._1, minMax._2, minMax._3, minMax._4)

  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean) =
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, SpatialPartitioner.getMinMax(rdd))


//  val s = Cell(0, NRectRange(NPoint(ll), NPoint(ur)))
  //
  //    val cellsPerDim = cellsPerDimension(s.range)
  //
  //    val newUr = ur.zipWithIndex.map { case (_, dim) =>
  //      if(ll(dim) + cellsPerDim(dim) * sideLength > ur(dim))
  //        ll(dim) + cellsPerDim(dim) * sideLength
  //      else
  //        ur(dim)
  //    }
  //
  //    ur = newUr

  protected[spatial] val numXCells: Int = {
    val xCells = Math.ceil(math.abs(maxX - minX) / sideLength).toInt
    maxX = minX + xCells*sideLength
    Math.ceil(math.abs(maxX - minX) / sideLength).toInt
  }

  protected[spatial] val numYCells: Int = {
    val yCells = Math.ceil(math.abs(maxY - minY) / sideLength).toInt
    maxY = minY + yCells*sideLength
    Math.ceil(math.abs(maxY - minY) / sideLength).toInt
  }

  /**
    * The cells which contain elements and the number of elements
    *
    * We iterate over all elements in the RDD, determine to which
    * cell it belongs and then simply aggregate by cell
    */
  protected[spatial] val cells: Array[(Cell, Int)] =
    SpatialPartitioner.buildHistogram(rdd,pointsOnly,numXCells,numYCells,minX,minY,maxX,maxY,sideLength,sideLength)

  protected[spatial] val start = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

  protected[spatial] var bsp = new BSP(
    start,
    cells, // for BSP we only need calculated cell sizes and their respective counts
    sideLength,
    maxCostPerPartition,
    pointsOnly,
    BSPartitioner.numCellThreshold
    )


//  printPartitions(Paths.get(System.getProperty("user.home"), "partis.wkt"))
//  printHistogram(Paths.get(System.getProperty("user.home"), "histo.wkt"))


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
  
  override def printPartitions(fName: java.nio.file.Path) {
    val list2 = bsp.partitions.map { cell => s"${cell.id};${cell.range.wkt}" }.toList
    super.writeToFile(list2, fName)
  }

  def printHistogram(fName: java.nio.file.Path) {

    val list2 = cells.map{ case (cell,cnt) => s"${cell.id};${cell.range.wkt};$cnt"}.toList
    super.writeToFile(list2, fName)
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
      val histoFile = java.nio.file.Paths.get(System.getProperty("user.home"), "stark_histogram.wkt")
      val partitionFile = java.nio.file.Paths.get(System.getProperty("user.home"), "stark_partitions.wkt")
      
//      println(s"saving historgram to $histoFile")
      printHistogram(histoFile)
      
//      println(s"saving partitions to $partitionFile")
      printPartitions(partitionFile)
      
//      println("you can use the script/plotpoints.py script to visualize points, cells, and partitions")
      
      throw new IllegalStateException(s"$g is not in any partition!")
        
    }
    
    
  }
}

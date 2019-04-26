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
    _maxY: Double,
    _sampleFraction: Int = 0): BSPartitioner[G, V] = {
    
    val _sideLength = math.min(math.abs(_maxX - _minX), math.abs(_maxY - _minY)) / _gridPPD
    
    new BSPartitioner(rdd, _sideLength, _maxCostPerPartition, withExtent, _minX, _maxX, _minY, _maxY, _sampleFraction)
    
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
class BSPartitioner[G <: STObject : ClassTag, V: ClassTag](
      @transient private val rdd: RDD[(G,V)],
      val sideLength: Double,
      val maxCostPerPartition: Double,
      val pointsOnly: Boolean,
      private val _minX: Double,
      private val _maxX: Double,
      private val _minY: Double,
      private val _maxY: Double,
      val sampleFraction: Double) extends GridPartitioner(_minX, _maxX, _minY, _maxY) {

  protected[partitioner] val theRDD = if(sampleFraction > 0) rdd.sample(withReplacement = false, fraction = sampleFraction) else rdd


  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean,
           minMax: (Double, Double, Double, Double),
           sampleFraction: Double) =
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, minMax._1, minMax._2, minMax._3, minMax._4, sampleFraction)

  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean,
           sampleFraction: Double = 0) =
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, GridPartitioner.getMinMax(rdd), sampleFraction)


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
    GridPartitioner.buildHistogram(theRDD,pointsOnly,numXCells,numYCells,minX,minY,maxX,maxY,sideLength,sideLength)

  protected[spatial] val start = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

  protected[spatial] val bsp = new BSP(
    start,
    cells, // for BSP we only need calculated cell sizes and their respective counts
    sideLength,
    maxCostPerPartition,
    pointsOnly,
    BSPartitioner.numCellThreshold)


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
  
  override def getPartitionId(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val c = Utils.getCenter(g.getGeo)

    val pX = c.getX
    val pY = c.getY
    val pc = NPoint(pX, pY)


    // find the partitionId where the point is contained in
    val part = bsp.partitions.find(_.range.contains(pc))

    /*
     * If the given point was not within any partition, calculate the distances to all other partitions
     * and assign the point to the closest partition.
     * Eventually, adjust the assigned partition range and extent
     */
    val (partitionId, outside) = part.map(p => (p.id,false)).getOrElse {
//      val iter = if(partitions.length > 100) partitions.par.iterator else partitions.iterator
//      val minPartitionId12 = iter.map{ case Cell(id, range, _) => (id, range.dist(pc)) }.minBy(_._2)._1

      var minDist = Double.MaxValue
      var minPartitionId: Int = -1
      var first = true

      for(partition <- bsp.partitions) {
        val dist = partition.range.dist(pc)
        if(first || dist < minDist) {
          minDist = dist
          minPartitionId = partition.id
          first = false
        }

      }

      (minPartitionId, true)
    }

    if(outside || (!pointsOnly && sampleFraction > 0)) {
      bsp.partitions(partitionId).extendBy(Utils.fromGeo(g.getGeo))
    }


    partitionId
  }

  override def equals(obj: scala.Any) = obj match {
    case sp: BSPartitioner[G,_] =>
      sp.rdd == rdd &&
      sp.sideLength == sideLength &&
      sp.maxCostPerPartition == maxCostPerPartition &&
      sp.pointsOnly == pointsOnly &&
      sp.sampleFraction == sampleFraction &&
      sp.minX == minX && sp.maxX == maxX &&
      sp.minY == minY && sp.maxY == maxY
    case _ => false
  }
}

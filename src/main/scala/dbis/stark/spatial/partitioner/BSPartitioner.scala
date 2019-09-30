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
    _sampleFraction: Int = 0, parallel: Boolean = false): BSPartitioner[G, V] = {
    
    val _sideLength = math.min(math.abs(_maxX - _minX), math.abs(_maxY - _minY)) / _gridPPD
    
    new BSPartitioner(rdd, _sideLength, _maxCostPerPartition, withExtent, _minX, _maxX, _minY, _maxY, _sampleFraction, parallel)
    
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
class BSPartitioner[G <: STObject : ClassTag, V: ClassTag]private[partitioner](
      @transient private val rdd: RDD[(G,V)],
      val sideLength: Double,
      val maxCostPerPartition: Double,
      val pointsOnly: Boolean,
      private val _minX: Double,
      private val _maxX: Double,
      private val _minY: Double,
      private val _maxY: Double,
      val sampleFraction: Double, private val parallel:Boolean) extends GridPartitioner(_minX, _maxX, _minY, _maxY) {

  protected[partitioner] val theRDD = if(sampleFraction > 0) rdd.sample(withReplacement = false, fraction = sampleFraction) else rdd


  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean,
           minMax: (Double, Double, Double, Double),
           sampleFraction: Double,
           parallel: Boolean) =
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, minMax._1, minMax._2, minMax._3, minMax._4, sampleFraction, parallel)

  def this(rdd: RDD[(G,V)],
           sideLength: Double,
           maxCostPerPartition: Double,
           pointsOnly: Boolean,
           sampleFraction: Double = 0,
           parallel: Boolean = true) =
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, GridPartitioner.getMinMax(rdd), sampleFraction,parallel)

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
  protected[spatial] val histogram: CellHistogram =
    GridPartitioner.buildHistogram(theRDD,pointsOnly,numXCells,numYCells,minX,minY,maxX,maxY,sideLength,sideLength)

  protected[spatial] val start = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

  protected[spatial] val bsp = if(parallel)
    // new BSPBinaryAsync(start,  histogram, sideLength, numXCells, maxCostPerPartition, pointsOnly, BSPartitioner.numCellThreshold)
    new BSP2(start,  histogram, sideLength, numXCells, maxCostPerPartition, pointsOnly, BSPartitioner.numCellThreshold)
  else
    new BSP(start,  histogram, sideLength, numXCells, maxCostPerPartition, pointsOnly, BSPartitioner.numCellThreshold)

  override def partitionBounds(idx: Int): Cell = bsp.partitions(idx)
  override def partitionExtent(idx: Int): NRectRange = bsp.partitions(idx).extent


  override def printPartitions(fName: java.nio.file.Path) {
    val list2 = bsp.partitions.map { cell => s"${cell.id};${cell.range.wkt};${cell.extent.wkt}" }.toList
    GridPartitioner.writeToFile(list2, fName)
  }

  def printHistogram(fName: java.nio.file.Path) {

    val list2 = histogram.buckets.values.map{ case (cell,cnt) => s"${cell.id};${cell.range.wkt};$cnt"}.toList
    GridPartitioner.writeToFile(list2, fName)
  } 
  
  override def numPartitions: Int = bsp.partitions.length
  
  override def getPartitionId(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val c = StarkUtils.getCenter(g.getGeo)

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

//      println(s"num partitions: ${bsp.partitions.length}")

      for(partition <- bsp.partitions) {
        val dist = partition.range.dist(pc)
        if(first || dist < minDist) {
          minDist = dist
//          println(s"setting min partition id from $minPartitionId to ${partition.id}")
          minPartitionId = partition.id
          first = false
        }

      }

      (minPartitionId, true)
    }

//    if(outside || (!pointsOnly && sampleFraction > 0)) {
//      bsp.partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
//    }

    if(outside) {
      bsp.partitions(partitionId).range = bsp.partitions(partitionId).range.extend(pc)

      //      if(!pointsOnly)
      bsp.partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
    } else if(!pointsOnly) {
      bsp.partitions(partitionId).extendBy(StarkUtils.fromGeo(g.getGeo))
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

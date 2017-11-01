package dbis.stark.spatial.partitioner

import dbis.stark.STObject
import dbis.stark.spatial.{Cell, _}
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
    _maxY: Double,
    sampleFraction: Double = 0): BSPartitioner[G, V] = {
    
    val _sideLength = math.min(math.abs(_maxX - _minX), math.abs(_maxY - _minY)) / _gridPPD
    
    new BSPartitioner(rdd, _sideLength, _maxCostPerPartition, withExtent, _minX, _maxX, _minY, _maxY, sampleFraction)
    
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
      rdd: RDD[(G,V)],
      val sideLength: Double,
      maxCostPerPartition: Double,
      pointsOnly: Boolean,
      _minX: Double,
      _maxX: Double,
      _minY: Double,
      _maxY: Double,
      sampleFraction: Double) extends SpatialPartitioner(_minX, _maxX, _minY, _maxY) {

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
    this(rdd, sideLength, maxCostPerPartition, pointsOnly, SpatialPartitioner.getMinMax(rdd), sampleFraction)


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
    SpatialPartitioner.buildHistogram(theRDD,pointsOnly,numXCells,numYCells,minX,minY,maxX,maxY,sideLength,sideLength)

  protected[spatial] val start = NRectRange(NPoint(minX, minY), NPoint(maxX, maxY))

  protected[spatial] val bsp = new BSP(
    start,
    cells, // for BSP we only need calculated cell sizes and their respective counts
    sideLength,
    maxCostPerPartition,
    pointsOnly,
    BSPartitioner.numCellThreshold
    )

  private val partitions = bsp.partitions

  override def partitionBounds(idx: Int): Cell = partitions(idx)
  override def partitionExtent(idx: Int): NRectRange = partitions(idx).extent
  
  def printPartitions(fName: java.nio.file.Path) {
    val list2 = bsp.partitions.map { cell => s"${cell.id};${cell.range.wkt}" }.toList.asJava
    java.nio.file.Files.write(fName, list2, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)
  }

  def printHistogram(fName: java.nio.file.Path) {

    val list2 = cells.map{ case (cell,cnt) => s"${cell.id};${cell.range.wkt};$cnt"}.toList.asJava
    java.nio.file.Files.write(fName, list2, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)

  }

  override def numPartitions: Int = partitions.length
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val c = Utils.getCenter(g.getGeo)

    val pX = c.getX
    val pY = c.getY
    val pc = NPoint(pX, pY)


    // find the partitionId where the point is contained in
    val part = partitions.find(_.range.contains(pc))

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

      for(partition <- partitions) {
//      partitions.foreach { partition =>
        val dist = partition.range.dist(pc)
        if(first || dist < minDist) {
          minDist = dist
          minPartitionId = partition.id
          first = false
        }

      }

      (minPartitionId, true)
    }

    if(outside) {
      partitions(partitionId).range = partitions(partitionId).range.extend(pc, SpatialPartitioner.EPS)

      if(!pointsOnly)
        partitions(partitionId).extendBy(Utils.fromEnvelope(g.getGeo))
    }

    partitionId
  }
}

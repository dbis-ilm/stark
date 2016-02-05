package dbis.spark.spatial

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import dbis.spark.spatial.SpatialGridPartitioner.RectRange
import SpatialGridPartitioner.Point
import etm.core.monitor.EtmMonitor

/**
 * A data class to store information about the created partitioning
 */
case class PartitionStats(numPartitions: Int, 
    avgPoints: Double,    
    maxPoints: List[(RectRange, Int)],
    minPoints: List[(RectRange, Int)],
    numPointsVariance: Double,
    area: Double,
    avgArea: Double,
    maxArea: List[(RectRange, Double)],
    minArea: List[(RectRange, Double)]
  )

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
  protected[spatial] val (minX, maxX, minY, maxY) = {
    
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
 
  // number of cells in the complete data space 
  val numXCells = Math.ceil((maxX - minX) / sideLength).toInt
  val numYCells = Math.ceil((maxY - minY) / sideLength).toInt
  
  /**
   * Compute the bounds of a cell with the given ID
   * @param id The ID of the cell to compute the bounds for
   */
  protected[spatial] def getCellBounds(id: Int): RectRange = {
//    require(id >= 0 && id < numPartitions, s"Invalid cell id (0 .. $numPartitions): $id")
      
    val dy = id / numYCells
    val dx = id % numXCells
    
    val llx = dx * sideLength + minX
    val lly = dy * sideLength + minY
    
    val urx = llx + sideLength
    val ury = lly + sideLength
      
    RectRange(id, Point(llx, lly), Point(urx, ury))
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
    }.reduceByKey( _ + _).collect()

    
  /**
   * Compute the cost for a partition, i.e. sum the cost
   * for each cell in that partition.
   * 
   * @param part The partition 
   */
  protected[spatial] def costEstimation(part: RectRange): Double =
    cells.filter { case (cell, cnt) => part.contains(cell) }.map(_._2).sum
      
  /**
   * Get the length in both dimensions of a partition
   * 
   * @param part The partition to compute the lengths for  
   */
  // TODO: should be a method of RectRange, shouldn't it?
  protected[spatial] def lengths(part: RectRange): (Double, Double) = (part.ur.x - part.ll.x , part.ur.y - part.ll.y)  
  
  /**
   * Split the given partition into two partitions so that 
   * <br/>
   * part = part1 u part2
   * <br/><br/>
   * A split is done along the cell borders of each dimension. The generate candidate split lines,
   * which is each cell border in each dimension and compute the cost for this candidate split.
   * The first split that creates a minimal cost difference between the two created partitions 
   * is chosen
   * 
   * @param part The partition to split
   * 
   */
  protected[spatial] def costBasedSplit(part: RectRange): (RectRange, RectRange) = {
    var minCostDiff = Double.PositiveInfinity
    
    // result variable with default value - will be overriden in any case
    var parts: (RectRange, RectRange) = (RectRange(-1, Point(0,0),Point(0,0)), RectRange(-1,Point(0,0),Point(0,0))) 
    
    // get the cells for the partition
    val (xLength, yLength) = lengths(part)
    
    val xCells = Math.ceil(xLength / sideLength).toInt
    val yCells = Math.ceil(yLength / sideLength).toInt
    
    /* start with the X dimension:
     * For each cell border at x dimension, calculate the split partitions
     * and compute their costs.
     */
    // x splits
    for(i <- (1 until xCells)) {
      // generated partitions 
      val p1 = RectRange(-1, 
                  part.ll,
                  Point( part.ll.x + i * sideLength, part.ur.y)
                )
      
      val p2 = RectRange(-1,
                  Point( part.ll.x + i * sideLength, part.ll.y),
                  part.ur
                )
      // get the costs          
      val p1Cost = costEstimation(p1)
      val p2Cost = costEstimation(p2)
      
      // if cost difference is (currently) minimal, store this partitioning 
      val diff = Math.abs( p1Cost - p2Cost )
      if(diff < minCostDiff) {
        minCostDiff = diff
        parts = (p1,p2)
      }
    }
  
    // same as above
    // y splits
    for(i <- (1 until yCells)) {
      val p1 = RectRange(-1, 
                  part.ll,
                  Point( part.ur.x, part.ll.y + i * sideLength)
                )
      
      val p2 = RectRange(-1,
                  Point( part.ll.x, part.ll.y + i * sideLength),
                  part.ur
                )
                
      val p1Cost = costEstimation(p1)
      val p2Cost = costEstimation(p2)
      
      val diff = Math.abs( p1Cost - p2Cost )
      if(diff < minCostDiff) {
        minCostDiff = diff
        parts = (p1,p2)
      }
    }
    
    /* at this point we have checked all candidate partitionings in each dimension
     * and have stored the one that creates a minimal cost difference between both
     * partitions - return this
     */
    
    parts
  }
  
  /**
   * Compute the partitioning using the cost based BSP algorithm
   * 
   * This is a lazy value    
   */
  protected[spatial] lazy val bounds = {
    import SpatialGridPartitioner._
    
    // start with a partition covering the complete data space
    val queue = Queue(RectRange(0, Point(minX, minY), Point(maxX, maxY)))

    val resultPartitions = ListBuffer.empty[RectRange]
    
    
    while(queue.nonEmpty) {
      val part = queue.dequeue()
    
      /* if the partition to process is more expensive (= has more elements) than max cost
       * AND it is still larger than one cell, split it
       * Otherwise we use it as a result partition
       * 
       * It may happen that a cell (which is our finest granularity) contains more elements
       * than max cost allows, however, since we cannot split a cell, we have to live with this
       */
      val (lx, ly) = lengths(part)
      if((costEstimation(part) > maxCostPerPartition) && (lx > sideLength || ly > sideLength) ) {
        val (p1, p2) = costBasedSplit(part)
        queue.enqueue(p1, p2)
      } else
        resultPartitions += part
    }
    
    resultPartitions.toList.zipWithIndex
  }
  
  /** 
   * Collect statistics about the generated partitioning
   * <br><br>
   * This is a lazy value so it is not computed until it is needed.
   * However, if the partitioning was not created before this value 
   * is accessed, it will trigger the partition computation 
   */
  lazy val partitionStats = {
    
    // this will trigger the computation, in case it was not done before
    val numParts = bounds.size
    
    val partCounts = cells.view.flatMap { case (cell, count) =>
      bounds.view.map(_._1).filter { p => p.contains(cell) }.map { p => (p, count) }        
    }.groupBy(_._1).map { case (part, arr) => (part, arr.map(_._2).sum) }.toList    
    
    // _2 is the count for each partition
    val avgPoints = partCounts.view.map(_._2).sum.toDouble / partCounts.size
    val maxPts = partCounts.view.map(_._2).max
    val minPts = partCounts.view.map(_._2).min
    
    val maxPoints = partCounts.filter(_._2 == maxPts)
    val minPoints = partCounts.filter(_._2 == minPts)
    
    val variance = partCounts.map { case (part, count) => Math.pow( count - avgPoints, 2) }.sum
    
    
    
    val area = bounds.view.map(_._1.area).sum
    val avgArea = area / numParts
    val partAreas = partCounts.map { case (part,_) => (part, part.area) }
    // _2 is the area of a partition
    val maxA = partAreas.view.map(_._2).max
    val minA = partAreas.view.map(_._2).min
    
    val maxArea = partAreas.filter(_._2 == maxA)
    val minArea = partAreas.filter(_._2 == minA)
    
    val areaVariance = partAreas.map{ case (part, area) => Math.pow( area - avgArea, 2) }.sum
    
    PartitionStats(numParts, avgPoints, maxPoints, minPoints, variance, area, avgArea, maxArea, minArea) 
  }
  
  override def numPartitions: Int = bounds.size
  
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]
    /* XXX: This will throw an error if the geometry is outside of our inital data space
     * However, this should not happen, because the partitioner is specially for a given RDD
     * which by definition is immutable. 
     */
    val part = bounds.filter{ case (r,idx) =>
      val c = g.getCentroid
      r.contains(SpatialGridPartitioner.Point(c.getX, c.getY)) 
    }.head
    
    part._2
    
  }
}
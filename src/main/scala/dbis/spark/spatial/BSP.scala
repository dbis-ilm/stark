package dbis.spark.spatial

import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer

/**
 * A data class to store information about the created partitioning
 */
case class PartitionStats(numPartitions: Int, 
    avgPoints: Double,    
    maxPoints: List[(NRectRange, Int)],
    minPoints: List[(NRectRange, Int)],
    numPointsVariance: Double,
    area: Double,
    avgArea: Double,
    maxArea: List[(NRectRange, Double)],
    minArea: List[(NRectRange, Double)]
  )
  
  
class BSP(ll: Array[Double], ur: Array[Double],
    cellHistogram: Array[(NRectRange, Int)],
    sideLength: Double,
    maxCostPerPartition: Double
  ) extends Serializable {
  
  require(ll.size == ur.size, "Equal number of dimension required")
  
  /**
   * Compute the cost for a partition, i.e. sum the cost
   * for each cell in that partition.
   * 
   * @param part The partition 
   */
  protected[spatial] def costEstimation(part: NRectRange): Double =
    cellHistogram.filter { case (cell, cnt) => part.contains(cell) }.map(_._2).sum
    
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
  protected[spatial] def costBasedSplit(part: NRectRange): (NRectRange, NRectRange) = {
    var minCostDiff = Double.PositiveInfinity
    
    // result variable with default value - will be overriden in any case
    var parts: (NRectRange, NRectRange) = (NRectRange(-1, NPoint(0,0),NPoint(0,0)), NRectRange(-1,NPoint(0,0),NPoint(0,0))) 
    
    // count how many cells we have in each dimension
    val cellsPerDimension = (0 until ll.size).map { dim => 
      math.ceil(part.lengths(dim) / sideLength).toInt  
    }.toArray
    
    /* process only those dimensions, were there is more than on cell, 
     * i.e. we could split, acutally
     * 
     *     
     */
    cellsPerDimension.zipWithIndex            // index is the dimension
                      .filter(_._1 > 1)       // filter for number of cells
                      .foreach { case (numCells, dim) =>

      // calculate candidate partitions it we split at each possible cell
      for(i <- (1 until numCells)) {
        
        // TODO: better documentation for this calculation formulas
        val p1 = {
          
          /* we need to copy the array, otherwise we have wrong values
           * in calculation for p2
           */
          val ur = part.ur.c.clone()
          ur(dim) = part.ll(dim) + i*sideLength
          
          NRectRange(part.ll.clone(), NPoint(ur))
        }
        
        val p2 = {
          val ll = part.ll.c.clone()
          ll(dim) += i*sideLength 
         
          NRectRange(NPoint(ll), part.ur.clone())
          
        }
        
        // calculate costs in each candidate partition
        val p1Cost = costEstimation(p1)
        val p2Cost = costEstimation(p2)
      
        // if cost difference is (currently) minimal, store this partitioning 
        val diff = Math.abs( p1Cost - p2Cost )
        if(diff < minCostDiff) {
          minCostDiff = diff
          parts = (p1,p2)
        }
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
    
    // start with a partition covering the complete data space
    val queue = Queue(NRectRange(0, NPoint(ll), NPoint(ur)))

    val resultPartitions = ListBuffer.empty[NRectRange]
    
    
    while(queue.nonEmpty) {
      val part = queue.dequeue()
    
      /* if the partition to process is more expensive (= has more elements) than max cost
       * AND it is still larger than one cell, split it
       * Otherwise we use it as a result partition
       * 
       * It may happen that a cell (which is our finest granularity) contains more elements
       * than max cost allows, however, since we cannot split a cell, we have to live with this
       */
      
      if((costEstimation(part) > maxCostPerPartition) && (part.lengths.find ( _ > sideLength ).isDefined) ) {
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
    
    val partCounts = cellHistogram.view.flatMap { case (cell, count) =>
      bounds.view.map(_._1).filter { p => p.contains(cell) }.map { p => (p, count) }        
    }.groupBy(_._1).map { case (part, arr) => (part, arr.map(_._2).sum) }.toList    
    
    // _2 is the count for each partition
    val avgPoints = partCounts.view.map(_._2).sum.toDouble / partCounts.size
    val maxPts = partCounts.view.map(_._2).max
    val minPts = partCounts.view.map(_._2).min
    
    val maxPoints = partCounts.filter(_._2 == maxPts)
    val minPoints = partCounts.filter(_._2 == minPts)
    
    val variance = partCounts.map { case (part, count) => Math.pow( count - avgPoints, 2) }.sum
    
    
    
    val area = bounds.view.map(_._1.volume).sum
    val avgArea = area / numParts
    val partAreas = partCounts.map { case (part,_) => (part, part.volume) }
    // _2 is the area of a partition
    val maxA = partAreas.view.map(_._2).max
    val minA = partAreas.view.map(_._2).min
    
    val maxArea = partAreas.filter(_._2 == maxA)
    val minArea = partAreas.filter(_._2 == minA)
    
    val areaVariance = partAreas.map{ case (part, area) => Math.pow( area - avgArea, 2) }.sum
    
    PartitionStats(numParts, avgPoints, maxPoints, minPoints, variance, area, avgArea, maxArea, minArea) 
  }  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
}
package dbis.stark.spatial.partitioner

import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveAction
import dbis.stark.spatial.NRectRange
import dbis.stark.spatial.NPoint

/**
 * A data class to store information about the created partitioning
 */
case class PartitionStats(numPartitions: Int, 
    avgPoints: Double,    
    maxPoints: List[(NRectRange, Int)],
    minPoints: List[(NRectRange, Int)],
    numPointsVariance: Double,
    volume: Double,
    avgVolume: Double,
    maxVolume: List[(NRectRange, Double)],
    minVolume: List[(NRectRange, Double)]
  )
  
/**
 * A binary space partitioning algorithm implementation based on 
 * 
 * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
 * by He, Tan, Luo, Feng, Fan 
 * 
 * @param _ll The lower left point of the data space, i.e. min value in all dimensions
 * @param ur The upper right point of the data space, i.e. max value in all dimensions
 * @param cellHistogram A list of all cells and the number of points in them. Empty cells can be left out
 * @param sideLength The side length of the (quadratic) cell
 * @param maxCostPerPartition The maximum cost that one partition should have to read (currently: number of points). 
 * This cannot be guaranteed as there may be more points in a cell than <code>maxCostPerPartition</code>, but a cell
 * cannot be further split.   
 */
class BSP(_ll: Array[Double], _ur: Array[Double],
    _cellHistogram: Array[(NRectRange, Int)],
    _sideLength: Double,
    _maxCostPerPartition: Double
  ) extends Serializable {
  
  require(_ll.size > 0, "zero dimension is not supported")
  require(_ll.size == _ur.size, "Equal number of dimension required")
  require(_cellHistogram.nonEmpty, "cell histogram must not be empty")
  require(_maxCostPerPartition > 0, "max cost per partition must not be negative or zero")
  require(_sideLength > 0, "cell side length must not be negative or zero")
  
  def this(_ll: NPoint, _ur: NPoint, hist: Array[(NRectRange, Int)], l: Double, cost: Double) = 
    this(_ll.c, _ur.c, hist, l, cost)
  
  
  // getter methods for external classes  
  def ll = _ll
  def ur = _ur
  def sideLength = _sideLength
  def maxCostPerPartition = _maxCostPerPartition
  
  
  /**
   * Compute the cost for a partition, i.e. sum the cost
   * for each cell in that partition.
   * 
   * @param part The partition 
   */
  def costEstimation(part: NRectRange): Double =
    _cellHistogram
      // consider only cells that really contains data and that belong to the given range/region
      .filter { case (cell, cnt) => cnt > 0 && part.contains(cell) }  
      .map(_._2) // project to count value (number of elements in cell)
      .sum       // sum up 
    
  protected[spatial] def cellsPerDimension(part: NRectRange) = (0 until part.dim).map { dim => 
      math.ceil(part.lengths(dim) / _sideLength).toInt  
    }    
    
  /**
   * Split the given partition into two partitions so that 
   * <br/>
   * part = part1 u part2
   * <br/><br/>
   * A split is done along the cell borders of each dimension. The generate candidate split lines,
   * which is each cell border in each dimension and compute the cost for this candidate split.
   * The first split that creates a minimal cost difference between the two created partitions 
   * is chosen
   * <br><br>
   * Note that if a partition consists of two cells and one cell has more elements that max costs
   * allows and the other one is empty, then the partition will be split, resulting in two partitions 
   * with one cell each. One partition contains the cell with the elements and the other partition contains
   * an empty cell. In this case we return <code>None</code> for the empty partition 
   * 
   * @param part The partition to split
   * @return Returns the two created partitions. If one of them is empty, it is <code>None</code>
   */
  protected[spatial] def costBasedSplit(part: NRectRange): (Option[NRectRange], Option[NRectRange]) = {
    var minCostDiff = Double.PositiveInfinity
    
    // result variable with default value - will be overridden in any case
    var parts: (Option[NRectRange], Option[NRectRange]) = (
      Some(NRectRange(-1, NPoint(0,0),NPoint(0,0))), 
      Some(NRectRange(-1,NPoint(0,0),NPoint(0,0)))
    ) 
    
    /* 
     * count how many cells we have in each dimension and
     * process only those dimensions, were there is more than on cell, 
     * i.e. we could split, actually
     */
    cellsPerDimension(part).zipWithIndex      // index is the dimension
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
          ur(dim) = part.ll(dim) + i*_sideLength
          
          NRectRange(part.ll.clone(), NPoint(ur))
        }
        
        val p2 = {
          val ll = part.ll.c.clone()
          ll(dim) += i*_sideLength 
         
          NRectRange(NPoint(ll), part.ur.clone())
          
        }
        
        // calculate costs in each candidate partition
        val p1Cost = costEstimation(p1)
        val p2Cost = costEstimation(p2)
      
        // if cost difference is (currently) minimal, store this partitioning 
        val diff = Math.abs( p1Cost - p2Cost )
        if(diff < minCostDiff) {
          minCostDiff = diff
          
          val s1 = if(p1Cost <= 0) None else Some(p1)
          val s2 = if(p2Cost <= 0) None else Some(p2)
            
          
          parts = (s1,s2)
        }
      }
    }
    
    
    /* at this point we have checked all candidate partitionings in each dimension
     * and have stored the one that creates a minimal cost difference between both
     * partitions - return this
     */
    
    parts
  }  
  
  protected[spatial] lazy val start = {
	  /* start with a partition covering the complete data space
	   * If the last cell ends beyond the max values created from
	   * the data points, adjust it to completely cover the last cell, 
	   * 
	   * i.e. we expand our space to complete cells
	   */
	  var s = NRectRange(0, NPoint(_ll), NPoint(_ur))
			  
			  val cellsPerDim = cellsPerDimension(s)
			  val newUr = _ur.zipWithIndex.map { case (value, dim) => 
			  if(_ll(dim) + cellsPerDim(dim) * _sideLength > _ur(dim))
				  _ll(dim) + cellsPerDim(dim) * _sideLength
				  else
					  _ur(dim)
	  }
	  // adjust start range
	  NRectRange(0, s.ll, NPoint(newUr))
    
  }
    
  /**
   * Compute the partitioning using the cost based BSP algorithm
   * 
   * This is a lazy value    
   */
  lazy val partitions = {
    
    // add it to processing queue
    val queue = Queue(start)

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
      
      if((costEstimation(part) > _maxCostPerPartition) && (part.lengths.find ( _ > _sideLength ).isDefined) ) {
        
        val (p1, p2) = costBasedSplit(part)
        
        // if the generated partition was empty, do not add it
        if(p1.isDefined)
        	queue.enqueue(p1.get)
        	
        if(p2.isDefined)
        	queue.enqueue(p2.get)
        	
      } else
        resultPartitions += part
    }
    
    // index is the ID of the partition
    resultPartitions.zipWithIndex.foreach { case (r, i) => 
      r.id = i  
    }

    resultPartitions.toList
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
    val numParts = partitions.size
    
    val partCounts = _cellHistogram.view
      .flatMap { case (cell, count) =>
        partitions.view
//          .map(_._1)
          .filter { p => p.contains(cell) }
          .map { p => (p, count) }        
      }
      .groupBy(_._1)
      .map { case (part, arr) => 
        (part, arr.map(_._2).sum) 
      }.toList    
    
    // _2 is the count for each partition
    val avgPoints = partCounts.view.map(_._2).sum.toDouble / partCounts.size
    val maxPts = partCounts.view.map(_._2).max
    val minPts = partCounts.view.map(_._2).min
    
    val maxPoints = partCounts.filter(_._2 == maxPts)
    val minPoints = partCounts.filter(_._2 == minPts)
    
    val variance = partCounts.map { case (part, count) => Math.pow( count - avgPoints, 2) }.sum
    
    
    
    val area = partitions.view.map(_.volume).sum
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

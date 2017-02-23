package dbis.stark.spatial.partitioner

import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import dbis.stark.spatial.NRectRange
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.Cell

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer

/**
 * A data class to store information about the created partitioning
 */
case class PartitionStats(
    ll: NPoint,
    ur: NPoint,
    start: Cell,
    numPartitions: Int, 
    avgPoints: Double,    
    maxPoints: List[(Cell, Int)],
    minPoints: List[(Cell, Int)],
    numPointsVariance: Double,
    volume: Double,
    avgVolume: Double,
    maxVolume: List[(Cell, Double)],
    minVolume: List[(Cell, Double)],
    histoSize: Int
  ) {
  
  override def toString() = s"""stats:
    start range: $start 
    ll: $ll
    ur: $ur
    numPartitions: $numPartitions
    avgPoints: $avgPoints
    maxPoints: $maxPoints
    minPoints: $minPoints
    numPointsVariance: $numPointsVariance
    volume: $volume
    avg vol: $avgVolume
    max vol: $maxVolume
    min vol: $minVolume
    histo size: $histoSize
    """
}
  
object BSP {
  
  val DEFAULT_PARTITION_BUFF_SIZE = 100
  
}

/**
 * A binary space partitioning algorithm implementation based on 
 * 
 * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
 * by He, Tan, Luo, Feng, Fan 
 * 
 * @param _ll The lower left point of the data space, i.e. min value in all dimensions
 * @param _ur The upper right point of the data space, i.e. max value in all dimensions
 * @param _cellHistogram A list of all cells and the number of points in them. Empty cells can be left out
 * @param _sideLength The side length of the (quadratic) cell
 * @param _maxCostPerPartition The maximum cost that one partition should have to read (currently: number of points).
 * This cannot be guaranteed as there may be more points in a cell than <code>maxCostPerPartition</code>, but a cell
 * cannot be further split.   
 */
class BSP(_ll: Array[Double], var _ur: Array[Double],
    _cellHistogram: Array[(Cell, Int)],
    _sideLength: Double,
    _maxCostPerPartition: Double,
    withExtent: Boolean = false,
    numCellThreshold: Int = -1
  ) extends Serializable {
  
  require(_ll.size > 0, "zero dimension is not supported")
  require(_ll.size == _ur.size, "Equal number of dimension required")
  require(_cellHistogram.nonEmpty, "cell histogram must not be empty")
  require(_maxCostPerPartition > 0, "max cost per partition must not be negative or zero")
  require(_sideLength > 0, "cell side length must not be negative or zero")
  
  def this(_ll: NPoint, _ur: NPoint, hist: Array[(Cell, Int)], l: Double, cost: Double, withExtent: Boolean) = 
    this(_ll.c, _ur.c, hist, l, cost, withExtent)
  
    
  
  // getter methods for external classes  
  def ll = _ll
  def ur = _ur
  def sideLength = _sideLength
  def maxCostPerPartition = _maxCostPerPartition
  
  
  private lazy val numXCells = math.ceil(math.abs(_ur.head - _ll.head) / _sideLength).toInt
  
  protected[spatial] def cellId(p: NPoint): Int = {
    val x = math.floor(math.abs(p(0) - ll(0)) / _sideLength).toInt
    val y = math.floor(math.abs(p(1) - ll(1)) / _sideLength).toInt
    y * numXCells + x
  }

  /**
    * Determine the IDs of the cells that are contained by the given range
    * @param r The range
    * @return Returns the list of Cell IDs
    */
  def getCellsIn(r: NRectRange): Seq[Int] = {
    
//	  require(r.ll.c.zipWithIndex.forall { case (c, idx) => c >= _ll(idx)} , s"""invalid LL of range for cells in range
//		  range: $r
// 		  ll: ${_ll.mkString(",")}
//  	  ur: ${_ur.mkString(",")}
//	  """)  
//    
//	  require(r.ur.c.zipWithIndex.forall { case (c, idx) => c <= _ur(idx)} , s"""invalid UR of range for cells in range
//      range: $r
//      ll: ${_ll.mkString(",")}
//      ur: ${_ur.mkString(",")}
//    """)
      
    
    val numCells = cellsPerDimension(r)
    
    // the cellId of the lower left point of the given range
    val llCellId = cellId(r.ll)

    (0 until numCells(1)).flatMap { i =>
      llCellId + i * numXCells until llCellId + numCells(0) + i * numXCells
    }
  }
  
  
  
  /**
   * Compute the cost for a partition, i.e. sum the cost
   * for each cell in that partition.
   * 
   * @param part The partition
   * @return Returns the cost, i.e. the number of points, of the given cell
   */
  def costEstimation(part: Cell): Double = {
    val cellIds = getCellsIn(part.range)
    var i = 0
    var sum = 0
    while (i < cellIds.size) {
      val id = cellIds(i)
      if (id >= 0 && id < _cellHistogram.length) {
        sum += _cellHistogram(id)._2
      }
      i += 1
    }
    sum

//        getCellsIn(part.range).iterator
//          .filter{id => id >= 0 && id < _cellHistogram.length && _cellHistogram(id)._2 > 0}
//          .map{id => _cellHistogram(id)._2}
//          .sum
  }
    
  protected[spatial] def cellsPerDimension(part: NRectRange): IndexedSeq[Int] = (0 until part.dim).map { dim =>
      math.ceil(part.lengths(dim) / _sideLength).toInt  
    }

  /**
    * Determine the extent of the given range. The extent is computed by combining the extents
    * of all cotnained elements
    * @param range The range to determine the extent fr
    * @return Returns the extent
    */
  protected[spatial] def extentForRange(range: NRectRange): NRectRange = {
//    getCellsIn(range)
//      .filter { id => id >= 0 && id < _cellHistogram.length } // FIXME: we should actually make sure cellInRange produces always valid cells
//      .map { id => _cellHistogram(id)._1.extent } // get the extent for the cells
//      .foldLeft(range){ (e1,e2) => e1.extend(e2) } // combine all extents to the maximum extent

    val cellIds = getCellsIn(range)

    var i = 0
    var extent = range

    while(i < cellIds.length) {
      val id = cellIds(i)
      if(id >= 0 && id < _cellHistogram.length) {
        extent = extent.extend(_cellHistogram(id)._1.extent)
      }
      i += 1
    }

    extent

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
  protected[spatial] def costBasedSplit(part: Cell): (Option[Cell], Option[Cell]) = {
    var minCostDiff = Double.PositiveInfinity
    
    // result variable with default value - will be overridden in any case
    var parts: (Option[Cell], Option[Cell]) = ( None, None ) 
    
    /* 
     * count how many cells we have in each dimension and
     * process only those dimensions, were there is more than on cell, 
     * i.e. we could split, actually
     */
    
    cellsPerDimension(part.range).iterator.zipWithIndex      // index is the dimension -- (numCells, dim)
                      .filter(_._1 > 1)             // filter for number of cells
                      .foreach { case (numCells, dim) =>


      var prevP1Range: Option[Cell] = None                    
//       calculate candidate partitions it we split at each possible cell
//      for(i <- (1 until numCells)) {
      var i = 1
      while(i < numCells) {
        
        // TODO: better documentation for this calculation formulas
        val p1 = {
          
          /* we need to copy the array, otherwise we have wrong values
           * in calculation for p2
           */
          val ur = part.range.ur.c.clone()
          ur(dim) = part.range.ll(dim) + i*_sideLength
          
          val range = NRectRange(part.range.ll.clone(), NPoint(ur)) 

          val cell = if(withExtent) {
            /* we create the extent of this new partition from the extent of
             * all contained cells
             * TODO: for each iteration, we could re-use the extent from the 
             * previous iteration and extend it with the extent of the new cells
             */
            
            val diffRange = if(prevP1Range.isEmpty) range else range.diff(prevP1Range.get.range)
            val diffRangeExtent = extentForRange(diffRange)
            val extent = prevP1Range.map{ p => p.extent.extend(diffRangeExtent)}.getOrElse(diffRangeExtent)   
            Cell(range, extent)
          } else {
            Cell(range)
          }
          
          cell
        }
        if(withExtent) {
          prevP1Range = Some(p1)
        }
        
        
        val p2 = {
          val rll = part.range.ll.c.clone()
          rll(dim) += i*_sideLength 
         
          /*
           * Here, we cannot add the extent of new cells, since P2 shrinks with the increase of
           * P1. Thus we have fewer cells and our extent can only shrink as well (or stay unchanged).
           * However, I have no good idea how to compute the shrinking.
           */
          val range = NRectRange(NPoint(rll), part.range.ur.clone())
          
          val cell = if(withExtent) {
//            val thecells = getCellsIn(range, ll(0), ll(1))
            val extent = extentForRange(range)
            Cell(range, extent)  
          } else {
            Cell(range)
          }
          
          cell
        }
        
        require(p1.range.extend(p2.range) == part.range, "created partitions must completely cover input partition")
        
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
        i += 1
      }
    }
    
    
    /* at this point we have checked all candidate partitionings in each dimension
     * and have stored the one that creates a minimal cost difference between both
     * partitions - return this
     */
    
    parts
  }  
  
  protected[spatial] var start = {
    
	  /* start with a partition covering the complete data space
	   * If the last cell ends beyond the max values created from
	   * the data points, adjust it to completely cover the last cell, 
	   * 
	   * i.e. we expand our space to complete cells
	   */
	  var s = Cell(0, NRectRange(NPoint(_ll), NPoint(_ur)))
			  
    val cellsPerDim = cellsPerDimension(s.range)
    val newUr = _ur.zipWithIndex.map { case (value, dim) =>
      if(_ll(dim) + cellsPerDim(dim) * _sideLength > _ur(dim))
        _ll(dim) + cellsPerDim(dim) * _sideLength
      else
        _ur(dim)
	  }
	  
	  _ur = newUr
	  
	  // adjust start range
	  Cell(0, NRectRange(s.range.ll, NPoint(newUr)))
    
  }
    
  /**
   * Compute the partitioning using the cost based BSP algorithm
   * 
   * This is a lazy value    
   */
  var partitions = {
//    val startTime = System.currentTimeMillis()
    val resultPartitions = new ArrayBuffer[Cell](BSP.DEFAULT_PARTITION_BUFF_SIZE)
    
    val nonempty = _cellHistogram.filter{ case (_, cnt) => cnt > 0 }.map(_._1)

    if(nonempty.size <= numCellThreshold) {
      resultPartitions ++= nonempty.map(_.clone())
    } else {
    
      // add it to processing queue
      val queue = Queue(start)
      while(queue.nonEmpty) {
        val part = queue.dequeue()
        
        /* if the partition to process is more expensive (= has more elements) than max cost
         * AND it is still larger than one cell, split it
         * Otherwise we use it as a result partition
         * 
         * It may happen that a cell (which is our finest granularity) contains more elements
         * than max cost allows, however, since we cannot split a cell, we have to live with this
         */
        
        if((costEstimation(part) > _maxCostPerPartition) && 
            (part.range.lengths.find ( _ > _sideLength ).isDefined) ) {
          
          val (p1, p2) = costBasedSplit(part)

          /* Do not add partition for further processing if 
           *  - the generated partition was empty
           *  - or it is the same as the input partition 
           * 
           * The second case may happen if one partition was empty 
           * 
           */
          if(p1.isDefined) {
            if(p1.get != part)
          	  queue.enqueue(p1.get)
          	else
          	  resultPartitions += p1.get
          }
          	
          if(p2.isDefined) {
            if(p2.get != part)
          	  queue.enqueue(p2.get)
          	else
          	  resultPartitions += p2.get
          }
          	
          	
        } else {
          resultPartitions += part.clone()
      
        }
      }
    }
    
    // index is the ID of the partition
    resultPartitions.zipWithIndex.foreach { case (p, i) =>
      p.id = i  
    }
    
//    val endTime = System.currentTimeMillis()
//    println(s"partitioning took ${endTime - startTime} ms")
    resultPartitions.toArray
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
          .filter { p => p.range.contains(cell.range) }
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
    
    
    
    val area = partitions.view.map(_.range.volume).sum
    val avgArea = area / numParts
    val partAreas = partCounts.map { case (part,_) => (part, part.range.volume) }
    // _2 is the area of a partition
    val maxA = partAreas.view.map(_._2).max
    val minA = partAreas.view.map(_._2).min
    
    val maxArea = partAreas.filter(_._2 == maxA)
    val minArea = partAreas.filter(_._2 == minA)
    
    val areaVariance = partAreas.map{ case (part, area) => Math.pow( area - avgArea, 2) }.sum
    
    PartitionStats(NPoint(ll), NPoint(ur), start,numParts, avgPoints, maxPoints, minPoints, variance, area, avgArea, maxArea, minArea, _cellHistogram.size) 
  }  
    
}

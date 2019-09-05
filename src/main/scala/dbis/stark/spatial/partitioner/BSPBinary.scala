package dbis.stark.spatial.partitioner

import dbis.stark.spatial.{Cell, NPoint, NRectRange}

import scala.collection.mutable.ArrayBuffer



/**
 * A binary space partitioning algorithm implementation based on 
 * 
 * MR-DBSCAN: A scalable MapReduce-based DBSCAN algorithm for heavily skewed data
 * by He, Tan, Luo, Feng, Fan 
 * 
 * @param _ll The lower left point of the data space, i.e. min value in all dimensions
 * @param _ur The upper right point of the data space, i.e. max value in all dimensions
 * @param histogram A list of all cells and the number of points in them. Empty cells can be left out
 * @param _sideLength The side length of the (quadratic) cell
 * @param _maxCostPerPartition The maximum cost that one partition should have to read (currently: number of points).
 * This cannot be guaranteed as there may be more points in a cell than <code>maxCostPerPartition</code>, but a cell
 * cannot be further split.   
 */
class BSPBinary(_ll: Array[Double], var _ur: Array[Double],
    depth: Int,
    histogram: Array[(Cell, Int)],
    _sideLength: Double,
    _maxCostPerPartition: Double,
    withExtent: Boolean = false,
    numCellThreshold: Int = -1
  ) extends Serializable {

  require(depth > 0, "depth must be > 0")
  require(_ll.length > 0, "zero dimension is not supported")
  require(_ll.length == _ur.size, "Equal number of dimension required")
  require(_maxCostPerPartition > 0, "max cost per partition must not be negative or zero")
  require(histogram.length > 0, "hisogram must not be empty")
  require(_sideLength > 0, "cell side length must be > 0")
    
  
  // getter methods for external classes  
  def ll = _ll
  def ur = _ur
  def maxCostPerPartition = _maxCostPerPartition
  
  
  private lazy val numXCells = math.ceil(math.abs(_ur.head - _ll.head) / _sideLength).toInt
  
  protected[spatial] def cellId(p: NPoint) = {
      
      val x = math.ceil(math.abs(p(0) - ll(0)) / _sideLength).toInt
      val y = math.ceil(math.abs(p(1) - ll(1)) / _sideLength).toInt
      y * numXCells + x
    }
  
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
      llCellId + i*numXCells until llCellId+numCells(0)+ i*numXCells
    }
  }
  
  
  
  /**
   * Compute the cost for a partition, i.e. sum the cost
   * for each cell in that partition.
   * 
   * @param part The partition 
   */
  def costEstimation(part: Cell): Double = getCellsIn(part.range)
    .filter{id => id >= 0 && id < histogram.length && histogram(id)._2 > 0}
    .map{id => histogram(id)._2}
    .sum
//    histogram
//      // consider only cells that really contains data and that belong to the given range/region
//      .filter { case (cell, cnt) => cnt > 0 && part.range.contains(cell.range) }  
//      .map(_._2) // project to count value (number of elements in cell)
//      .sum       // sum up 
    
  protected[spatial] def cellsPerDimension(part: NRectRange) = (0 until part.dim).map { dim =>  
      math.ceil(part.lengths(dim) / _sideLength).toInt  
    }
      
  protected[spatial] def extentForRange(range: NRectRange) = {
    getCellsIn(range)
      .filter { id => id >= 0 && id < histogram.length } // FIXME: we should actually make sure cellInRange produces always valid cells
      .map { id => histogram(id)._1.extent } // get the extent for the cells
      .foldLeft(range){ (e1,e2) => e1.extend(e2) } // combine all extents to the maximum extent 
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
  protected[spatial] def split(part: Cell, depth: Int): ArrayBuffer[Cell] = {
//    println(s"splitting ${part.range} at level $depth")
    
    val cost = costEstimation(part)
    val res = if(depth > 0 && cost  > _maxCostPerPartition) {
      
    	val parts = new ArrayBuffer[Cell](10)
      val middles = part.range.lengths.map { l => l / 2 } 
      
      val r1ll = part.range.ll.clone()
      val r1ur = r1ll.c.zipWithIndex.map {case (v,i) => v + middles(i)  }
      val r1 = NRectRange(r1ll, NPoint(r1ur) )
      
      val r2 = NRectRange(NPoint(r1ur(0), r1ll(1)), NPoint(part.range.ur(0), r1ur(1)))
      
      val r3 = NRectRange(NPoint(r1ll(0), part.range.ur(1)), NPoint(r1ur(0), part.range.ur(1)) )
      
      val r4 = NRectRange(r1.ur.clone(), part.range.ur.clone())
      
      val cost1 = costEstimation(Cell(r1)) 
      if(cost1 > 0) {
        if(cost1 > _maxCostPerPartition)
          parts ++= split(Cell(r1), depth - 1)
        else 
          parts += Cell(r1)
      }
      
      val cost2 = costEstimation(Cell(r2)) 
      if(cost2 > 0) {
        if(cost2 > _maxCostPerPartition)
          parts ++= split(Cell(r2), depth - 1)
        else 
          parts += Cell(r2)
      }
      
      val cost3 = costEstimation(Cell(r3)) 
      if(cost3 > 0) {
        if(cost3 > _maxCostPerPartition)
          parts ++= split(Cell(r3), depth - 1)
        else 
          parts += Cell(r3)
      }
      
      val cost4 = costEstimation(Cell(r4)) 
      if(cost4 > 0) {
        if(cost4 > _maxCostPerPartition)
          parts ++= split(Cell(r4), depth - 1)
        else 
          parts += Cell(r4)
      }

      parts
    } else { 
      ArrayBuffer(part)
    }

    
    res
  }  
    
  /**
   * Compute the partitioning using the cost based BSP algorithm
   * 
   */
  var partitions = {
    
    histogram.filter(_._2 > 0).flatMap { case (cell, cnt) =>
      
      if(cnt > _maxCostPerPartition) {
        
        split(cell, depth).map { c => Cell(c.range, extentForRange(c.range)) }
        
      } else {
        if(withExtent) {
          val extent = extentForRange(cell.range)
          Array(Cell(cell.range, extent))
        } else 
          Array(cell)
      }
        
    }.toArray    
  }
  
  /** 
   * Collect statistics about the generated partitioning
   * <br><br>
   * This is a lazy value so it is not computed until it is needed.
   * However, if the partitioning was not created before this value 
   * is accessed, it will trigger the partition computation 
   */
//  lazy val partitionStats = {
//    
//    // this will trigger the computation, in case it was not done before
//    val numParts = partitions.size
//    
//    val partCounts = histogram.view
//      .flatMap { case (cell, count) =>
//        partitions.view
////          .map(_._1)
//          .filter { p => p.range.contains(cell.range) }
//          .map { p => (p, count) }        
//      }
//      .groupBy(_._1)
//      .map { case (part, arr) => 
//        (part, arr.map(_._2).sum) 
//      }.toList    
//    
//    // _2 is the count for each partition
//    val avgPoints = partCounts.view.map(_._2).sum.toDouble / partCounts.size
//    val maxPts = partCounts.view.map(_._2).max
//    val minPts = partCounts.view.map(_._2).min
//    
//    val maxPoints = partCounts.filter(_._2 == maxPts)
//    val minPoints = partCounts.filter(_._2 == minPts)
//    
//    val variance = partCounts.map { case (part, count) => Math.pow( count - avgPoints, 2) }.sum
//    
//    
//    
//    val area = partitions.view.map(_.range.volume).sum
//    val avgArea = area / numParts
//    val partAreas = partCounts.map { case (part,_) => (part, part.range.volume) }
//    // _2 is the area of a partition
//    val maxA = partAreas.view.map(_._2).max
//    val minA = partAreas.view.map(_._2).min
//    
//    val maxArea = partAreas.filter(_._2 == maxA)
//    val minArea = partAreas.filter(_._2 == minA)
//    
//    val areaVariance = partAreas.map{ case (part, area) => Math.pow( area - avgArea, 2) }.sum
//    
//    PartitionStats(NPoint(ll), NPoint(ur), start,numParts, avgPoints, maxPoints, minPoints, variance, area, avgArea, maxArea, minArea, histogram.size) 
//  }
  
  
//  var minCostDiff = Double.PositiveInfinity
//    var parts: (Option[Cell], Option[Cell]) = (None, None)
//    
//    var prevP1Range: Option[Cell] = None
//    
//    (0 until part.range.dim).foreach { dim =>
//      val p1 = {
//        val ur = part.range.ur.c.clone()
//        ur(dim) = part.range.ll(dim) + part.range.lengths(dim) 
//        val range = NRectRange(part.range.ll.clone(), NPoint(ur))
//        
//        val cell = if(withExtent) {
//          /* we create the extent of this new partition from the extent of
//           * all contained cells
//           * TODO: for each iteration, we could re-use the extent from the 
//           * previous iteration and extend it with the extent of the new cells
//           */
//          
//          val diffRange = if(prevP1Range.isEmpty) range else range.diff(prevP1Range.get.range)
//          val diffRangeExtent = extentForRange(diffRange)
//          val extent = prevP1Range.map{ p => p.extent.extend(diffRangeExtent)}.getOrElse(diffRangeExtent)   
//          Cell(range, extent)
//        } else {
//          Cell(range)
//        }
//        
//        cell
//      }
//      
//      val p2 = {
//        val rll = part.range.ll.c.clone()
//        rll(dim) += part.range.lengths(dim) 
//         
//        /*
//         * Here, we cannot add the extent of new cells, since P2 shrinks with the increase of
//         * P1. Thus we have fewer cells and our extent can only shrink as well (or stay unchanged).
//         * However, I have no good idea how to compute the shrinking.
//         */
//        val range = NRectRange(NPoint(rll), part.range.ur.clone())
//        
//        val cell = if(withExtent) {
//          val extent = extentForRange(range)
//          Cell(range, extent)  
//        } else {
//          Cell(range)
//        }
//        
//        cell
//         
//      }
//      
//      
//      // calculate costs in each candidate partition
//      val p1Cost = costEstimation(p1)
//      val p2Cost = costEstimation(p2)
//    
//      // if cost difference is (currently) minimal, store this partitioning 
//      val diff = Math.abs( p1Cost - p2Cost )
//      if(diff < minCostDiff) {
//        minCostDiff = diff
//        
//        val s1 = if(p1Cost <= 0) None else Some(p1)
//        val s2 = if(p2Cost <= 0) None else Some(p2)
//          
//        
//        parts = (s1,s2)
//      }
  
  
  
    
}

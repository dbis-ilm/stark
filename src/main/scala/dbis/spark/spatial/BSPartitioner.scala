package dbis.spark.spatial

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import dbis.spark.spatial.SpatialGridPartitioner.RectRange
import SpatialGridPartitioner.Point
import etm.core.monitor.EtmMonitor

case class PartitionStats(numPartitions: Int, 
    avgPoints: Double,    
    maxPoints: (RectRange, Int),
    minPoints: (RectRange, Int),
    numPointsVariance: Double,
    area: Double,
    avgArea: Double,
    maxArea: (RectRange, Double),
    minArea: (RectRange, Double)
  )

class BSPartitioner[G <: Geometry : ClassTag, V: ClassTag](
    @transient private val rdd: RDD[_ <: Product2[G,V]],
    sideLength: Double,
    maxCostPerPartition: Double = 1.0) extends SpatialPartitioner {
  
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
  
  val numXCells = Math.ceil((maxX - minX) / sideLength).toInt
  val numYCells = Math.ceil((maxY - minY) / sideLength).toInt
  
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
  
  protected[spatial] val cells = rdd.map { case (g,v) =>  
      val p = g.getCentroid
      
      val newX = p.getX - minX
      val newY = p.getY - minY
    
      val x = (newX.toInt / sideLength).toInt
      val y = (newY.toInt / sideLength).toInt
      
      val cellId = y * numXCells + x
    
      (getCellBounds(cellId),1)
    }.reduceByKey( _ + _).collect()

    
  protected[spatial] def costEstimation(part: RectRange): Double =
    cells.filter { case (cell, cnt) => part.contains(cell) }.map(_._2).sum
      
  protected[spatial] def lengths(part: RectRange): (Double, Double) = (part.ur.x - part.ll.x , part.ur.y - part.ll.y)  
    
  protected[spatial] def costBasedSplit(part: RectRange): (RectRange, RectRange) = {
    var minCostDiff = Double.PositiveInfinity
    
    var parts: (RectRange, RectRange) = (RectRange(-1, Point(0,0),Point(0,0)), RectRange(-1,Point(0,0),Point(0,0))) 
    
    val (xLength, yLength) = lengths(part)
    
    val xCells = Math.ceil(xLength / sideLength).toInt
    val yCells = Math.ceil(yLength / sideLength).toInt
    
    // x splits
    for(i <- (1 until xCells)) {
      val p1 = RectRange(-1, 
                  part.ll,
                  Point( part.ll.x + i * sideLength, part.ur.y)
                )
      
      val p2 = RectRange(-1,
                  Point( part.ll.x + i * sideLength, part.ll.y),
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
    
    parts
  }
  
  protected[spatial] lazy val bounds = {
    import SpatialGridPartitioner._  
    val queue = Queue(RectRange(0, Point(minX, minY), Point(maxX, maxY)))

    val resultPartitions = ListBuffer.empty[RectRange]
    
    while(queue.nonEmpty) {
      val part = queue.dequeue()
      
      val (lx, ly) = lengths(part)
      if((costEstimation(part) > maxCostPerPartition) && (lx > sideLength || ly > sideLength) ) {
        val (p1, p2) = costBasedSplit(part)
        queue.enqueue(p1, p2)
      } else
        resultPartitions += part
    }
    
    resultPartitions.toList.zipWithIndex
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
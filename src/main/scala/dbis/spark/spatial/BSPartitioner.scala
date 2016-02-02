package dbis.spark.spatial

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer

class BSPartitioner[G <: Geometry : ClassTag, V: ClassTag](
    @transient private val rdd: RDD[_ <: Product2[G,V]],
    sideLength: Double,
    maxCostPerPartition: Double = 1.0) extends SpatialGridPartitioner(1, rdd) {
  
  private val numXCells = Math.ceil(xLength / sideLength).toInt
  private val numYCells = Math.ceil(yLength / sideLength).toInt
  private val numCells = numXCells * numYCells  
  
  lazy val numPoints = rdd.count()
  lazy val f = 5
  lazy val h = 1 + Math.ceil( Math.log(numPoints / f) )
  
  
  private val cellCounts = rdd.map { case (g,v) => 
      val p = g.getCentroid
      
      val newX = p.getX - minX
      val newY = p.getY - minY
      
      val x = (newX.toInt / sideLength).toInt
      val y = (newY.toInt / sideLength).toInt
    
      val cellId = y * numXCells + x
    
      (cellId,1)
    }.reduceByKey(_ + _).collect()
  
    
  private val bounds = {
    import SpatialGridPartitioner._  
    val queue = Queue(RectRange(0, Point(minX, minY), Point(maxX, maxX)))

    def getCells(part: RectRange): List[RectRange] = ???
    
    
    def costEstimation(part: RectRange): Double = {
      val count = rdd.filter { case (g,v) =>
        val c = g.getCentroid
        part.contains(Point(c.getX, c.getY))  
      }.count()
      
      // cost formular from MR-DBSCAN
//      val da = 1 + h + Math.sqrt(count) * (2 / Math.sqrt(f) - 1) + count * (1 / (f-1))
//      
//      count * da
      
      // at this point, the cost is the number of points in the partition
      count 
    }
    
    def costBasedSplit(part: RectRange): (RectRange, RectRange) = {
      
      var minCostDiff = Double.PositiveInfinity
      
      var parts: (RectRange, RectRange) = (RectRange(-1, Point(0,0),Point(0,0)), RectRange(-1,Point(0,0),Point(0,0))) 
      
      // x splits
      for(i <- (1 until numXCells)) {
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
      for(i <- (1 until numYCells)) {
        val p1 = RectRange(-1, 
                    part.ll,
                    Point( part.ur.x, part.ur.y + i * sideLength)
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
    
    val resultPartitions = ListBuffer.empty[RectRange]
    
    while(queue.nonEmpty) {
      val part = queue.dequeue()
      
      if(costEstimation(part) > maxCostPerPartition) {
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
    
    val part = bounds.filter{ case (r,idx) =>
      val c = g.getCentroid
      r.contains(SpatialGridPartitioner.Point(c.getX, c.getY)) 
    }.head
    
    part._2
    
  }
}
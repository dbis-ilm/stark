package dbis.spark.spatial.indexed

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag

class SpatialGridPartitioner[G <: Geometry : ClassTag, V: ClassTag](
    partitionsPerDimension: Int, 
    rdd: RDD[_ <: Product2[G,V]], 
    dimensions: Int = 2) extends Partitioner {
  
  require(dimensions == 2, "Only 2 dimensions supported currently")
  
  def numPartitions: Int = Math.pow(partitionsPerDimension,dimensions).toInt
  
  private val (minX, maxX, minY, maxY, xLength, yLength) = {
    
    val coords = rdd.map{ case (g,v) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }//.cache()
    
    val minX = coords.map(_._1).min()
    val maxX = coords.map(_._2).max() + 1
    
    val minY = coords.map(_._3).min()
    val maxY = coords.map(_._4).max() + 1
    
    
    val xLength = (maxX - minX) / partitionsPerDimension 
    val yLength = (maxY - minY) / partitionsPerDimension
    
    (minX, maxX, minY, maxY, xLength, yLength)
  }
  
  private def getCellId(p: SpatialGridPartitioner.Point): Int = {
    
    require(p.x >= minX || p.x <= maxX || p.y >= minY || p.y <= maxY, s"$p out of range!")
      
    val newX = p.x - minX
    val newY = p.y - minY
    
    val x = (newX.toInt / xLength).toInt
    val y = (newY.toInt / yLength).toInt
    
    val cellId = y * partitionsPerDimension + x
    
    cellId
  }
  
  def getPartition(key: Any): Int = {
    val center = key.asInstanceOf[G].getCentroid
    
    val p = SpatialGridPartitioner.Point(center.getX, center.getY)
    
    val id = getCellId(p)
    
    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id")
    
    id
  }
  
}

object SpatialGridPartitioner {

  private case class Point(x: Double, y: Double)
  
  private case class RectRange(id: Int, ll: Point, ur: Point) {
    
    def contains(p: Point): Boolean = p.x >= ll.x && p.y >= ll.y && p.x < ur.x && p.y < ur.y 
    
  } 
}
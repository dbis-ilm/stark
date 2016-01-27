package dbis.spark.spatial.indexed

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag

class RTreePartitioner[G <: Geometry : ClassTag, V: ClassTag](
    partitions: Int, 
    rdd: RDD[_ <: Product2[G,V]]
  ) extends Partitioner {
  
  case class Point(x: Double, y: Double)
  
  case class RectRange(id: Int, ll: Point, ur: Point) {
    
    def contains(p: Point): Boolean = p.x >= ll.x && p.y >= ll.y && p.x < ur.x && p.y < ur.y 
    
  } 
  
  def numPartitions: Int = partitions 
  
  private val rangeBounds: List[RectRange] = {
    
    val coords = rdd.map{ case (g,v) =>
      val env = g.getEnvelopeInternal
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    }.cache()
    
    val minX = coords.map(_._1).min()
    val maxX = coords.map(_._2).max()
    
    val minY = coords.map(_._3).min()
    val maxY = coords.map(_._4).max()
    
    
    val xLength = (maxX - minX) / partitions
    val yLength = (maxY - minY) / partitions
    
    (0 until numPartitions).map{ idx =>
      
      val llX = minX + idx*xLength
      val llY = minY + idx*yLength
      
      val urX = llX + xLength
      val urY = llY + yLength
      
      RectRange( idx,
          Point( llX , llY ) ,
          Point( urX , urY )  
      )
    }.toList
  }
  
  require(rangeBounds.size == partitions, s"Number of computed partitions did not equal requested number of partitions: ${rangeBounds.size} != $partitions")
  
  
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[G]
    
    val center = k.getCentroid
    val p = Point(center.getX, center.getY)
    
    val e = rangeBounds.filter { range => range.contains(p) }.map(_.id).headOption
    
    if(e.isEmpty)
      throw new IllegalStateException(s"No partition for centroid: $center")
    
    e.get
  }
  
}
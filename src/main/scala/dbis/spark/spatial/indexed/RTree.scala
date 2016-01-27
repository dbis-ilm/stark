package dbis.spark.spatial.indexed

import scala.collection.JavaConversions._
import com.vividsolutions.jts.index.strtree.AbstractNode
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import com.vividsolutions.jts.index.strtree.STRtree

class RTree[G <: Geometry : ClassTag, D: ClassTag ](capacity: Int) extends STRtree(capacity)  {

  def insert(geom: G, data: D) = 
    super.insert(geom.getEnvelopeInternal, data)
  
  def query(geom: G): Iterator[D] = 
    super.query(geom.getEnvelopeInternal).map(_.asInstanceOf[D]).toIterator
  
}


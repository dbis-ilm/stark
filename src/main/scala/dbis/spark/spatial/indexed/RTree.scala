package dbis.spark.spatial.indexed

import scala.collection.JavaConversions._
import com.vividsolutions.jts.index.strtree.AbstractNode
import com.vividsolutions.jts.index.strtree.MySTRtree
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag

class RTree[G <: Geometry : ClassTag, D: ClassTag ](capacity: Int) extends MySTRtree(capacity) {

  
  protected[spatial] def this(root: AbstractNode) {
    this(4)
    setRoot(root, getNodeCapacity)
  }
  
  protected[spatial] def getSubTree(num: Int): List[AbstractNode] = {
    
    if(num == 1)
      return List(root)
    
    var allChildren = root.getChildBoundables.map{ b => b.asInstanceOf[AbstractNode]}
    
    while(allChildren.size < num) {
      
      allChildren = allChildren.flatMap { child => 
                      child.getChildBoundables.map{ b => b.asInstanceOf[AbstractNode] } 
                    }
      
    }
    
    allChildren.toList
  }
  
  override def insert(geom: G, data: D) = 
    super.insert(geom.getEnvelopeInternal, data)
  
  override def query(geom: G): Iterator[D] = 
    super.query(geom.getEnvelopeInternal).map(_.asInstanceOf[D]).toIterator
  
}


package dbis.spatialspark

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.index.strtree.AbstractNode

import scala.collection.JavaConversions._

class RTree(capacity: Int) extends STRtree(capacity) {

  def this() {
    this(RTree.DEFAULT_NODE_CAPACITY)
  }
  
  def this(root: AbstractNode) {
    this()
    this.root = root 
  }
  
  def getSubTree(num: Int): List[AbstractNode] = {
    
    if(num == 1)
      return List(root)
    
    var allChildren = root.getChildBoundables.map{ b => b.asInstanceOf[AbstractNode]}
    
    while(allChildren.size < num) {
      
      allChildren = allChildren.flatMap { child => 
                      child.getChildBoundables.map{ b => b.asInstanceOf[AbstractNode]} 
                    }
      
    }
    
    allChildren.toList
  }
  
  def setRoot(node: AbstractNode) { 
    root = node
  }
  
}

object RTree {
  
  final val DEFAULT_NODE_CAPACITY = 10
  
}

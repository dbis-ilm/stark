package dbis.stark.spatial.indexed

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import com.vividsolutions.jts.index.strtree.AbstractNode
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.strtree.STRtree

import dbis.stark.SpatialObject

/**
 * A R-Tree abstraction based on VividSolution's ST R-Tree implementation
 * 
 * @param capacity The number of elements in a node
 */
class RTree[G <: SpatialObject : ClassTag, D: ClassTag ](
    @transient private val capacity: Int
  ) extends STRtree(capacity)  {

  /**
   * Insert data into the tree
   * 
   * @param geom The geometry (key) to index
   * @param data The associated value
   */
  def insert(geom: G, data: D) = 
    super.insert(geom.getEnvelopeInternal, data)
  
  /**
   * Query the tree and find all elements in the tree that intersect
   * with the query geometry
   * 
   * @param geom The geometry to compute intersection for
   * @returns Returns all elements of the tree that intersect with the query geometry  
   */
  def query(geom: G): List[D] = 
    super.query(geom.getEnvelopeInternal).map(_.asInstanceOf[D]).toList
}


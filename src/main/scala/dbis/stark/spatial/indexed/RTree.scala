package dbis.stark.spatial.indexed

import scala.collection.JavaConversions.asScalaBuffer

import scala.reflect.ClassTag

import com.vividsolutions.jts.index.strtree.AbstractNode
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.strtree.STRtree

import dbis.stark.STObject
import com.vividsolutions.jts.index.strtree.ItemDistance
import com.vividsolutions.jts.index.ItemVisitor
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.index.strtree.STRtreePlus
import com.vividsolutions.jts.index.strtree.GeometryItemDistance

protected[indexed] class Data[G,T](var ts: Int, val data: T, val so: G) extends Serializable

/**
 * A R-Tree abstraction based on VividSolution's ST R-Tree implementation
 * 
 * @param capacity The number of elements in a node
 */
class RTree[G <: STObject : ClassTag, D: ClassTag ](
    @transient private val capacity: Int
  ) extends STRtreePlus[Data[G,D]](capacity)  {

  private var timestamp = 0
  protected[indexed] def ts = timestamp
  
  /**
   * Insert data into the tree
   * 
   * @param geom The geometry (key) to index
   * @param data The associated value
   */
  def insert(geom: G, data: D) = 
    super.insert(geom.getEnvelopeInternal, new Data(-1,data, geom))
  
  /**
   * Query the tree and find all elements in the tree that intersect
   * with the query geometry
   * 
   * @param geom The geometry to compute intersection for
   * @returns Returns all elements of the tree that intersect with the query geometry  
   */
    def query(geom: STObject) = 
      super.query(geom.getEnvelopeInternal).map(_.asInstanceOf[Data[G,D]].data).iterator
  
  /**
   * A read only query variant of the tree.
   * 
   * A query only increases the timestamp of an item.
   */
  def queryRO(qry: STObject, pred: (STObject, STObject) => Boolean) = doQueryRO(qry, qry.getEnvelopeInternal, pred)
  
  def withinDistanceRO(qry: STObject, distFunc: (STObject,STObject) => Double, maxDist: Double) = {
    val env = qry.getGeo.getEnvelopeInternal
    val env2 = new Envelope(
        new Coordinate(env.getMinX - maxDist - 1, env.getMinY - maxDist - 1), 
        new Coordinate(env.getMaxX + maxDist + 1, env.getMaxY + maxDist + 1))
    
    def pred(g1: STObject, g2: STObject) = distFunc(g1,g2) <= maxDist 
    
    doQueryRO(qry, env2, pred) 
  }
  
  
  def withinDistance(qry: STObject, distFunc: (STObject,STObject) => Double, maxDist: Double) = {
    val env = qry.getGeo.getEnvelopeInternal
    val env2 = new Envelope(
        new Coordinate(env.getMinX - maxDist - 1, env.getMinY - maxDist - 1), 
        new Coordinate(env.getMaxX + maxDist + 1, env.getMaxY + maxDist + 1))
    
    super.query(env2).map(_.asInstanceOf[Data[G,D]]).iterator.filter { p => distFunc(qry, p.so) <= maxDist }.map(_.data)
  }
  
  private def doQueryRO(qry: STObject, env: Envelope, pred: (STObject, STObject) => Boolean) = {
    class MyVisitor(ts: Int) extends ItemVisitor {
      
      override def visitItem(item: Any) {
        val i = item.asInstanceOf[Data[G,D]]
        if(i.ts == ts - 1 && pred(qry, i.so) )
          i.ts += 1
      }
    }
    
    super.query(env, new MyVisitor(timestamp))
    timestamp += 1 // increment timestamp for next query
  }
  
  private def unnest[T](l: java.util.ArrayList[_]): List[Data[G,D]] = 
    l.flatMap { e => e match {
      case d: Data[G,D] => List(d)
      case a: java.util.ArrayList[_] => unnest(a) 
    }}.toList
  
  protected[indexed] def items = super.itemsTree()
      .flatMap{ l => (l: @unchecked) match {
        case d: Data[G,D] => List(d)
        case a: java.util.ArrayList[_] => unnest(a)
        } 
      } 
  
  def result = items.filter { d => d.ts == timestamp - 1 }.map(_.data).toList
  
  /**
   * Query the tree to find k nearest neighbors.
   * 
   * Not implemented yet 
   * 
   * Maybe we could use JTSPlus: https://github.com/jiayuasu/JTSplus
   * From the GeoSpark Guys  
   */
  def kNN(geom: STObject, k: Int): Iterator[D] = //???
//    super.nearestNeighbour(geom.getEnvelopeInternal, geom, ItemDistance)
    super.kNearestNeighbour(geom.getEnvelopeInternal, geom, new GeometryItemDistance(), k).map(_.asInstanceOf[Data[G,D]].data).iterator
}


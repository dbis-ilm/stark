package dbis.stark.spatial.indexed

import dbis.stark.{Distance, STObject}
import org.locationtech.jts.index.quadtree.NodeBase
import org.locationtech.jts.index.strtree.AbstractNode

import scala.reflect.ClassTag

trait Index[G <: STObject,V] {

  def build(): Unit

  def insert(k: G, v: V): Unit

  def query(q: STObject): Iterator[V]

  def kNN(geom: STObject, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[V]

  def withinDistance(qry: G, distFunc: (G,G) => Distance, maxDist: Distance): Iterator[V]

  private[indexed] def root(): Any

}

class RTreeIndex[G <: STObject : ClassTag,V : ClassTag](order: Int) extends Index[G,V] {
  require(order > 0, "Order of R-Tree must be > 0")

  val tree = new RTree[G,V](capacity = order)

  override def build(): Unit = tree.build()

  override def insert(k: G, v: V) = tree.insert(k, v)

  override def query(q: STObject) = tree.query(q)

  override def kNN(geom: STObject, k: Int, distFunc: (STObject, STObject) => Distance) = tree.kNN(geom, k, distFunc)

  override def withinDistance(qry: G, distFunc: (G, G) => Distance, maxDist: Distance) = tree.withinDistance(qry, distFunc, maxDist)

  override private[indexed] def root(): AbstractNode = tree.getRoot
}

class QuadTreeIndex[G <: STObject : ClassTag,V : ClassTag](maxDepth: Int, minNum: Int) extends Index[G,V] {

  require(maxDepth > 0, "Max-Depth must be > 0")
  require(minNum > 0, "Min-Num must be > 0")

  val tree = new QuadTree[G,V](maxDepth, minNum)

  override def build(): Unit = {}

  override def insert(k: G, v: V) = tree.insert(k,v)

  override def query(q: STObject) = tree.query(q)

  override def kNN(geom: STObject, k: Int, distFunc: (STObject, STObject) => Distance) = tree.kNN(geom, k, distFunc)

  override def withinDistance(qry: G, distFunc: (G, G) => Distance, maxDist: Distance) = tree.withinDistance(qry, distFunc, maxDist)

  override private[indexed] def root(): NodeBase = tree.root()


}

object IndexFactory {

  def get[G <: STObject : ClassTag,V : ClassTag](conf: IndexConfig): Index[G,V] = conf match {
    case rConf: RTreeConfig => new RTreeIndex(order = rConf.order)
    case qConf: QuadTreeConfig => new QuadTreeIndex(maxDepth = qConf.maxDepth, minNum = qConf.minNum)
  }

}

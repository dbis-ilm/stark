package dbis.stark.spatial.indexed

import dbis.stark.{Distance, STObject}
import org.locationtech.jts.index.quadtree.NodeBase

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

class QuadTree[G <: STObject : ClassTag, D: ClassTag](
     private val maxDepth: Int,
     private val minNum: Int
   ) extends org.locationtech.jts.index.quadtree.Quadtree {

  def root(): NodeBase = ???


  def insert(k: G, v: D): Unit =
    super.insert(k.getEnvelopeInternal, new Data(v,k))

  def query(box: STObject): Iterator[D] =
    super.query(box.getEnvelopeInternal).iterator().asScala.map(_.asInstanceOf[Data[G,D]].data)

  def kNN(geom: STObject, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[D] = {
    ???
  }

  def withinDistance(qry: G, distFunc: (G,G) => Distance, maxDist: Distance): Iterator[D] = ???

}

package dbis.stark.spatial.indexed

import dbis.stark.STObject
import org.locationtech.jts.index.quadtree.NodeBase

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class QuadTree[G <: STObject : ClassTag, D: ClassTag](
     private val maxDepth: Int,
     private val minNum: Int
   ) extends org.locationtech.jts.index.quadtree.Quadtree with Index[G,D] {

  require(maxDepth > 0, "Max-Depth must be > 0")
  require(minNum > 0, "Min-Num must be > 0")

  override def root(): NodeBase = ???


  override def insert(k: G, v: D): Unit =
    super.insert(k.getEnvelopeInternal, new Data(v, k))

  override def query(box: STObject): Iterator[D] =
    super.query(box.getEnvelopeInternal).iterator().asScala.map(_.asInstanceOf[Data[G,D]].data)

  override def build(): Unit = {}
}

package dbis.stark.spatial.indexed

import dbis.stark.STObject
import dbis.stark.STObject.GeoType
import org.locationtech.jts.index.quadtree.NodeBase

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class QuadTree[D: ClassTag](
     private val maxDepth: Int,
     private val minNum: Int
   ) extends org.locationtech.jts.index.quadtree.Quadtree with Index[D] {

  require(maxDepth > 0, "Max-Depth must be > 0")
  require(minNum > 0, "Min-Num must be > 0")

  override def root(): NodeBase = ???


  override def insert(k: STObject, v: D): Unit =
    insert(k.getGeo, v)

  override def insert(mbr: GeoType, data: D): Unit =
    super.insert(mbr.getEnvelopeInternal, new Data(data, mbr))

  override def query(box: STObject): Iterator[D] =
    super.query(box.getGeo.getEnvelopeInternal).iterator().asScala.map(_.asInstanceOf[Data[D]].data)

  override def build(): Unit = {}
}

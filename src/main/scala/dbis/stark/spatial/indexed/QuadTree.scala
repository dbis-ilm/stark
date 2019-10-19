package dbis.stark.spatial.indexed

import dbis.stark.STObject
import dbis.stark.STObject.{GeoType, MBR}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class QuadTree[D: ClassTag](
     private val maxDepth: Int,
     private val minNum: Int
   ) extends org.locationtech.jts.index.quadtree.Quadtree with Index[D] {

  require(maxDepth > 0, "Max-Depth must be > 0")
  require(minNum > 0, "Min-Num must be > 0")

  override def root(): MBR = {

//    var root:Option[Root] = None
//    val visitor = new ItemVisitor {
//      override def visitItem(item: Any): Unit = {
//        if(root.isEmpty && item.isInstanceOf[Root])
//          root = Some(item.asInstanceOf[Root])
//      }
//    }
//
//    super.query(STObject(0,0).getGeo.getEnvelopeInternal, visitor)

    ???
  }


  override def insert(k: STObject, v: D): Unit =
    insert(k.getGeo, v)

  override def insert(mbr: GeoType, data: D): Unit =
    super.insert(mbr.getEnvelopeInternal, new Data(data, mbr))

  override def query(box: STObject): Iterator[D] =
    super.query(box.getGeo.getEnvelopeInternal).asScala.iterator.map(_.asInstanceOf[Data[D]].data)

  override def queryL(q: STObject): Array[D] =
    super.query(q.getGeo.getEnvelopeInternal).toArray.map(_.asInstanceOf[Data[D]].data)

  override def build(): Unit = {}

  override def items = super.queryAll().iterator().asScala.map(_.asInstanceOf[Data[D]].data)


}

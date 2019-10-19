package dbis.stark.spatial.indexed

import dbis.stark.STObject
import dbis.stark.STObject.{GeoType, MBR}
import org.locationtech.jts.index.intervalrtree.SortedPackedIntervalRTree
import utils.InvertavlTreeVisitor

import scala.collection.JavaConversions.asScalaBuffer
import scala.reflect.ClassTag


/**
  * A R-Tree abstraction based on VividSolution's ST R-Tree implementation
  *
  *
  */

class IntervalTree1[D: ClassTag ]() extends SortedPackedIntervalRTree with Index[D] {

  /**
    * Insert data into the tree
    *
    * @param geom The geometry (key) to index
    * @param data The associated value
    */
  def insert(geom: STObject, data: D) : Unit ={
    super.insert(geom.time.get.start.value,geom.time.get.end.get.value,new Data(data, geom))
  }

  override def insert(mbr: GeoType, data: D): Unit = throw new UnsupportedOperationException()

  /**
    * Query the tree and find all elements in the tree that intersect
    * with the query geometry
    *
    * @param geom The geometry to compute intersection for
    **/
  def query(geom: STObject) : Iterator[D]= {
    val visitor: InvertavlTreeVisitor = new InvertavlTreeVisitor()
    super.query(geom.time.get.start.value, geom.time.get.end.get.value, visitor)

    visitor.getVisitedItems.map(_.asInstanceOf[Data[D]].data).iterator
  }

  override def queryL(geom: STObject): Array[D] = {
    val visitor: InvertavlTreeVisitor = new InvertavlTreeVisitor()
    super.query(geom.time.get.start.value, geom.time.get.end.get.value, visitor)

    visitor.getVisitedItems.map(_.asInstanceOf[Data[D]].data).toArray
  }

  override def build(): Unit = {}

  override def root():MBR = ???

  override def items = {
    val visitor: InvertavlTreeVisitor = new InvertavlTreeVisitor()
    this.query(Double.MinValue, Double.MaxValue, visitor)
    visitor.getVisitedItems.map(_.asInstanceOf[Data[D]].data).iterator
  }


}




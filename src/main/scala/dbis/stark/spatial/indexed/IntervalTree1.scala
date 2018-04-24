package dbis.stark.spatial.indexed

import dbis.stark.STObject
import org.locationtech.jts.index.intervalrtree.SortedPackedIntervalRTree
import utils.InvertavlTreeVisitor

import scala.collection.JavaConversions.asScalaBuffer
import scala.reflect.ClassTag


/**
 * A R-Tree abstraction based on VividSolution's ST R-Tree implementation
 *
 *
 */
class IntervalTree1[G <: STObject : ClassTag, D: ClassTag ]() extends SortedPackedIntervalRTree with Index[G,D] {

  /**
   * Insert data into the tree
   *
   * @param geom The geometry (key) to index
   * @param data The associated value
   */
  def insert(geom: G, data: D) : Unit ={
   super.insert(geom.time.get.start.value,geom.time.get.end.get.value,new Data(data, geom))
  }

  /**
   * Query the tree and find all elements in the tree that intersect
   * with the query geometry
   *
   * @param geom The geometry to compute intersection for
   **/
    def query(geom: STObject) : Iterator[D]= {

    val visitor: InvertavlTreeVisitor = new InvertavlTreeVisitor()
    super.query(geom.time.get.start.value, geom.time.get.end.get.value, visitor)

    visitor.getVisitedItems.map(_.asInstanceOf[Data[G, D]].data).iterator

  }

  override def build(): Unit = {}

  override private[indexed] def root() = ???
}




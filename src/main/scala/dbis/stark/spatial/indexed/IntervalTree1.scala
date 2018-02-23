package dbis.stark.spatial.indexed

import org.locationtech.jts.index.intervalrtree.SortedPackedIntervalRTree
import dbis.stark.STObject
import utils.InvertavlTreeVisitor

import scala.collection.JavaConversions.asScalaBuffer
import scala.reflect.ClassTag


/**
 * A R-Tree abstraction based on VividSolution's ST R-Tree implementation
 *
 *
 */
class IntervalTree1[G <: STObject : ClassTag, V: ClassTag ] {




  val tree = new SortedPackedIntervalRTree()

  /**
   * Insert data into the tree
   *
   * @param geom The geometry (key) to index
   * @param data The associated value
   */
  def insert(geom: G, data: (G,V)) : Unit ={
   tree.insert(geom.time.get.start.value,geom.time.get.end.get.value,new Data(data, geom))
  }

  /**
   * Query the tree and find all elements in the tree that intersect
   * with the query geometry
   *
   * @param geom The geometry to compute intersection for
   **/
    def query(geom: G) : Iterator[(G,V)]= {

    val visitor: InvertavlTreeVisitor = new InvertavlTreeVisitor()
    tree.query(geom.time.get.start.value, geom.time.get.end.get.value, visitor)

    visitor.getVisitedItems.map(_.asInstanceOf[Data[G, (G,V)]].data).iterator

  }


}




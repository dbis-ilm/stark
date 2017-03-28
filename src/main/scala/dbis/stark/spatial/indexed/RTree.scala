package dbis.stark.spatial.indexed

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry}
import com.vividsolutions.jts.index.strtree.{ItemBoundable, ItemDistance, STRtreePlus}
import dbis.stark.STObject

import scala.collection.JavaConversions.asScalaBuffer
import scala.reflect.ClassTag

protected[indexed] class Data[G <: STObject, T](/*var ts: Int, */ val data: T, val so: G) extends Serializable

/**
  * A R-Tree abstraction based on VividSolution's ST R-Tree implementation
  *
  * @param capacity The number of elements in a node
  */
class RTree[G <: STObject : ClassTag, D: ClassTag](
                                                    @transient private val capacity: Int
                                                  ) extends STRtreePlus[Data[G, D]](capacity) {
  // we extend the STRtreePlus (based on JTSPlus) which implements kNN search

  //  private var timestamp = 0
  //  protected[indexed] def ts: Int = timestamp

  /**
    * Insert data into the tree
    *
    * @param geom The geometry (key) to index
    * @param data The associated value
    */
  def insert(geom: G, data: D): Unit =
    super.insert(geom.getEnvelopeInternal, new Data(/*-1,*/ data, geom))

  /**
    * Query the tree and find all elements in the tree that intersect
    * with the query geometry
    *
    * @param geom The geometry to compute intersection for
    * @return Returns all elements of the tree that intersect with the query geometry
    */
  def query(geom: STObject): Iterator[D] = {

    super.query(geom.getEnvelopeInternal).map(_.asInstanceOf[Data[G, D]].data).iterator
  }

  /**
    * A read only query variant of the tree.
    *
    * A query only increases the timestamp of an item.
    */
  //  def queryRO(qry: STObject, pred: (STObject, STObject) => Boolean) = doQueryRO(qry, qry.getEnvelopeInternal, pred)

  //  def withinDistanceRO(qry: STObject, distFunc: (STObject,STObject) => Double, maxDist: Double) = {
  //    val env = qry.getGeo.getEnvelopeInternal
  //    val env2 = new Envelope(
  //        new Coordinate(env.getMinX - maxDist - 1, env.getMinY - maxDist - 1),
  //        new Coordinate(env.getMaxX + maxDist + 1, env.getMaxY + maxDist + 1))
  //
  //    def pred(g1: STObject, g2: STObject) = distFunc(g1,g2) <= maxDist
  //
  //    doQueryRO(qry, env2, pred)
  //  }


  def withinDistance(qry: G, distFunc: (G, G) => Double, maxDist: Double) = {
    val env = qry.getGeo.getEnvelopeInternal
    val env2 = new Envelope(
      new Coordinate(env.getMinX - maxDist - 1, env.getMinY - maxDist - 1),
      new Coordinate(env.getMaxX + maxDist + 1, env.getMaxY + maxDist + 1))

    super.query(env2).map(_.asInstanceOf[Data[G, D]]).iterator.filter { p => distFunc(qry, p.so) <= maxDist }.map(_.data)
  }

  //  private def doQueryRO(qry: STObject, env: Envelope, pred: (STObject, STObject) => Boolean) = {
  //    class MyVisitor(ts: Int) extends ItemVisitor {
  //
  //      override def visitItem(item: Any) {
  //        val i = item.asInstanceOf[Data[G,D]]
  //        if(i.ts == ts - 1 && pred(i.so, qry) )
  //          i.ts += 1
  //      }
  //    }
  //
  //    super.query(env, new MyVisitor(timestamp))
  //    timestamp += 1 // increment timestamp for next query
  //  }

  /**
    * Helper method to convert and unnest a list of Data elements into
    * a flat list of Data items
    *
    * @param l The list that contains plain elements and other lists
    * @return Returns a flat list Data
    */
  private def unnest[T](l: java.util.ArrayList[_]): List[Data[G, D]] =
    l.flatMap {
      case d: Data[G, D] => List(d)
      case a: java.util.ArrayList[_] => unnest(a)
    }.toList

  /**
    * Get all items in the tree
    *
    * @return Returns a list containing all Data items in the tree
    */
  protected[indexed] def items = super.itemsTree()
    .flatMap { l => (l: @unchecked) match {
      case d: Data[G, D] => List(d)
      case a: java.util.ArrayList[_] => unnest(a)
    }
    }

  /**
    * If the tree was queried using the *RO methods you can use this method
    * to retreive the final result of the tree.
    *
    * @return Returns the list of elements that are the result of previous queries
    */
  //  def result: List[D] = items.filter { d => d.ts == timestamp - 1 }.map(_.data).toList

  /**
    * Query the tree to find k nearest neighbors.
    *
    * @param geom The reference object
    * @param k    The number of neighbors
    */
  def kNN(geom: STObject, k: Int): Iterator[D] = {
    if (size <= 0)
      Iterator.empty
    else
      super.kNearestNeighbour(geom.getEnvelopeInternal, geom, new DataDistance, k).map(_.data).iterator
  }
}

/**
  * Companion object containing helper method
  */
protected[stark] object DataDistance {
  def getGeo(o: AnyRef): Geometry = o match {
    case so: STObject => so.getGeo
    case d: Data[_, _] => d.so.getGeo
    case _ => throw new IllegalArgumentException(s"unsupported type: ${o.getClass}")
  }
}

/**
  * A distance metric that is used internally by the STRtree for comparing entries
  */
protected[stark] class DataDistance[G <: STObject, D] extends ItemDistance {
  def distance(a: ItemBoundable, b: ItemBoundable): Double = {

    val dataA = DataDistance.getGeo(a.getItem)
    val dataB = DataDistance.getGeo(b.getItem)

    dataA.distance(dataB)

  }
}


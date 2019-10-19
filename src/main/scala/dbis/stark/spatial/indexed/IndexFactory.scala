package dbis.stark.spatial.indexed

import dbis.stark.STObject.{GeoType, MBR}
import dbis.stark.{Distance, STObject}
import org.locationtech.jts.index.strtree.RTree

import scala.reflect.ClassTag

/**
  * This trait defines basic operations that we expect from an index.
  * These operations are insert and (range) query along with a explicit
  * build and root for internal use.
  * @tparam V Payload data type
  */
trait Index[V] {

  /**
    * Explicitly build the index
    */
  def build(): Unit

  /**
    * Insert an item into the index.
    * @param k The key
    * @param v The payload value
    */
  def insert(k: STObject, v: V): Unit

  def insert(mbr: GeoType, data: V): Unit

  /**
    * Query the tree
    * @param q Query object
    * @return Returns an iterator over all candidates that match the query.
    *         A second pruning step may be required, depending on the actual
    *         index implementation
    */
  def query(q: STObject): Iterator[V]
  def queryL(q: STObject): Array[V]

  /**
    * An accessor method to get the root node of the index tree.
    * @return Returns a reference to the root of the implementing tree.
    *         Users of this method need to cast to the correct type.
    */
  def root(): MBR

  def items: Iterator[V]
}

/**
  * A interface to explicitly add kNN query-possibility to an index
  * @tparam V The payload result data type
  */
trait KnnIndex[V] {
  /**
    * Perform k-nearest neighbor search
    * @param geom The query geometry to find nearest neighbors for
    * @param k The number of results to find.
    * @param distFunc The distance function to use to calculate
    *                 distances between objects during querying
    * @return Returns an iterator over the k nearest neighbors
    */
  def kNN(geom: STObject, k: Int, distFunc: (STObject, STObject) => Distance): Iterator[(V,Distance)]
}

/**
  * An interface to explicitly add withindDistance capabilities to an index
  * @tparam V The payload result data type
  */
trait WithinDistanceIndex[V] {
  /**
    * Perform withinDistance radius earch
    *
    * @param qry      The query object
    * @param distFunc The distance function to use to calculate
    *                 distances between objects during querying
    * @param maxDist  The maximum distance (radius)
    * @return Returns an iterator over all elements within the specified
    *         distance around the query object
    */
  def withinDistance(qry: STObject, distFunc: (STObject,STObject) => Distance, maxDist: Distance): Iterator[V]
}

/**
  * A factory object to conveniently create the corresponding index structure to a [[IndexConfig]]
  */
object IndexFactory {

  /**
    * Return the index structure for the given configuration
    * @param conf The [[IndexConfig]] to get the corresponding structure of
    * @tparam V Payload data type
    * @return Returns the index that corresponds to the given configuration
    */
  def get[V : ClassTag](conf: IndexConfig): Index[V] = conf match {
    case rConf: RTreeConfig => new RTree[V](capacity = rConf.order)
    case qConf: QuadTreeConfig => new QuadTree(maxDepth = qConf.maxDepth, minNum = qConf.minNum)
    case _: IntervalTreeConfig => new IntervalTree1[V]()
  }

}

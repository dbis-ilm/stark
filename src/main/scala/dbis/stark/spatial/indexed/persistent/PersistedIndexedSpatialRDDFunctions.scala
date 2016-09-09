package dbis.stark.spatial.indexed.persistent

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import dbis.stark.SpatialObject
import dbis.stark.spatial.indexed.live.IndexedSpatialRDD
import dbis.stark.spatial.indexed.RTree

class PersistedIndexedSpatialRDDFunctions[G <: SpatialObject : ClassTag, V: ClassTag](
    rdd: RDD[RTree[G, (G,V)]]) {

  def contains(qry: G) = new PersistedIndexedContainsSpatialRDD(qry, rdd)

  def containedby(qry: G) = new PersistedIndexedContainedbySpatialRDD(qry, rdd)

  def intersect(qry: G) = new PersistedIndexedIntersectionSpatialRDD(qry, rdd)

  def join[V2: ClassTag](other: RDD[(G,V2)], pred: (G,G) => Boolean) = new IndexedSpatialJoinRDD(rdd, other, pred)

  def kNN(qry: G, k: Int) = {
    val r = new PersistedIndexedKNNSpatialRDD(qry, k, rdd)
      .map { case (g,v) => (g, (g.distance(qry), v)) }
      .sortBy(_._2._1, ascending = true)
      .take(k)
      
    rdd.sparkContext.parallelize(r)
  }
  
  def withinDistance(qry: G, maxDist: Double, distFunc: (SpatialObject,SpatialObject) => Double) = 
    new PersistedIndexedWithinDistanceSpatialRDD(qry, maxDist, distFunc, rdd)
  
  
  /**
   * This gets all entries out of the index and returns
   * a plain flat RDD
   */
  def flatten = rdd.mapPartitions((trees) => trees.flatMap(tree => tree.result), true)

}
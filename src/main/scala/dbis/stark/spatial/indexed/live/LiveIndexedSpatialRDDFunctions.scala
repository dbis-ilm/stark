package dbis.stark.spatial.indexed.live

import dbis.stark.SpatialObject
import scala.reflect.ClassTag
import dbis.stark.spatial.SpatialPartitioner
import org.apache.spark.rdd.RDD

class LiveIndexedSpatialRDDFunctions[G <: SpatialObject : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner[G,V],
    rdd: RDD[(G,V)]
  ) extends Serializable {


  def intersect(qry: G) = new LiveIndexedIntersectionSpatialRDD(qry, partitioner, rdd)

  def contains(qry: G) = new LiveIndexedContainsSpatialRDD(qry, partitioner, rdd)

  def containedby(qry: G) = new LiveIndexedContainedbySpatialRDD(qry, partitioner, rdd)

  def kNN(qry: G, k: Int): RDD[(G,(Double,V))] = {
    val r = new LiveIndexedKNNSpatialRDD(qry, k, partitioner, rdd)
      .map { case (g,v) => (g, (g.distance(qry.getGeo), v)) }
      .sortBy(_._2._1, ascending = true)
      .take(k)

    rdd.sparkContext.parallelize(r)
  }
  
  def withinDistance[G2 <: SpatialObject : ClassTag](
      qry: G, 
      maxDist: Double, 
      distFunc: (SpatialObject,SpatialObject) => Double
    ) = new LiveIndexedWithinDistanceSpatialRDD(qry, maxDist, distFunc, partitioner, rdd)
}
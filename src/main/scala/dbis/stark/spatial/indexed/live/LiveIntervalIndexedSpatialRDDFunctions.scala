package dbis.stark.spatial.indexed.live

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.partitioner.SpatialPartitioner
import dbis.stark.spatial.{IndexTyp, PlainSpatialRDDFunctions, SpatialFilterRDD, _}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object LiveIntervalIndexedSpatialRDDFunctions {
  var skipFilter = false

}

class LiveIntervalIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
                                                                                     rdd: RDD[(G, V)]
                                                                                   ) extends PlainSpatialRDDFunctions[G, V](rdd) with Serializable {


  override def intersects(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.INTERSECTS,IndexTyp.TEMPORAL)
  override def containedby(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINEDBY,IndexTyp.TEMPORAL)
  override def contains(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINS,IndexTyp.TEMPORAL)







  def join2[V2: ClassTag](other: RDD[(G, V2)], pred: (STObject, STObject) => Boolean) =
    new PlainSpatialRDDFunctions(rdd).join(other, pred)

  //    new LiveIndexedSpatialCartesianJoinRDD(rdd.sparkContext, rdd, other, pred, capacity)


  def join1[V2: ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner] = None) = ???

  override def cluster[KeyType](
                                 minPts: Int,
                                 epsilon: Double,
                                 keyExtractor: ((G, V)) => KeyType,
                                 includeNoise: Boolean = true,
                                 maxPartitionCost: Int = 10,
                                 outfile: Option[String] = None
                               ): RDD[(G, (Int, V))] = ???


}


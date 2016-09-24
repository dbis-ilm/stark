package dbis.stark.spatial.indexed.live

import dbis.stark.STObject
import scala.reflect.ClassTag
import dbis.stark.spatial.SpatialPartitioner
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.NRectRange
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.Predicates
import dbis.stark.spatial.SpatialRDDFunctions
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.JoinPredicate._
import org.apache.spark.rdd.ShuffledRDD

class LiveIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner[G,V],
    rdd: RDD[(G,V)],
    capacity: Int
  ) extends SpatialRDDFunctions[G,V] with Serializable {

  def intersects(qry: G) = new LiveIndexedFilterSpatialRDD(qry, capacity, Predicates.intersects _, partitioner, rdd) 

  def contains(qry: G) = new LiveIndexedFilterSpatialRDD(qry, capacity, Predicates.contains _, partitioner, rdd)

  def containedby(qry: G) = new LiveIndexedFilterSpatialRDD(qry, capacity, Predicates.containedby _, partitioner, rdd)

  def withinDistance(
		  qry: G, 
		  maxDist: Double, 
		  distFunc: (STObject,STObject) => Double
	  ) = new LiveIndexedFilterSpatialRDD(qry, capacity, Predicates.withinDistance(maxDist, distFunc) _, partitioner, rdd)
  
  
  def kNN(qry: G, k: Int): RDD[(G,(Double,V))] = {
    val r = new LiveIndexedKNNSpatialRDD(qry, k, partitioner, rdd)
      .map { case (g,v) => (g, (g.distance(qry.getGeo), v)) }
      .sortBy(_._2._1, ascending = true)
      .take(k)

    rdd.sparkContext.parallelize(r)
  }
  
  
  def join[V2: ClassTag](other: RDD[(G,V2)], pred: (G,G) => Boolean) = 
    new LiveIndexedSpatialCartesianJoinRDD(rdd.sparkContext, rdd, other, pred, capacity)
  
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: SpatialPartitioner[G,_]) = {
    val predicate: (STObject,STObject) => Boolean = pred match {
        case INTERSECTS => Predicates.intersects _
        case CONTAINS => Predicates.contains _
        case CONTAINEDBY => Predicates.containedby _
        case _ => throw new IllegalArgumentException(s"$pred is not implemented for join")
      }
      
      new LiveIndexedJoinSpatialRDD(
          rdd.partitionBy(partitioner),
          other.partitionBy(partitioner),
          predicate,
          capacity)  
  }
  
  def cluster[KeyType](
		  minPts: Int,
		  epsilon: Double,
		  keyExtractor: ((G,V)) => KeyType,
		  includeNoise: Boolean = true,
		  maxPartitionCost: Int = 10,
		  outfile: Option[String] = None
		  ) : RDD[(G, (Int, V))] = ???
}


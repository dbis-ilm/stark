package dbis.stark.spatial.indexed.persistent

import dbis.stark.{Distance, STObject}
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{Index, RTree}
import dbis.stark.spatial.partitioner.SpatialPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    rdd: RDD[Index[G, (G,V)]]) extends Serializable {

  def contains(qry: G) = rdd.flatMap { tree => tree.query(qry).filter{ c => c._1.contains(qry) } } 

  def containedby(qry: G) = rdd.flatMap{ tree => tree.query(qry).filter{ c => c._1.containedBy(qry)} } 

  def intersects(qry: G) = rdd.flatMap { tree => tree.query(qry).filter{ c => c._1.intersects(qry)} } 

  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean) = 
    new PersistentIndexedSpatialCartesianJoinRDD(rdd.sparkContext,rdd, other, pred) 

  
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner] = None) =    
    new PersistentIndexedSpatialJoinRDD(rdd, other, pred)
  
  
  def kNN(qry: G, k: Int, distFunc: (STObject, STObject) => Distance) = {

    val nn = rdd.mapPartitions({ trees =>
        trees.flatMap { tree => 
        tree.kNN(qry, k, distFunc)
      }
    }, true)
    .map { case (g,v) => (g, (distFunc(g,qry), v)) }
    .sortBy(_._2._1, ascending = true)
    .take(k)
    
    rdd.sparkContext.parallelize(nn)
  }
  

  def withinDistance(qry: G, maxDist: Distance, distFunc: (STObject,STObject) => Distance) =
    rdd.mapPartitions({ trees => 
    trees.flatMap{ tree =>
//      tree.query(qry, Predicates.withinDistance(maxDist, distFunc) _)
      tree.withinDistance(qry, distFunc, maxDist)
      
    }
  }, true) // preserve partitioning

}
package dbis.stark.spatial.indexed.persistent

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.Predicates
import dbis.stark.spatial.SpatialRDDFunctions
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.JoinPredicate._
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.SpatialPartitioner

class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    rdd: RDD[RTree[G, (G,V)]]) extends Serializable {

  def contains(qry: G) = rdd.mapPartitions({ trees =>
    trees.flatMap {tree =>
      tree.query(qry).filter{ c => c._1.contains(qry) } 
    }
    
  }, true)
  
  def containsRO(qry: G) = rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.contains _)
      tree
    }
  }, true) // preserve partitioning
    

  def containedby(qry: G) = rdd.mapPartitions({ trees => 
    trees.flatMap{ tree =>
//      tree.queryRO(qry, Predicates.containedby _)
      tree.query(qry).filter{ c => c._1.containedBy(qry)}
    }
  }, true) // preserve partitioning
  
  
  def containedbyRO(qry: G) = rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.containedby _)
      tree
    }
  }, true) // preserve partitioning

  def intersects(qry: G) = rdd.mapPartitions({ trees => 
    trees.flatMap{ tree =>
      tree.query(qry).filter{ c => c._1.intersects(qry)}
    }
  }, true) // preserve partitioning
  
  def intersectsRO(qry: G) = rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.intersects _)
      tree
    }
  }, true) // preserve partitioning

  
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean) = 
    new PersistentIndexedSpatialCartesianJoinRDD(rdd.sparkContext,rdd, other, pred) 

  
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner[G,_]] = None) =    
    new PersistantIndexedSpatialJoinRDD(rdd, other, pred)
  
  
  def kNN(qry: G, k: Int) = {
    val nn = rdd.mapPartitions({ trees =>
        trees.flatMap { tree => 
        tree.kNN(qry, k)
      }
    }, true)
    .map { case (g,v) => (g, (g.distance(qry), v)) }
    .sortBy(_._2._1, ascending = true)
    .take(k)
    
    rdd.sparkContext.parallelize(nn)
  }
  
  def withinDistanceRO(qry: G, maxDist: Double, distFunc: (STObject,STObject) => Double) = 
    rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      //tree.queryRO(qry, Predicates.withinDistance(maxDist, distFunc) _)
      tree.withinDistanceRO(qry, distFunc, maxDist)
      tree
    }
  }, true) // preserve partitioning
  
  def withinDistance(qry: G, maxDist: Double, distFunc: (STObject,STObject) => Double) = 
    rdd.mapPartitions({ trees => 
    trees.flatMap{ tree =>
//      tree.query(qry, Predicates.withinDistance(maxDist, distFunc) _)
      tree.withinDistance(qry, distFunc, maxDist)
      
    }
  }, true) // preserve partitioning
  
  
  /**
   * This gets all entries out of the index and returns
   * a plain flat RDD
   */
  def flatten = rdd.mapPartitions((trees) => trees.flatMap(tree => tree.result), true)

}
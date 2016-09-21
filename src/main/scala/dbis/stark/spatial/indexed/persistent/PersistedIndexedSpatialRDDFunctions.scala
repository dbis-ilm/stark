package dbis.stark.spatial.indexed.persistent

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import dbis.stark.STObject
import dbis.stark.spatial.indexed.live.IndexedSpatialRDD
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.Predicates

class PersistedIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    rdd: RDD[RTree[G, (G,V)]]) {

  def contains(qry: G) = rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.contains _)
      tree
    }
  }, true) // preserve partitioning
    

  def containedby(qry: G) = rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.containedby _)
      tree
    }
  }, true) // preserve partitioning

  def intersects(qry: G) = rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.intersects _)
      tree
    }
  }, true) // preserve partitioning

  def join[V2: ClassTag](other: RDD[(G,V2)], pred: (G,G) => Boolean) = new PersistantIndexedSpatialJoinRDD(rdd, other, pred)

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
  
  def withinDistance(qry: G, maxDist: Double, distFunc: (STObject,STObject) => Double) = 
    rdd.mapPartitions({ trees => 
    trees.map{ tree =>
      tree.queryRO(qry, Predicates.withinDistance(maxDist, distFunc) _)
      tree
    }
  }, true) // preserve partitioning
  
  
  /**
   * This gets all entries out of the index and returns
   * a plain flat RDD
   */
  def flatten = rdd.mapPartitions((trees) => trees.flatMap(tree => tree.result), true)

}
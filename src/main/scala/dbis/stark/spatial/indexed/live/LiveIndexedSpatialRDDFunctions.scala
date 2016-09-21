package dbis.stark.spatial.indexed.live

import dbis.stark.STObject
import scala.reflect.ClassTag
import dbis.stark.spatial.SpatialPartitioner
import org.apache.spark.rdd.RDD
import dbis.stark.spatial.plain.LiveIndexedJoinSpatialRDD
import dbis.stark.spatial.NRectRange
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.Predicates

class LiveIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner[G,V],
    rdd: RDD[(G,V)],
    capacity: Int
  ) extends Serializable {

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
  
  
  def join[V2: ClassTag](other: RDD[(G,V2)], pred: (G,G) => Boolean) = new LiveIndexedJoinSpatialRDD(rdd, other, pred, capacity)
}



//
//rdd.mapPartitionsWithIndex({(idx, iter) =>
//    
//    val env = qry.getGeo.getEnvelopeInternal
//    val ll = NPoint(env.getMinX, env.getMaxX)
//    val ur = NPoint(env.getMinY, env.getMaxY)
//    val intersect = partitioner.partitionBounds(idx).intersects(NRectRange(ll,ur))
//    
//    if(intersect) {
//      val indexTree = new RTree[G,(G,V)](capacity)
//    
//      // Build our index live on-the-fly
//      iter.foreach{ case (geom, data) =>
//        /* we insert a pair of (geom, data) because we want the tupled
//         * structure as a result so that subsequent RDDs build from this 
//         * result can also be used as SpatialRDD
//         */
//        indexTree.insert(geom, (geom,data))
//      }
//      indexTree.build()
//    
//    
//    
//      // now query the index
//      val result = indexTree.query(qry)
//      
//      /* The result of a r-tree query are all elements that 
//       * intersect with the MBB of the query region. Thus, 
//       * for all result elements we need to check if they
//       * really intersect with the actual geometry
//       */
//      val res = result.filter{ case (g,v) => qry.intersects(g) }  
//      res.iterator
//    } else
//      Iterator.empty
//  })
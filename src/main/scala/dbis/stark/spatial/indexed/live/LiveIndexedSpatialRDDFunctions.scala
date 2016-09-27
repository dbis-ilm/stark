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
import dbis.stark.spatial.Utils
import dbis.stark.spatial.plain.PlainSpatialRDDFunctions


object LiveIndexedSpatialRDDFunctions {
  
  private def doWork[G <: STObject : ClassTag, V: ClassTag](
      qry: G,
      iter: Iterator[(G,V)], 
      predicate: (STObject, STObject) => Boolean,
      capacity: Int) = {
    
    val indexTree = new RTree[G,(G,V)](capacity)
    
    // Build our index live on-the-fly
    iter.foreach{ case (geom, data) =>
      /* we insert a pair of (geom, data) because we want the tupled
       * structure as a result so that subsequent RDDs build from this 
       * result can also be used as SpatialRDD
       */
      indexTree.insert(geom, (geom,data))
    }
    indexTree.build()
    
    
    // now query the index
    val result = indexTree.query(qry)
    
    /* The result of a r-tree query are all elements that 
     * intersect with the MBB of the query region. Thus, 
     * for all result elements we need to check if they
     * really intersect with the actual geometry
     */
    result.filter{ case (g,_) => predicate(qry,g) }
    
  }
  
}

class LiveIndexedSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)],
    capacity: Int
  ) extends SpatialRDDFunctions[G,V] with Serializable {

//    new LiveIndexedFilterSpatialRDD(qry, capacity, Predicates.intersects _, rdd) 
  def intersects(qry: G) = rdd.mapPartitionsWithIndex({(idx,iter) =>
    
    val partitionCheck = rdd.partitioner.map { p =>
      p match {
        case sp: SpatialPartitioner[G,V] => Utils.toEnvelope(sp.partitionBounds(idx).extent).contains(qry.getGeo.getEnvelopeInternal)
        case _ => true // a non spatial partitioner was used. thus we cannot be sure if we could exclude this partition and hence have to check it
      }
    }.getOrElse(true)
    
    if(partitionCheck)
      LiveIndexedSpatialRDDFunctions.doWork(qry,iter, Predicates.intersects _, capacity)
    else
      Iterator.empty
  })
    

  def contains(qry: G) = rdd.mapPartitionsWithIndex({(idx,iter) =>
    val partitionCheck = rdd.partitioner.map { p =>
      p match {
        case sp: SpatialPartitioner[G,V] => Utils.toEnvelope(sp.partitionBounds(idx).extent).intersects(qry.getGeo.getEnvelopeInternal)
        case _ => true
      }
    }.getOrElse(true)
    
    if(partitionCheck)
      LiveIndexedSpatialRDDFunctions.doWork(qry, iter, Predicates.contains _, capacity)
    else 
      Iterator.empty
  
    }) 

  def containedby(qry: G) = rdd.mapPartitionsWithIndex({(idx,iter) =>
    val partitionCheck = rdd.partitioner.map { p =>
      p match {
        case sp: SpatialPartitioner[G,V] => Utils.toEnvelope(sp.partitionBounds(idx).extent).intersects(qry.getGeo.getEnvelopeInternal)
        case _ => true
      }
    }.getOrElse(true)
    
    if(partitionCheck)
      LiveIndexedSpatialRDDFunctions.doWork(qry, iter, Predicates.containedby _, capacity)
    else 
      Iterator.empty
  
    }) 

  def withinDistance(
		  qry: G, 
		  maxDist: Double, 
		  distFunc: (STObject,STObject) => Double
	  ) = rdd.mapPartitions({iter =>
      // we don't know how the distance function looks like and thus have to scan all partitions
      LiveIndexedSpatialRDDFunctions.doWork(qry, iter, Predicates.withinDistance(maxDist, distFunc) _, capacity)
  
    }) 
  
  
  def kNN(qry: G, k: Int): RDD[(G,(Double,V))] = {
    val r = rdd.mapPartitionsWithIndex({(idx,iter) =>
              val partitionCheck = rdd.partitioner.map { p =>
                p match {
                  case sp: SpatialPartitioner[G,V] => Utils.toEnvelope(sp.partitionBounds(idx).extent).intersects(qry.getGeo.getEnvelopeInternal)
                  case _ => true
                }
              }.getOrElse(true)
              
              if(partitionCheck) {
                val tree = new RTree[G,(G,V)](capacity)
              	
                iter.foreach{ case (g,v) => tree.insert(g,(g,v)) }   
                	
                tree.build()
                
                val result = tree.kNN(qry, k)
                result
              }
              else 
                Iterator.empty
            
            })
            
          .map { case (g,v) => (g, (g.distance(qry.getGeo), v)) }
          .sortBy(_._2._1, ascending = true)
          .take(k)

    rdd.sparkContext.parallelize(r)
  }
  
  /**
   * Perform a spatial join using the given predicate function.
   * When using this variant partitions cannot not be pruned. And basically a cartesian product has
   * to be computed and filtered<br><br>
   * 
   * <b>NOTE</b> This method will <b>NOT</b> use an index as the given predicate function may want to find elements that are not returned
   * by the index query (which does an intersect)
   * 
   * @param other The other RDD to join with
   * @param pred A function to compute the join predicate. The first parameter is the geometry of the left input RDD (i.e. the RDD on which this function is called)
   * and the parameter is the geometry of <code>other</code>
   * @return Returns an RDD containing the Join result
   */
  def join[V2: ClassTag](other: RDD[(G,V2)], pred: (STObject,STObject) => Boolean) =
    new PlainSpatialRDDFunctions(rdd).join(other, pred)
//    new LiveIndexedSpatialCartesianJoinRDD(rdd.sparkContext, rdd, other, pred, capacity)
  
  /**
   * Perform a spatial join using the given predicate and a partitioner.
   * The input RDDs are both partitioned using the provided partitioner. (If they were already partitoned by the same
   * partitioner nothing is changed). 
   * This method uses the fact of the same partitioning of both RDDs and prunes partitiones that cannot contribute to the
   * join
   * 
   * @param other The other RDD to join with
   * @param pred The join predicate
   * @param partitioner The partitioner to partition both RDDs with
   * @return Returns an RDD containing the Join result 
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner[G,_]] = None) = {
      
      new LiveIndexedJoinSpatialRDD(
          if(partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd,
          if(partitioner.isDefined) other.partitionBy(partitioner.get) else other,
          pred,
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


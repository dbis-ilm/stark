package dbis.stark.spatial.plain

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.mllib.linalg.Vectors

import dbis.stark.STObject
import dbis.stark.spatial.SpatialRDD
import dbis.stark.spatial.SpatialGridPartitioner
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.BSPartitioner
import dbis.stark.spatial.indexed.RTree
import dbis.stark.dbscan.DBScan
import dbis.stark.dbscan.ClusterLabel
import dbis.stark.spatial.Utils
import dbis.stark.spatial.NRectRange
import dbis.stark.spatial.NPoint
import dbis.stark.spatial.SpatialRDDFunctions
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.JoinPredicate._
import dbis.stark.spatial.Predicates


/**
 * A helper class used in the implicit conversions
 *
 * @param rdd The original RDD to treat as a spatial RDD
 */
class PlainSpatialRDDFunctions[G <: STObject : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)]
  ) extends SpatialRDDFunctions[G,V] with Serializable {

  /**
   * Find all elements that intersect with a given query geometry
   */
  def intersects(qry: G) = rdd.mapPartitionsWithIndex({(idx,iter) =>
    
    val partitionCheck = rdd.partitioner.map { p =>
      p match {
        case sp: SpatialPartitioner[G,V] => Utils.toEnvelope(sp.partitionBounds(idx).extent).intersects(qry.getGeo.getEnvelopeInternal)
        case _ => true
      }
    }.getOrElse(true)
    
    if(partitionCheck)
      iter.filter { case (g,_) => qry.intersects(g) }
    else
      Iterator.empty
  })
    

  /**
   * Find all elements that are contained by a given query geometry
   */
  def containedby(qry: G) = rdd.mapPartitionsWithIndex({(idx,iter) =>
    
    val partitionCheck = rdd.partitioner.map { p =>
      p match {
        case sp: SpatialPartitioner[G,V] => qry.getGeo.getEnvelopeInternal.contains(Utils.toEnvelope(sp.partitionBounds(idx).extent))
        case _ => true
      }
    }.getOrElse(true)
    
    if(partitionCheck)
      iter.filter { case (g,_) => qry.intersects(g) }
    else
      Iterator.empty
  })

  /**
   * Find all elements that contain a given other geometry
   */
  def contains(o: G) = rdd.mapPartitionsWithIndex({(idx,iter) =>
    
    val partitionCheck = rdd.partitioner.map { p =>
      p match {
        case sp: SpatialPartitioner[G,V] => Utils.toEnvelope(sp.partitionBounds(idx).extent).contains(o.getGeo.getEnvelopeInternal)
        case _ => true // a non spatial partitioner was used. thus we cannot be sure if we could exclude this partition and hence have to check it
      }
    }.getOrElse(true)
    
    if(partitionCheck)
      iter.filter { case (g,_) => g.contains(o) }
    else
      Iterator.empty
  })

  def withinDistance(qry: G, maxDist: Double, distFunc: (STObject,STObject) => Double) =
    rdd.mapPartitionsWithIndex({(idx,iter) =>
    
//    val partitionCheck = rdd.partitioner.map { p =>
//      p match {
//        case sp: SpatialPartitioner[G,V] => {
//          val qryEnv = qry.getGeo.getEnvelopeInternal
//          val r = NRectRange(
//              NPoint(qryEnv.getMinX - maxDist - 1, qryEnv.getMinY - maxDist - 1), 
//              NPoint(qryEnv.getMaxX + maxDist + 1, qryEnv.getMaxY + maxDist + 1))
// 
//          val bounds = sp.partitionBounds(idx)
//          
////          Utils.toEnvelope(r).intersects(Utils.toEnvelope(bounds))
//          
//          
//        }
//        case _ => true
//      }
//    }.getOrElse(true)
      
    /* FIXME: We cannot check if we have to process a partition, because of the distance function. 
     * We would have to apply the distance function on the partition bounds to see if the partition is 
     * matches the distance function criteria. However, the dist func is defined on STObject but the
     * partitions are of NRectRange... 
     */
    val partitionCheck = true
    
    if(partitionCheck)
      iter.filter { case (g,_) => distFunc(g,qry) <= maxDist }
    else
      Iterator.empty
  })
    
      
  def kNN(qry: G, k: Int): RDD[(G,(Double,V))] = {
    // compute k NN for each partition individually --> n * k results
    val r = rdd.mapPartitions({iter => iter.map { case (g,v) => 
        val d = qry.distance(g)
        (g,(d,v)) // compute and return distance
      }
      .toList
      .sortWith(_._2._1 < _._2._1) // on distance
      .take(k) // take only the fist k 
      .toIterator // remove the iterator
    })

    // sort all n lists and sort by distance, then take only the first k elements
    val arr = r.sortBy(_._2._1, ascending = true).take(k)

    // return as an RDD
    rdd.sparkContext.parallelize(arr)
  }   
  
  
  /**
   * Join this SpatialRDD with another (spatial) RDD.
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
    def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean) = 
      new CartesianSpatialJoinRDD(rdd.sparkContext,rdd, other, pred) 
  
    def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: SpatialPartitioner[G,_]) = {
      
      val predicate: (STObject,STObject) => Boolean = pred match {
        case INTERSECTS => Predicates.intersects _
        case CONTAINS => Predicates.contains _
        case CONTAINEDBY => Predicates.containedby _
        case _ => throw new IllegalArgumentException(s"pred is not implemented for join")
      }
      
      
      new JoinSpatialRDD(
          rdd.partitionBy(partitioner) , 
          other.partitionBy(partitioner), 
          predicate)
    }
      
  
  
  /**
   * Cluster this SpatialRDD using DBSCAN
   *
   * @param minPts
   * @param epsilon
   * @param keyExtractor A function that extracts or generates a unique key for each point
   * @param includeNoise A flag whether or not to include noise points in the result
   * @param maxPartitionCost Maximum cost (= number of points) per partition
   * @param outfile An optional filename to write clustering result to
   * @return Returns an RDD which contains the corresponding cluster ID for each tuple
   */
  def cluster[KeyType](
		  minPts: Int,
		  epsilon: Double,
		  keyExtractor: ((G,V)) => KeyType,
		  includeNoise: Boolean = true,
		  maxPartitionCost: Int = 10,
		  outfile: Option[String] = None
		  ) = {
				  
	  // create a dbscan object with given parameters
	  val dbscan = new DBScan[KeyType,(G,V)](epsilon, minPts)
	  dbscan.maxPartitionSize = maxPartitionCost
			  
	  // DBScan expects Vectors -> transform the geometry into a vector
	  val r = rdd.map{ case (g,v) =>
  	  /* TODO: can we make this work for polygons as well?
		   * See Generalized DBSCAN:
		   * Sander, Ester, Krieger, Xu
		   * "Density-Based Clustering in Spatial Databases: The Algorithm GDBSCAN and Its Applications"
		   * http://link.springer.com/article/10.1023%2FA%3A1009745219419
		   */
		  val c = g.getCentroid
		  
		  
		  /* extract the key for the cluster points
		   * this is used to distinguish points that have the same coordinates
		   */
		  val key = keyExtractor((g,v))
		  
		  /* emit the tuple as input for the clustering
		   *  (id, coordinates, payload)
		   * The payload is the actual tuple to which we will project at the end
		   * to hide the internals of this implementation and to return data that
		   * matches the input format and
		   */
		  
		  (key, Vectors.dense(c.getY, c.getX), (g,v))
	  }
	  
	  // start the DBScan computation
	  val model = dbscan.run(r)
			  
	  /*
	   * Finally, transform into a form that corresponds to a spatial RDD
	   * (Geo, (ClusterID, V)) - where V is the rest of the tuple, i.e. its
	   * actual content.
	   *
	   * Also, remove noise if desired.
	   * TODO: If noise has to be removed, can we use this more deeply inside
	   * the DBSCAN code to reduce data?
	   *
	   * We do know that there is a payload, hence calling .get is safe
	   */
	  val points = (if(includeNoise) model.points else model.points.filter(_.label != ClusterLabel.Noise))
	  
	  /* if the outfile is defined, write the clustering result
	   * this can be used for debugging and visualization
	   */
	  if(outfile.isDefined)
		  points.coalesce(1).saveAsTextFile(outfile.get)
		  
	  points.map { p => (p.payload.get._1, (p.clusterId, p.payload.get._2)) }
  }

  // LIVE
  
  def liveIndex(order: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(None, order)
  
  def liveIndex(partitioner: SpatialPartitioner[G,V], order: Int): LiveIndexedSpatialRDDFunctions[G,V] = 
    liveIndex(Some(partitioner), order)
  
  def liveIndex(partitioner: Option[SpatialPartitioner[G,V]], order: Int) = {
    val reparted = if(partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd
		new LiveIndexedSpatialRDDFunctions(reparted, order)
  }

  
  
  
  def index(partitioner: SpatialPartitioner[G,V], order: Int = 10) = {
    makeIdx(rdd.partitionBy(partitioner), order)
  }
//    makeIdx(new ShuffledRDD[G,V,V](rdd, new SpatialGridPartitioner(5, rdd)))

  /**
   * Index the given RDD on its geometry component using an R-Tree
   */
  private def makeIdx(rdd: RDD[(G,V)], treeCapacity: Int) = rdd.mapPartitions( iter => {
    val tree = new RTree[G,(G,V)](treeCapacity)

    for((g,v) <- iter) {
      tree.insert(g, (g,v))
    }

    List(tree).toIterator
  } ,
  true) // preserve partitioning


}

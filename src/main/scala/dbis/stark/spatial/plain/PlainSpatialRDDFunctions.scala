package dbis.stark.spatial.plain

import dbis.stark.STObject
import dbis.stark.dbscan.{ClusterLabel, DBScan}
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import dbis.stark.spatial.{JoinPredicate, SpatialPartitioner, SpatialRDDFunctions, Utils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


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
  def intersects(qry: G) = new SpatialFilterRDD[G,V](rdd, qry, JoinPredicate.INTERSECTS)
    
  /**
   * Find all elements that are contained by a given query geometry
   */
  def containedby(qry: G) = new SpatialFilterRDD[G,V](rdd, qry, JoinPredicate.CONTAINEDBY)

  /**
   * Find all elements that contain a given other geometry
   */
  def contains(o: G) = new SpatialFilterRDD[G,V](rdd, o, JoinPredicate.CONTAINS)

  def withinDistance(qry: G, maxDist: Double, distFunc: (STObject,STObject) => Double) =
    rdd.filter{ case (g,_) => distFunc(qry,g) <= maxDist }
      
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
   * Join this SpatialRDD with another (spatial) RDD.<br><br>
   * <b>NOTE:</b> There will be no partition pruning and basically all cartesian combinations have to be checked
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (STObject,STObject) => Boolean) = 
    new CartesianSpatialJoinRDD(rdd.sparkContext,rdd, other, pred) 

  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: JoinPredicate, partitioner: Option[SpatialPartitioner] = None) =      
    new JoinSpatialRDD(
        if(partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd , 
        if(partitioner.isDefined) other.partitionBy(partitioner.get) else other, 
        pred)
      

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
	  val points = if (includeNoise) model.points else model.points.filter(_.label != ClusterLabel.Noise)
	  
	  /* if the outfile is defined, write the clustering result
	   * this can be used for debugging and visualization
	   */
	  if(outfile.isDefined)
		  points.coalesce(1).saveAsTextFile(outfile.get)
		  
	  points.map { p => (p.payload.get._1, (p.clusterId, p.payload.get._2)) }
  }

  // LIVE
  
  def liveIndex(order: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(None, order)
  
  def liveIndex(partitioner: SpatialPartitioner, order: Int): LiveIndexedSpatialRDDFunctions[G,V] = 
    liveIndex(Some(partitioner), order)
  
  def liveIndex(partitioner: Option[SpatialPartitioner], order: Int) = {
    val reparted = if(partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd
		new LiveIndexedSpatialRDDFunctions(reparted, order)
  }

  def index(partitioner: SpatialPartitioner, order: Int): RDD[RTree[G,(G,V)]] = index(Some(partitioner), order)

  /**
   * Create an index for each partition. 
   * 
   * This puts all data items of a partition into an Index structure, e.g., R-tree
   * and thus changes the type of the RDD from RDD[(STObject, V)] to RDD[RTree[STObject, (STObject, V)]]
   */
  def index(partitioner: Option[SpatialPartitioner], order: Int = 10): RDD[RTree[G,(G,V)]] = {
    val reparted = if(partitioner.isDefined) rdd.partitionBy(partitioner.get) else rdd
    reparted.mapPartitions(iter => {
      val tree = new RTree[G, (G, V)](order)

      for ((g, v) <- iter) {
        tree.insert(g, (g, v))
      }

      Iterator.single(tree)
    },
      preservesPartitioning = true) // preserve partitioning
  }

}

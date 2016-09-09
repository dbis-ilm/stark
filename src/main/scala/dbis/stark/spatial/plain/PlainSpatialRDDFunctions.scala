package dbis.stark.spatial.plain

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.mllib.linalg.Vectors
import dbis.stark.SpatialObject
import dbis.stark.spatial.SpatialRDD
import dbis.stark.spatial.SpatialGridPartitioner
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.spatial.BSPartitioner
import dbis.stark.spatial.indexed.RTree
import dbis.stark.dbscan.DBScan
import dbis.stark.dbscan.ClusterLabel


/**
 * A helper class used in the implicit conversions
 *
 * @param rdd The original RDD to treat as a spatial RDD
 */
class PlainSpatialRDDFunctions[G <: SpatialObject : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)]
  ) extends Serializable {


  def intersect(qry: G): SpatialRDD[G,V] = new IntersectionSpatialRDD(qry, rdd)

  def containedby(qry: G): SpatialRDD[G,V] = new ContainedBySpatialRDD(qry, rdd)

  def contains(qry: G): SpatialRDD[G,V] = new ContainsSpatialRDD(qry, rdd)

  def kNN(qry: G, k: Int): RDD[(G,(Double,V))] = {
    // compute k NN for each partition individually --> n * k results
    val r = new KNNSpatialRDD(qry, k, rdd)

    // sort all n lists and sort by distance, then take only the first k elements
    val arr = r.sortBy(_._2._1, ascending = true).take(k)

    // return as an RDD
    rdd.sparkContext.parallelize(arr)
  }
  
  def withinDistance(qry: G, maxDist: Double, distFunc: (SpatialObject,SpatialObject) => Double) = 
    new WithinDistanceSpatialRDD(qry,maxDist, distFunc, rdd)
  
  /**
   * Join this SpatialRDD with another (spatial) RDD.
   *
   * @param other The other RDD to join with.
   * @param pred The join predicate as a function
   * @return Returns a RDD with the joined values
   */
  def join[V2 : ClassTag](other: RDD[(G, V2)], pred: (G,G) => Boolean) = new JoinSpatialRDD(
    new ShuffledRDD[G,V,V](rdd, new BSPartitioner[G,V](rdd, 10, 10)) , other, pred)
  
  
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
  def liveIndex(ppD: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(new SpatialGridPartitioner(ppD, rdd))

  def liveIndex(partitioner: SpatialPartitioner[G,V]) = new LiveIndexedSpatialRDDFunctions(partitioner, rdd)

  def indexFixedGrid(ppD: Int, order: Int = 10) = makeIdx(grid(ppD), order)

  def index(cost: Double, cellSize: Double, order: Int = 10) = {
    val bsp = new BSPartitioner(rdd, cellSize, cost)
    makeIdx(new ShuffledRDD[G,V,V](rdd, bsp), order)
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

  /**
   * Use a Grid partitioner with a fixed number of paritions per dimension to partition
   * the dataset.
   *
   * @param ppD The number of partitions per Dimension
   * @return Returns a shuffled RDD partitioned according to the given parameter
   */
  def grid(ppD: Int) = new ShuffledRDD[G,V,V](rdd, new SpatialGridPartitioner[G,V](ppD, rdd))

}
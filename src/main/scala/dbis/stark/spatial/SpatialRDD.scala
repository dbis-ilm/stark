package dbis.stark.spatial

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Dependency
import org.apache.spark.Partitioner
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import java.nio.file.Files
import java.nio.file.StandardOpenOption

import dbis.stark.spatial.plain.IntersectionSpatialRDD
import dbis.stark.spatial.plain.ContainsSpatialRDD
import dbis.stark.spatial.plain.ContainedBySpatialRDD
import dbis.stark.spatial.plain.JoinSpatialRDD
import dbis.stark.spatial.plain.KNNSpatialRDD

import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.indexed.live.LiveIndexedIntersectionSpatialRDD
import dbis.stark.spatial.indexed.live.LiveIndexedContainsSpatialRDD
import dbis.stark.spatial.indexed.live.LiveIndexedContainedbySpatialRDD
import dbis.stark.spatial.indexed.live.IndexedSpatialRDD

import dbis.stark.spatial.indexed.persistent.PersistedIndexedIntersectionSpatialRDD
import dbis.stark.spatial.indexed.persistent.PersistedIndexedContainsSpatialRDD
import dbis.stark.spatial.indexed.persistent.PersistedIndexedContainedbySpatialRDD
import dbis.stark.spatial.indexed.persistent.IndexedSpatialJoinRDD

import dbis.stark.SpatialObject
import dbis.stark.SpatialObject._
import dbis.stark.dbscan.DBScan

/**
 * A base class for spatial RDD without indexing
 * 
 * @param prev The parent RDD
 */
abstract class SpatialRDD[G <: SpatialObject : ClassTag, V: ClassTag](
    @transient private val _sc: SparkContext,
    @transient private val _deps: Seq[Dependency[_]]
  ) extends RDD[(G,V)](_sc, _deps) {
  
  def this(@transient oneParent: RDD[(G,V)]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))
  
  /**
   * We do not repartition our data.
   */
  override protected def getPartitions: Array[Partition] = firstParent[(G,V)].partitions

  
  /**
   * Compute an intersection of the elements in this RDD with the given geometry
   */
  def intersect(qry: G): IntersectionSpatialRDD[G,V] = new IntersectionSpatialRDD(qry, this)
  
  /**
   * Find all elements that are contained by the given geometry
   * 
   * @param qry The Geometry that should contains elements of this RDD
   * @return Returns an RDD containing the elements of this RDD that are completely contained by qry
   */
  def containedby(qry: G): SpatialRDD[G,V] = new ContainedBySpatialRDD(qry, this)
  
  /**
   * Find all elements that contain the given geometry. 
   * 
   * @param g The geometry that must be contained by other geometries in this RDD
   * @return Returns an RDD consisting of all elements in this RDD that contain the given geometry g 
   */
  def contains(g: G): SpatialRDD[G,V] = new ContainsSpatialRDD(g, this)
  
  /**
   * Find the k nearest neighbors of the given geometry in this RDD
   * 
   * @param qry The geometry to find the nearest neighbors of
   * @param k The number of nearest neighbors to find
   * @return Returns an RDD containing the k nearest neighbors of qry
   */
  def kNN(qry: G, k: Int): RDD[(G,(Double,V))] = {
    // compute k NN for each partition individually --> n * k results
    val r = new KNNSpatialRDD(qry, k, this) 
    
    // sort all n lists and sort by distance, then take only the first k elements
    val arr = r.sortBy(_._2._1, ascending = true).take(k)  
    
    // return as an RDD
    this.sparkContext.parallelize(arr, this.getNumPartitions)
  }
  
}

/**
 * A helper companion object that contains implicit conversion methods to convert
 * simple RDDs to SpatialRDDs
 */
object SpatialRDD {
	
  /** 
   * Convert an RDD to a "plain" spatial RDD which uses no indexing.
   * 
   * @param rdd The RDD to convert
   * @return Returns a SpatialRDDFunctions object that contains spatial methods
   */
	implicit def convertSpatialPlain[G <: SpatialObject : ClassTag, V: ClassTag](rdd: RDD[(G, V)]): SpatialRDDFunctions[G,V] = new SpatialRDDFunctions[G,V](rdd)
	
	/** 
	 * Convert an RDD to a SpatialRDD which uses persisted indexing.
	 * 
	 * @param rdd The RDD to convert
	 * @return Returns a IndexedSpatialRDDFunctions object that contains spatial methods that use indexing
	 */
	implicit def convertSpatialPersistedIndexing[G <: SpatialObject : ClassTag, V: ClassTag](rdd: RDD[RTree[G,(G,V)]]) = new IndexedSpatialRDDFunctions(rdd)

}

//---------------------------------------------------------------------------------------
// PLAIN
//---------------------------------------------------------------------------------------

/**
 * A helper class used in the implicit conversions
 * 
 * @param rdd The original RDD to treat as a spatial RDD
 */
class SpatialRDDFunctions[G <: SpatialObject : ClassTag, V: ClassTag](
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
  
  // LIVE
  def liveIndex(ppD: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(new SpatialGridPartitioner(ppD, rdd))
  
  def liveIndex(partitioner: SpatialPartitioner[G,V]) = new LiveIndexedSpatialRDDFunctions(partitioner, rdd)
  
  def indexFixedGrid(ppD: Int) = makeIdx(grid(ppD))
  
  def index(cost: Double, cellSize: Double) = {
    val bsp = new BSPartitioner(rdd, cellSize, cost)
    makeIdx(new ShuffledRDD[G,V,V](rdd, bsp))
  }
//    makeIdx(new ShuffledRDD[G,V,V](rdd, new SpatialGridPartitioner(5, rdd)))
  
  /**
   * Index the given RDD on its geometry component using an R-Tree
   */
  private def makeIdx(rdd: RDD[(G,V)], treeCapacity: Int = 10) = rdd.mapPartitions( iter => { 
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
   * @param outfile An optional filename to write clustering result to
   * @return Returns an RDD which contains the corresponding cluster ID for each tuple 
   */
  def cluster[KeyType](
      minPts: Int, 
      epsilon: Double,
      keyExtractor: ((G,V)) => KeyType, 
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
    
    /* if the outfile is defined, write the clustering result
     * this can be used for debugging and visualization 
     */
    if(outfile.isDefined)
      model.points.coalesce(1).saveAsTextFile(outfile.get)

      
    /*
     * Finally, transform into a form that corresponds to a spatial RDD
     * (Geo, (ClusterID, V)) - where V is the rest of the tuple, i.e. its
     * actual content.
     * 
     * We do know that there is a payload, hence calling .get is safe 
     */
    model.points.map { p => (p.payload.get._1, (p.clusterId, p.payload.get._2)) }
  }
}


//---------------------------------------------------------------------------------------
// LIVE INDEXING
//---------------------------------------------------------------------------------------

class LiveIndexedSpatialRDDFunctions[G <: SpatialObject : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner[G,V], 
    rdd: RDD[(G,V)]
  ) extends Serializable {
  

  def intersect(qry: G): IndexedSpatialRDD[G,V] = new LiveIndexedIntersectionSpatialRDD(qry, partitioner, rdd)
  
  def contains(qry: G) = new LiveIndexedContainsSpatialRDD(qry, partitioner, rdd)
  
  def containedby(qry: G) = new LiveIndexedContainedbySpatialRDD(qry, partitioner, rdd)
  
  
//  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
}

//---------------------------------------------------------------------------------------
// PERSITED INDEX
//---------------------------------------------------------------------------------------

class IndexedSpatialRDDFunctions[G <: SpatialObject : ClassTag, V: ClassTag](
    rdd: RDD[RTree[G, (G,V)]]) {
  
  def contains(qry: G) = new PersistedIndexedContainsSpatialRDD(qry, rdd)
  
  def containedby(qry: G) = new PersistedIndexedContainedbySpatialRDD(qry, rdd)
  
  def intersect(qry: G) = new PersistedIndexedIntersectionSpatialRDD(qry, rdd)
  
  def join[V2: ClassTag](other: RDD[(G,V2)]) = new IndexedSpatialJoinRDD(rdd, other)
  
  def kNN(qry: G, k: Int): IndexedSpatialRDD[G,V] = ???
  
}






























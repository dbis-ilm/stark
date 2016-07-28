package dbis.spark.spatial

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Dependency
import org.apache.spark.Partitioner
import org.apache.spark.OneToOneDependency
import scala.reflect.ClassTag
import com.vividsolutions.jts.geom.Geometry
import scala.collection.JavaConversions._
import dbis.spark.spatial.indexed.live.LiveIntersectionIndexedSpatialRDD
import dbis.spark.spatial.indexed.live.IndexedSpatialRDD
import org.apache.spark.rdd.ShuffledRDD
import dbis.spark.spatial.plain.IntersectionSpatialRDD
import dbis.spark.spatial.plain.KNNSpatialRDD
import dbis.spark.spatial.indexed.persistent.PersistedIndexedIntersectionSpatialRDD
import dbis.spark.spatial.indexed.RTree
import dbis.spark.spatial.plain.ContainedBySpatialRDD
import dbis.spark.spatial.plain.JoinSpatialRDD
import org.apache.spark.SparkContext
import dbis.spark.spatial.indexed.persistent.IndexedSpatialJoinRDD
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import scala.collection.JavaConverters._
import com.vividsolutions.jts.io.WKTReader
import dbis.spark.spatial.plain.ContainsSpatialRDD

/**
 * A base class for spatial RDD without indexing
 * 
 * @param prev The parent RDD
 */
abstract class SpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
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
  def containedBy(qry: G): ContainedBySpatialRDD[G,V] = new ContainedBySpatialRDD(qry, this)
  
  /**
   * Find all elements that contain the given geometry. 
   * 
   * @param g The geometry that must be contained by other geometries in this RDD
   * @return Returns an RDD consisting of all elements in this RDD that contain the given geometry g 
   */
  def contains(g: G): ContainsSpatialRDD[G,V] = new ContainsSpatialRDD(g, this)
  
  /**
   * Find the k nearest neighbors of the given geometry in this RDD
   * 
   * @param qry The geometry to find the nearest neighbors of
   * @param k The number of nearest neighbors to find
   * @return Returns an RDD containing the k nearest neighbors of qry
   */
  def kNN(qry: G, k: Int): KNNSpatialRDD[G,V] = new KNNSpatialRDD(qry, k, this)
  
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
	implicit def convertSpatialLive[G <: Geometry : ClassTag, V: ClassTag](rdd: RDD[(G, V)]): SpatialRDDFunctions[G,V] = new SpatialRDDFunctions[G,V](rdd)
	
	/** 
	 * Convert an RDD to a SpatialRDD which uses persisted indexing.
	 * 
	 * @param rdd The RDD to convert
	 * @return Returns a IndexedSpatialRDDFunctions object that contains spatial methods that use indexing
	 */
	implicit def convertSpatialPersistedIndexing[G <: Geometry : ClassTag, V: ClassTag](rdd: RDD[RTree[G,(G,V)]]) = new IndexedSpatialRDDFunctions(rdd)

	/**
	 * Convert a string into a geometry object. The String must be a valid WKT representation
	 * 
	 * @param s The WKT string
	 * @return The geometry parsed from the given textual representation
	 */
	implicit def stringToGeom(s: String): Geometry = new WKTReader().read(s)
			
}

//---------------------------------------------------------------------------------------
// PLAIN
//---------------------------------------------------------------------------------------

/**
 * A helper class used in the implicit conversions
 * 
 * @param rdd The original RDD to treat as a spatial RDD
 */
class SpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)]
  ) extends Serializable {
  
  
  def intersect(qry: G): RDD[(G,V)] = new IntersectionSpatialRDD(qry, rdd)
  
  def contains(qry: G): RDD[(G,V)] = new ContainedBySpatialRDD(qry, rdd)
  
  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
  
  // LIVE
  def liveIndex(ppD: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(new SpatialGridPartitioner(ppD, rdd))
  
  def liveIndex(partitioner: SpatialPartitioner[G,V]) = new LiveIndexedSpatialRDDFunctions(partitioner, rdd)
  
  def indexFixedGrid(ppD: Int) = makeIdx(grid(ppD))
  
  def index(cost: Double, cellSize: Double) = 
    makeIdx(new ShuffledRDD[G,V,V](rdd, new BSPartitioner(rdd, cellSize, cost)))
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
}


//---------------------------------------------------------------------------------------
// LIVE INDEXING
//---------------------------------------------------------------------------------------

class LiveIndexedSpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner[G,V], 
    rdd: RDD[(G,V)]
  ) extends Serializable {
  

  def intersect(qry: G): IndexedSpatialRDD[G,V] = new LiveIntersectionIndexedSpatialRDD(qry, partitioner, rdd)
  
//  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
}

//---------------------------------------------------------------------------------------
// PERSITED INDEX
//---------------------------------------------------------------------------------------

class IndexedSpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    rdd: RDD[RTree[G, (G,V)]]) {
  
  def intersect(qry: G) = new PersistedIndexedIntersectionSpatialRDD(qry, rdd)
  
  def join[V2: ClassTag](other: RDD[(G,V2)]) = new IndexedSpatialJoinRDD(rdd, other)
  
}






























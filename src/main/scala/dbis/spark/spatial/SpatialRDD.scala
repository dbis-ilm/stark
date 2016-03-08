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
import org.apache.spark.Logging
import dbis.spark.spatial.indexed.live.LiveIntersectionIndexedSpatialRDD
import dbis.spark.spatial.indexed.live.IndexedSpatialRDD
import org.apache.spark.rdd.ShuffledRDD
import dbis.spark.spatial.plain.IntersectionSpatialRDD
import dbis.spark.spatial.plain.KNNSpatialRDD
import dbis.spark.spatial.indexed.persistent.PersistedIndexedIntersectionSpatialRDD
import dbis.spark.spatial.indexed.RTree
import dbis.spark.spatial.plain.ContainsSpatialRDD
import dbis.spark.spatial.plain.JoinSpatialRDD
import org.apache.spark.SparkContext
import dbis.spark.spatial.indexed.persistent.IndexedSpatialJoinRDD
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import scala.collection.JavaConverters._

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
  
  def contains(qry: G): ContainsSpatialRDD[G,V] = new ContainsSpatialRDD(qry, this)
  
  def kNN(qry: G, k: Int): KNNSpatialRDD[G,V] = new KNNSpatialRDD(qry, k, this)
  
}

class SpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    rdd: RDD[(G,V)]
  ) extends Logging with Serializable {
  

  def intersect(qry: G): RDD[(G,V)] = new IntersectionSpatialRDD(qry, rdd)
  
  def contains(qry: G): RDD[(G,V)] = new ContainsSpatialRDD(qry, rdd)
  
  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
  
  def liveIndex(ppD: Int): LiveIndexedSpatialRDDFunctions[G,V] = liveIndex(new SpatialGridPartitioner(ppD, rdd))
  
  def liveIndex(partitioner: SpatialPartitioner[G,V]) = new LiveIndexedSpatialRDDFunctions(partitioner, rdd)
  
  def indexFixedGrid(ppD: Int) = makeIdx(grid(ppD))
  
  def index(cost: Double, cellSize: Double) = 
    makeIdx(new ShuffledRDD[G,V,V](rdd, new BSPartitioner(rdd, cellSize, cost)))
//    makeIdx(new ShuffledRDD[G,V,V](rdd, new SpatialGridPartitioner(5, rdd)))
  
  private def makeIdx(rdd: RDD[(G,V)]) = rdd.mapPartitions( iter => { 
  
      val tree = new RTree[G,(G,V)](10)
      
      for((g,v) <- iter) {
        tree.insert(g, (g,v))
      }
      
      List(tree).toIterator
  } , 
  true) // preserve partitioning
  
  def grid(ppD: Int) = new ShuffledRDD[G,V,V](rdd, new SpatialGridPartitioner[G,V](ppD, rdd))
   
  def join[V2 : ClassTag](other: RDD[(G, V2)]) = new JoinSpatialRDD(
      new ShuffledRDD[G,V,V](rdd, new BSPartitioner[G,V](rdd, 10, 10)) , other)
}

class LiveIndexedSpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    partitioner: SpatialPartitioner[G,V], 
    rdd: RDD[(G,V)]
  ) extends Logging with Serializable {
  

  def intersect(qry: G): IndexedSpatialRDD[G,V] = new LiveIntersectionIndexedSpatialRDD(qry, partitioner, rdd)
  
//  def kNN(qry: G, k: Int): RDD[(G,V)] = new KNNSpatialRDD(qry, k, rdd)
}


class IndexedSpatialRDDFunctions[G <: Geometry : ClassTag, V: ClassTag](
    rdd: RDD[RTree[G, (G,V)]]) {
  
  def intersect(qry: G) = new PersistedIndexedIntersectionSpatialRDD(qry, rdd)
  
  def join[V2: ClassTag](other: RDD[(G,V2)]) = new IndexedSpatialJoinRDD(rdd, other)
  
}


object SpatialRDD {
  
  
  implicit def convertSpatialF[G <: Geometry : ClassTag, V: ClassTag](rdd: RDD[(G, V)]): SpatialRDDFunctions[G,V] = new SpatialRDDFunctions[G,V](rdd)
  
  implicit def convertMy[G <: Geometry : ClassTag, V: ClassTag](rdd: RDD[RTree[G,(G,V)]]) = new IndexedSpatialRDDFunctions(rdd)
  
  
}




























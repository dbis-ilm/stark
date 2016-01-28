package dbis.spark.spatial.indexed

import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.OneToOneDependency
import org.apache.spark.Dependency
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import scala.util.Random
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv

class IntersectionIndexedSpatialRDD[G <: Geometry : ClassTag, V: ClassTag](
    qry: G,
    @transient private val prev: RDD[(G,V)]
  ) extends IndexedSpatialRDD(prev) {
  
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(G,V)] = {
    val indexTree = split.asInstanceOf[IndexedSpatialPartition[G,(G,V)]].theIndex 
    
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[G, V, V]]
    val iter = SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(G, V)]]
    
    /*
     * Build our index live on-the-fly
     *  get the iterator for this split and insert it into the index
     */
    iter.foreach{ case (geom, data) => 
      
      println(s" ${split.index}  --> $geom")
      
//      logDebug(s"insert: ${split.index} : $geom -> $data")
      indexTree.insert(geom, (geom,data))}
    
    // now query the index
    val result = indexTree.query(qry)
    
    result
  }
  
}
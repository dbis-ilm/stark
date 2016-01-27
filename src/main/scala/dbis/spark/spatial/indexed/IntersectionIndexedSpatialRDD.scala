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
    println(s"compute indexed + ${split.index}")
//    if(index)
      computeIndexed(split, context)
//    else
//      computeNoIndex(split, context)
  }
  
  private[this] def computeIndexed(split: Partition, context: TaskContext): Iterator[(G,V)] = {
    val indexTree = split.asInstanceOf[IndexedSpatialPartition[G,(G,V)]].theIndex 
    
    /*
     * Build our index live on-the-fly
     *  get the iterator for this split and insert it into the index
     */
    iterator(split, context).foreach{ case (geom, data) => 
      
      logDebug(s"insert: ${split.index} : $geom -> $data")
      indexTree.insert(geom, (geom,data))}
    
    // now query the index
    val result = indexTree.query(qry)
    
    result
  }
  
  private[this] def computeNoIndex(split: Partition, context: TaskContext): Iterator[(G,V)] = {
    iterator(split, context).filter{ case (g,d) => g.intersects(qry)}
  }
  


  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getDependencies: Seq[Dependency[_]] = Seq(new OneToOneDependency(prev))

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient override val partitioner: Option[Partitioner] = None
  
}
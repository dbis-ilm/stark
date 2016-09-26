package dbis.stark.spatial.indexed.live

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner
import dbis.stark.spatial.SpatialPartitioner
import dbis.stark.STObject

abstract class LiveIndexedRDD[G <: STObject : ClassTag, V: ClassTag](
    @transient private val oneParent: RDD[(G,V)] 
  ) extends RDD[(G,V)](oneParent.context, Nil) { //Seq(new ShuffleDependency[K,V,V](oneParent, parti))
      
  override val partitioner = oneParent.partitioner
  
}
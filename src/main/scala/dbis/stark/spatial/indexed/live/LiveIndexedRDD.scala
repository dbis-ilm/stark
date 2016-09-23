package dbis.stark.spatial.indexed.live

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner

abstract class LiveIndexedRDD[K: ClassTag, V: ClassTag](
    @transient private val oneParent: RDD[(K,V)], 
    private val parti: Partitioner
  ) extends RDD[(K,V)](oneParent.context, Seq(new ShuffleDependency[K,V,V](oneParent, parti))) {
      
  override val partitioner: Option[Partitioner] = Some(parti)
  
}
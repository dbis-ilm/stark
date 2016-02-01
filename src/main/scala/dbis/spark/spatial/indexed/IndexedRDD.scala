package dbis.spark.spatial.indexed

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner
import org.apache.spark.ShuffleDependency

abstract class IndexedRDD[K: ClassTag, V: ClassTag](
    @transient private val oneParent: RDD[(K,V)], 
    private val parti: Partitioner
  ) extends RDD[(K,V)](oneParent.context, Seq(new ShuffleDependency[K,V,V](oneParent, parti))) {
      
  @transient override val partitioner: Option[Partitioner] = Some(parti)
  
}
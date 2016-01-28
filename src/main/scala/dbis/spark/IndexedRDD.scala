package dbis.spark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner
import org.apache.spark.NarrowDependency
import org.apache.spark.ShuffleDependency

abstract class IndexedRDD[K: ClassTag, V: ClassTag](
    @transient private val oneParent: RDD[(K,V)], 
    private val parti: Partitioner
  ) extends RDD[(K,V)](oneParent.context, 
      
//      Seq(new OneToOneDependency(oneParent))) {
      
      Seq(new ShuffleDependency[K,V,V](oneParent, parti))) {
      
  @transient override val partitioner: Option[Partitioner] = Some(parti)
  
}
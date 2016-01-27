package dbis.spark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner

abstract class IndexedRDD[K: ClassTag, V: ClassTag](
    oneParent: RDD[(K,V)], 
    parti: Partitioner
  ) extends RDD[(K,V)](oneParent.context, Seq(new ShuffleDependency[K,V,V](oneParent, parti))) {
  

  @transient override val partitioner: Option[Partitioner] = Some(parti)
  
  
}
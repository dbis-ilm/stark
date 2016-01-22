package dbis.spark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency

abstract class IndexedRDD[K: ClassTag, V: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var _deps: Seq[Dependency[(K,V)]]
  ) extends RDD[(K,V)](_sc, _deps) {
  
  def this(oneParent: RDD[(K,V)]) = this(oneParent.context, Seq(new OneToOneDependency(oneParent)))
  
}
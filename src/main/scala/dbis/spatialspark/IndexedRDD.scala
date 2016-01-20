package dbis.spatialspark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.SparkContext

abstract class IndexedRDD[T: ClassTag](
  @transient private var sc: SparkContext
) extends RDD[T](sc, Nil) {
  
}
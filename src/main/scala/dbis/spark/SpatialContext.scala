package dbis.spark

import org.apache.spark.SparkContext
import dbis.spark.spatial.indexed.IndexedSpatialRDD
import com.vividsolutions.jts.geom.Geometry
import scala.reflect.ClassTag

class SpatialContext(sc: SparkContext) {
  
  def loadIndexed[G <: Geometry : ClassTag, V: ClassTag](path: String, f: String => (G,V), parallelism: Int): IndexedSpatialRDD[G,V] = {
    ???
  }
  
}

object SpatialContext {
  
  implicit def createSpatialContext(sc: SparkContext): SpatialContext = new SpatialContext(sc)
  
}
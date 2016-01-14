package dbis.spatialspark

import org.apache.spark.SparkContext
import com.vividsolutions.jts.geom.Point

import ExtendedRDD._

class SpatialRDDTest {
  
  val sc = new SparkContext
  
  val rdd = sc.textFile("", 3)
  val s = rdd.makeSpatial{ x => 3}
  
}
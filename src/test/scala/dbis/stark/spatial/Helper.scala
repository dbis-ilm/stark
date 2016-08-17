package dbis.stark.spatial

import org.apache.spark.SparkContext
import dbis.stark.SpatialObject

object Helper {
  
  def createRDD(sc: SparkContext, file: String = "src/test/resources/new_eventful_flat_1000.csv", sep: Char = ',') = {
    sc.textFile(file, 2)
      .map { line => line.split(sep) }
      .map { arr => (arr(0), arr(1).toInt, arr(2), SpatialObject(arr(7))) }
      .keyBy( _._4)
  } 
  
}
package dbis.stark.spatial

import org.apache.spark.SparkContext
import dbis.stark.SpatialObject
import scala.collection.JavaConverters._


object Helper {
  
  def createRDD(sc: SparkContext, file: String = "src/test/resources/new_eventful_flat_1000.csv", sep: Char = ',', numParts: Int = 1) = {
    sc.textFile(file,numParts)
      .map { line => line.split(sep) }
      .map { arr => (arr(0), arr(1).toInt, arr(2), SpatialObject(arr(7))) }
      .keyBy( _._4)
  } 
  
  
  def rmrf(dir: java.nio.file.Path) {
    if(java.nio.file.Files.exists(dir)) {
      
      if(java.nio.file.Files.isDirectory(dir)) {
        
        val files = java.nio.file.Files.list(dir).collect(java.util.stream.Collectors.toList()).asScala        
        files.foreach { file => rmrf(file) }
      }
      
      java.nio.file.Files.deleteIfExists(dir)
    }
  }
}
package dbis.stark

import java.io.File
import org.apache.spark.SparkContext


object TestUtils {

  case class FileOperationError(msg: String) extends RuntimeException(msg)

  def rmrf(root: String): Unit = rmrf(new File(root))

  def rmrf(root: File): Unit = {
    if (root.isFile) root.delete()
    else if (root.exists) {
      root.listFiles.foreach(rmrf)
      root.delete()
    }
  }

  def rm(file: String): Unit = rm(new File(file))

  def rm(file: File): Unit =
    if (!file.delete) throw FileOperationError(s"Deleting $file failed!")

  def mkdir(path: String): Unit = (new File(path)).mkdirs
  
  def createRDD(sc: SparkContext, file: String = "src/test/resources/new_eventful_flat_1000.csv", sep: Char = ',', numParts: Int = 1) = {
    sc.textFile(file,numParts)
      .map { line => line.split(sep) }
      .map { arr => (arr(0), arr(1).toInt, arr(2), SpatialObject(arr(7))) }
      .keyBy( _._4)
  } 
}
package dbis.stark.raster

import dbis.stark.STObject
import org.apache.spark.rdd.RDD

class RasterRDDFunctions(rdd: RDD[Tile]) {

  def filter(qry: STObject) = new RasterFilterVectorRDD(qry, rdd)

}

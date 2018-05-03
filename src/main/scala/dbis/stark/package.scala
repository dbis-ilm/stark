package dbis.stark

import org.apache.spark.sql.SparkSession

package object raster {

def initRaster(spark: SparkSession): Unit = {
    org.apache.spark.sql.raster.registerUDTs()
}

}
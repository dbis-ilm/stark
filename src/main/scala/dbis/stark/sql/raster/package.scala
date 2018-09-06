package dbis.stark.sql

import org.apache.spark.sql.raster.TileUDT

package object raster {
  def registerUDTs(): Unit = {
    TileUDT
  }
}

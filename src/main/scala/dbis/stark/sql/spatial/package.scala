package dbis.stark.sql

import org.apache.spark.sql.spatial.STObjectUDT

package object spatial {
  def registerUDTs(): Unit = {
    STObjectUDT
  }
}


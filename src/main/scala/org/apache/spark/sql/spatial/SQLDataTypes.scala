package org.apache.spark.sql.spatial

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.types.DataType

/**
  * :: DeveloperApi ::
  * SQL data types for tiles.
  */
@Since("2.0.0")
@DeveloperApi
object SQLDataTypes {

  val STObjectType: DataType = new STObjectUDT
}
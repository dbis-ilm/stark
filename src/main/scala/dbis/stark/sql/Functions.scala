package dbis.stark.sql

import dbis.stark.STObject
import dbis.stark.sql.spatial._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Functions {

  spatial.registerUDTs()
  //raster.registerUDTs()

  val fromWKT = udf(STObject.fromWKT _)

  def register(implicit spark: SparkSession): Unit = {


    spark.sessionState.functionRegistry.createOrReplaceTempFunction("asString", STAsString)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromwkt", STGeomFromWKT)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_point", STPoint)

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_contains", STContains)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_containedby", STContainedBy)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_intersects", STIntersects)

//    spark.udf.register("st_contains", STContains)
//    spark.udf.register("st_containedby", STContainedBy)
//    spark.udf.register("st_intersects", STIntersects)
  }
}

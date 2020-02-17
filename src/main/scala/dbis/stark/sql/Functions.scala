package dbis.stark.sql

import dbis.stark.STObject
import dbis.stark.sql.raster._
import dbis.stark.sql.spatial._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Functions {

  spatial.registerUDTs()
  raster.registerUDTs()

  val fromWKT = udf(STObject.fromWKT _)

  def register(implicit spark: SparkSession): Unit = {


    spark.sessionState.functionRegistry.createOrReplaceTempFunction("asString", STAsString)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromwkt", STGeomFromWKT)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromtile", STGeomFromTile)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_point", STPoint)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_sto", MakeSTObject)


    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_contains", STContains)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_containedby", STContainedBy)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_intersects", STIntersects)

    //Select-Getters
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ulx", GetUlx)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("uly", GetUly)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("width", GetWidth)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("height", GetHeight)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("data", GetData)

    //Raster-Functions
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("histogram", CalcTileHistogram)
    spark.udf.register("rasterHistogram", new CalcRasterHistogram)
  }
}

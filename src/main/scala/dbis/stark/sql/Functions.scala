package dbis.stark.sql

import dbis.stark.STObject
import dbis.stark.spatial.PredicatesFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Functions {

  spatial.registerUDTs()
  raster.registerUDTs()

  val fromWKT = udf(STObject.fromWKT _)

  def register(implicit spark: SparkSession): Unit = {

    spark.udf.register("fromWKT", STObject.fromWKT _)
    spark.udf.register("asString", STObject.asString _)

    spark.udf.register("containedBy", PredicatesFunctions.containedby _)
    spark.udf.register("intersects", PredicatesFunctions.intersects _)
    spark.udf.register("contains", PredicatesFunctions.contains _)

  }
}

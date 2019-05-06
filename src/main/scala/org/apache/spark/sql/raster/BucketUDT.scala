package org.apache.spark.sql.raster

import dbis.stark.raster.{Bucket, Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

@SQLUserDefinedType(udt = classOf[BucketUDT])
class BucketUDT extends UserDefinedType[Bucket[Double]] {
  override def typeName = "bucket"

  override final def sqlType: StructType = _sqlType

  override def serialize(obj: Bucket[Double]): InternalRow = {
    val row = new GenericInternalRow(3)
    row.setInt(0, obj.values)
    row.setDouble(1, obj.lowerBucketBound)
    row.setDouble(2, obj.upperBucketBound)

    row
  }

  override def deserialize(bucket: Any): Bucket[Double] = {
    bucket match {
      case row: InternalRow =>
        require(row.numFields == 3,
          s"BucketUDT.deserialize given row with length ${row.numFields} but requires length == 3")
        val values = row.getInt(0)
        val lower = row.getDouble(1)
        val upper = row.getDouble(2)

        new Bucket[Double](values, lower, upper)
    }
  }

  override def pyUDT: String = "pyspark.stark.raster.BucketUDT"

  override def userClass: Class[Bucket[Double]] = classOf[Bucket[Double]]

  private[spark] override def asNullable: BucketUDT = this

  private[this] val _sqlType = {
    StructType(Seq(
      StructField("values", IntegerType, nullable = true),
      StructField("lowerBounds", DoubleType, nullable = true),
    StructField("upperBounds", DoubleType, nullable = true)))
  }
}

case object BucketUDT extends BucketUDT {
  UDTRegistration.register(classOf[Bucket[Double]].getName, classOf[BucketUDT].getName)
}


package org.apache.spark.sql.raster

import dbis.stark.raster.Tile

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._

@SQLUserDefinedType(udt = classOf[TileUDT])
class TileUDT extends UserDefinedType[Tile[Double]] {
  override def typeName = "tile"

  override final def sqlType: StructType = _sqlType

  override def serialize(obj: Tile[Double]): InternalRow = {
    val row = new GenericInternalRow(6)
    row.setDouble(0, obj.ulx)
    row.setDouble(1, obj.uly)
    row.setInt(2, obj.width)
    row.setInt(3, obj.height)
    row.setDouble(4, obj.pixelWidth)
    row.update(5, UnsafeArrayData.fromPrimitiveArray(obj.data))
    row
  }

  override def deserialize(datum: Any): Tile[Double] = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 6,
          s"TileUDT.deserialize given row with length ${row.numFields} but requires length == 6")
        val ulx = row.getDouble(0)
        val uly = math.round(row.getDouble(1) * 10d) / 10d // fixme: correct?

        val width = row.getInt(2)
        val height = row.getInt(3)
        val pw = row.getDouble(4)
        val data = row.getArray(5).toDoubleArray()//.toByteArray()
        Tile(ulx, uly, width, height, data, pw)
    }
  }

  override def pyUDT: String = "pyspark.stark.raster.TileUDT"

  override def userClass: Class[Tile[Double]] = classOf[Tile[Double]]

  private[spark] override def asNullable: TileUDT = this

  private[this] val _sqlType = {
    StructType(Seq(
      StructField("ulx", DoubleType, nullable = false),
      StructField("uly", DoubleType, nullable = false),
      StructField("width", IntegerType, nullable = false),
      StructField("height", IntegerType, nullable = false),
      StructField("pixelwidth", DoubleType, nullable = false),
      StructField("values", ArrayType(ByteType, containsNull = false), nullable = true)))
  }
}

case object TileUDT extends TileUDT {
  UDTRegistration.register(classOf[Tile[Byte]].getName, classOf[TileUDT].getName)
}

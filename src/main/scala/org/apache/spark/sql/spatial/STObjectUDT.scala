package org.apache.spark.sql.spatial


import dbis.stark.STObject
import dbis.stark.STObject.GeoType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry


@SQLUserDefinedType(udt = classOf[STObjectUDT])
private[sql] class STObjectUDT extends UserDefinedType[STObject] {
  override def typeName = "STObject"

  override final def sqlType: StructType = _sqlType

  override def serialize(obj: STObject): InternalRow = {
    val row = new GenericInternalRow(1)
    row.update(0, obj.getGeo)
    row
  }

  override def deserialize(datum: Any): STObject = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 1,
          s"STObject.deserialize given row with length ${row.numFields} but requires length == 1")
        val geo = row.get(0, BinaryType).asInstanceOf[Geometry]
        new STObject(geo, null)
    }
  }

  override def pyUDT: String = "pyspark.stark.STObjectUDT"

  override def userClass: Class[STObject] = classOf[STObject]

  private[spark] override def asNullable: STObjectUDT = this

  private[this] val _sqlType = {
    StructType(Seq(
      StructField("geo", BinaryType, nullable = false)))
  }
}

case object STObjectUDT extends STObjectUDT {
  println("register STObjectUDT")
  UDTRegistration.register(classOf[STObject].getName, classOf[STObjectUDT].getName)
}
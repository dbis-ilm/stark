package org.apache.spark.sql.spatial


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import dbis.stark.STObject
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._


@SQLUserDefinedType(udt = classOf[STObjectUDT])
private[sql] class STObjectUDT extends UserDefinedType[STObject] {
  override def typeName = "STObjectUDT"

  override def deserialize(datum: Any): STObject = StarkSerializer.deserialize(datum)

  override def serialize(obj: STObject) = StarkSerializer.serialize(obj)

  override def pyUDT: String = "pyspark.stark.STObjectUDT"

  override def userClass: Class[STObject] = classOf[STObject]

  private[spark] override def asNullable: STObjectUDT = this

  private[this] val _sqlType =
//    StructType(Seq(
//      StructField("geo", ArrayType, nullable = false),
    ArrayType(ByteType, containsNull = false)

  override final def sqlType= _sqlType
}

case object STObjectUDT extends STObjectUDT {
  UDTRegistration.register(classOf[STObject].getName, classOf[STObjectUDT].getName)
}

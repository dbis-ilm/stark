package org.apache.spark.sql.spatial


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import dbis.stark.STObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry


@SQLUserDefinedType(udt = classOf[STObjectUDT])
private[sql] class STObjectUDT extends UserDefinedType[STObject] {
  override def typeName = "STObject"

  override def deserialize(datum: Any): STObject = datum match {
    case a: ArrayData =>
      val byis = new ByteArrayInputStream(a.toByteArray())
      val in = new ObjectInputStream(byis)

      val so = in.readObject.asInstanceOf[STObject]
      in.close()
      so
  }

  override def serialize(obj: STObject): GenericArrayData = {

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)

    val arr = new GenericArrayData(baos.toByteArray)
    oos.close()
    arr
  }

//  override def serialize(obj: STObject): InternalRow = {
//    val row = new GenericInternalRow(1)
//    println(s"about to serialize $obj")
//    row.update(0, obj.getGeo)
//    println(s"finished $row")
//    row
//  }
//
//  override def deserialize(datum: Any): STObject = {
//    println("deserialize to STObject")
//    datum match {
//      case row: InternalRow =>
//        require(row.numFields == 1,
//          s"STObject.deserialize given row with length ${row.numFields} but requires length == 1")
//        println("about to read geo as BinaryType")
//        val raw = row.get(0, BinaryType)
//        println(s"about to cast to geometry: $raw")
//        val geo = raw.asInstanceOf[Geometry]
//        println(s"create stobject: $geo")
//        val res = new STObject(geo, None) // TODO (de-)serialize time too
//        res
//      case _ => error(s"something else $datum")
//    }
//  }

  override def pyUDT: String = "pyspark.stark.STObjectUDT"

  override def userClass: Class[STObject] = classOf[STObject]

  private[spark] override def asNullable: STObjectUDT = this

  private[this] val _sqlType = {
//    StructType(Seq(
//      StructField("geo", BinaryType, nullable = false)))
    ArrayType(ByteType, containsNull = false)
  }

  override final def sqlType= _sqlType
}

case object STObjectUDT extends STObjectUDT {
  UDTRegistration.register(classOf[STObject].getName, classOf[STObjectUDT].getName)
}
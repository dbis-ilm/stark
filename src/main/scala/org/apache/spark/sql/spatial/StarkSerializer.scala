package org.apache.spark.sql.spatial

import java.io._

import dbis.stark.STObject
import dbis.stark.raster.RasterUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.raster.TileUDT

/**
  * Helper class providing methods the (de-)serialize STObjects
  *
  */
object StarkSerializer {

  def serialize(obj: STObject): GenericArrayData =  {

    val baos = new ByteArrayOutputStream()
//    var out: ObjectOutput = null
//    var bytes: Array[Byte] = Array.emptyByteArray
//    try {
//      out = new ObjectOutputStream(baos)
//      out.writeObject(obj)
//      out.flush()
//      bytes = baos.toByteArray
//      println(s"out: ${bytes.length}")
//    } finally {
//        baos.close()
////        out.close()
//    }

    try {
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.flush()
    baos.flush()
    val arr = new GenericArrayData(baos.toByteArray)
    oos.close()
    arr
    } catch {
      case e: Throwable =>
        println(s"could NOT serialize: ${e.getMessage}")
        ???
    }
  }

  def deserialize(datum: Any): STObject = datum match {
    case a: ArrayData =>
      val bytes = a.toByteArray()
//      println(s"in: ${bytes.length}")
      val byis = new ByteArrayInputStream(bytes)
      var in: ObjectInput = null
      var so: STObject = null
      try {
        in = new ObjectInputStream(byis)
        so = in.readObject.asInstanceOf[STObject]
      } catch {
        case e: Throwable =>
          println(s"could NOT deserialize: ${e.getMessage}")
          e.printStackTrace()
      } finally{
        if(in != null)
          in.close()
      }

      so
    case t:TileUDT =>
      STObject(RasterUtils.tileToGeo(t.deserialize(t)))
    case ur: UnsafeRow =>
      val t = new org.apache.spark.sql.raster.TileUDT()
      val newTile = t.deserialize(ur)
      STObject(newTile.wkt)
  }

//  private val kryo = new Kryo()
//  kryo.register(classOf[STObject])
//
//  import org.locationtech.jts.geom.GeometryCollection
//  import org.locationtech.jts.geom.LineString
//  import org.locationtech.jts.geom.MultiLineString
//  import org.locationtech.jts.geom.MultiPoint
//  import org.locationtech.jts.geom.MultiPolygon
//
//  kryo.register(classOf[Point])
//  kryo.register(classOf[LineString])
//  kryo.register(classOf[Polygon])
//  kryo.register(classOf[MultiPoint])
//  kryo.register(classOf[MultiLineString])
//  kryo.register(classOf[MultiPolygon])
//  kryo.register(classOf[GeometryCollection])
//  kryo.register(classOf[Envelope])
//
//  def serialize(obj: STObject): GenericArrayData =  {
//
//    val baos = new ByteArrayOutputStream()
////    val kryo = new Kryo()
//    val output = new Output(baos)
//    kryo.writeObject(output, obj)
//    output.close()
//    new GenericArrayData(baos.toByteArray)
//  }
//
//  def deserialize(datum: Any): STObject = datum match {
//    case a: ArrayData =>
//      val byis = new ByteArrayInputStream(a.toByteArray())
////      val kryo = new Kryo()
//      val input = new Input(byis)
//      val obj = kryo.readObject(input, classOf[STObject])
//      input.close()
//      obj
//  }




//  def serialize(obj: STObject): InternalRow = {
//    val row = new GenericInternalRow(2)
//    row.update(0, obj.getGeo)
//    row.update(1, obj.time)
//    row
//  }
//
//  def deserialize(datum: Any): STObject = {
//    datum match {
//      case row: InternalRow =>
//        require(row.numFields == 2,
//          s"STObject.deserialize given row with length ${row.numFields} but requires length == 1")
//        val raw = row.get(0, BinaryType)
//        val geo = raw.asInstanceOf[Geometry]
//        val time = row.get(1, BinaryType).asInstanceOf[Option[TemporalExpression]]
//        val res = new STObject(geo, time) // TODO (de-)serialize time too
//        res
//      case _ => error(s"something else $datum")
//    }
//  }

}

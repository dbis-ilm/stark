package dbis.stark.sql.spatial

import java.time.{LocalDate, ZoneId}

import dbis.stark.STObject
import dbis.stark.raster.RasterUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.raster.TileUDT
import org.apache.spark.sql.spatial.STObjectUDT
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

abstract class STConstructor(exprs: Seq[Expression])
  extends Expression
    with CodegenFallback {

  override def nullable = false
  override def dataType: DataType = STObjectUDT
  override def children = exprs
}

case class STGeomFromWKT(exprs: Seq[Expression]) extends Expression
  with CodegenFallback {
  require(exprs.length == 1, s"Only one expression allowed for STGeomFromText, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val evaled = exprs.head.eval(input)

    if(evaled == null) {

      sys.error(s"shit. ${input.getClass.getCanonicalName}${input.toString}  expr: ${exprs.mkString(";")}  ${exprs.head.prettyName}  cols: ${input.numFields} ")
    }

    val inputString = evaled.asInstanceOf[UTF8String].toString
    val theObject = STObject(inputString)

//    new GenericArrayData(STObjectUDT.serialize(theObject).toByteArray())
    STObjectUDT.serialize(theObject)
  }

  override def nullable = false
  override def dataType: DataType = STObjectUDT
  override def children = exprs
}

case class STGeomFromTile(exprs: Seq[Expression]) extends Expression
  with CodegenFallback {
  require(exprs.length == 1, s"Only one expression allowed for STGeomFromTile, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val evaled = exprs.head.eval(input)
    val tile = TileUDT.deserialize(evaled)

    val theObject = RasterUtils.tileToGeo(tile)

    STObjectUDT.serialize(theObject)
  }

  override def nullable = false
  override def dataType: DataType = STObjectUDT
  override def children = exprs
}



case class STPoint(private val exprs: Seq[Expression]) extends Expression with CodegenFallback  { //STConstructor(exprs)
  require(exprs.length == 2 || exprs.length == 3, s"Exactly two or three expressions allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    if(exprs == null)
      sys.error("expression is null")

    if(exprs.head == null)
      sys.error("expr.head is null")

    if(exprs.head.eval(input) == null)
      sys.error(s"evaled value is null for ${exprs.head}")
    val x = exprs.head.eval(input).asInstanceOf[Decimal].toDouble
    val y = exprs(1).eval(input).asInstanceOf[Decimal].toDouble

    val z = if(exprs.length == 3) Some(exprs(2).eval(input).asInstanceOf[Decimal].toDouble) else None

    val theObject = z.map(STObject(x,y,_)).getOrElse(STObject(x,y))

//    new GenericArrayData(STObjectUDT.serialize(theObject).toByteArray())
    STObjectUDT.serialize(theObject)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = STObjectUDT

  override def children: Seq[Expression] = exprs
}

case class MakeSTObject(private val exprs: Seq[Expression]) extends Expression with CodegenFallback { //STConstructor(exprs)
  override def nullable: Boolean = false
  override def dataType: DataType = STObjectUDT
  override def children: Seq[Expression] = exprs

  override def eval(input: InternalRow): Any = {
    val wkt = exprs.head.eval(input)

    if(wkt == null) {
      sys.error(s"shit. ${input.getClass.getCanonicalName}${input.toString}  expr: ${exprs.mkString(";")}  ${exprs.head.prettyName}  cols: ${input.numFields} ")
    }



    val wktString = wkt.asInstanceOf[UTF8String].toString

    val date: Option[Long] = if(exprs.length == 2) {
      val dateEvaled = exprs.last.eval(input)
      if(dateEvaled == null) {
        sys.error(s"dunno.... date is null: ${input.getClass.getCanonicalName}${input.toString}  expr: ${exprs.mkString(";")}  ${exprs.head.prettyName}  cols: ${input.numFields} ")
      }

      val daysSinceEpoch = dateEvaled.asInstanceOf[java.lang.Integer]
      Some(LocalDate.ofEpochDay(daysSinceEpoch.longValue()).atStartOfDay(ZoneId.of("UTC")).toEpochSecond)
    } else if (exprs.length == 4) {


      def convert(i: Int): Integer = {
        val evaled = exprs(i).eval(input)
        evaled match {
          case i: java.lang.Integer => i
          case s: UTF8String => s.toString.toInt
        }
      }

      val year = convert(1)
      val month = convert(2)
      val day = convert(3)

      Some(LocalDate.of(year, month, day).atStartOfDay(ZoneId.of("UTC")).toEpochSecond)
    } else
      None

    val theObject = date match {
      case None => STObject(wktString)
      case Some(l) => STObject(wktString, l)
    }

    STObjectUDT.serialize(theObject)
  }
}
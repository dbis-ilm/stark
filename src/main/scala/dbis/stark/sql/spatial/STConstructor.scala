package dbis.stark.sql.spatial

import dbis.stark.STObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
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

case class STGeomFromWKT(exprs: Seq[Expression]) extends STConstructor(exprs)
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


case class STPoint(private val exprs: Seq[Expression]) extends STConstructor(exprs) {
  require(exprs.length == 2 || exprs.length == 3, s"Exactly two or three expressions allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val x = exprs.head.eval(input).asInstanceOf[Decimal].toDouble
    val y = exprs(1).eval(input).asInstanceOf[Decimal].toDouble

    val z = if(exprs.length == 3) Some(exprs(2).eval(input).asInstanceOf[Decimal].toDouble) else None

    val theObject = z.map(STObject(x,y,_)).getOrElse(STObject(x,y))

//    new GenericArrayData(STObjectUDT.serialize(theObject).toByteArray())
    STObjectUDT.serialize(theObject)
  }
}
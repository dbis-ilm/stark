package dbis.stark.sql.spatial

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.spatial.STObjectUDT
import org.apache.spark.sql.types.StringType

abstract class STFunction(exprs: Seq[Expression])
  extends Expression
    with CodegenFallback{

  override def nullable = false
  override def children = exprs

  def first = exprs.head
}

case class STAsString(exprs: Seq[Expression]) extends STFunction(exprs) {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val bytes = first.eval(input).asInstanceOf[ArrayData]
    new GenericArrayData(STObjectUDT.deserialize(bytes).toString)
  }

  override def dataType = StringType
}
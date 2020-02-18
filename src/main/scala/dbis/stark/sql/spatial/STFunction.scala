package dbis.stark.sql.spatial

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.raster.TileUDT
import org.apache.spark.sql.spatial.STObjectUDT
import org.apache.spark.sql.types.{DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

abstract class STFunction(exprs: Seq[Expression])
  extends Expression
    with CodegenFallback{

  override def nullable = false
  override def children = exprs

  def first = exprs.head
}

case class STAsWKT(exprs: Seq[Expression]) extends Expression
  with CodegenFallback{

  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def nullable = false
  override def children = exprs

  def first = exprs.head

  override def eval(input: InternalRow) = {
    val so = STObjectUDT.deserialize(first.eval(input))
    UTF8String.fromString(so.wkt)
//    new GenericArrayData(STObjectUDT.deserialize(bytes).toString)
  }

  override def dataType = StringType
}

case class TileMax(exprs: Seq[Expression]) extends Expression with CodegenFallback {


  override def nullable: Boolean = false
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = exprs

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(exprs.head.eval(input))
    if(tile.getSMA.isEmpty)
      tile.computeSMA()

    tile.getSMA.get.max
  }
}

case class TileMin(exprs: Seq[Expression]) extends Expression with CodegenFallback {


  override def nullable: Boolean = false
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = exprs

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(exprs.head.eval(input))
    if(tile.getSMA.isEmpty)
      tile.computeSMA()

    tile.getSMA.get.min
  }
}

case class TileMedian(exprs: Seq[Expression]) extends Expression with CodegenFallback {


  override def nullable: Boolean = false
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = exprs

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(exprs.head.eval(input))
    if(tile.getSMA.isEmpty)
      tile.computeSMA()

    tile.getSMA.get.median
  }
}
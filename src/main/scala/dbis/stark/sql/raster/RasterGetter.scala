package dbis.stark.sql.raster

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.raster.TileUDT
import org.apache.spark.sql.types._

abstract class RasterGetter
  extends Expression
    with CodegenFallback{

  def child: Expression

  override def children = Seq(child)

  override def nullable = false
}

case class GetUlx(exprs: Seq[Expression]) extends RasterGetter {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.ulx
  }

  override def child = exprs(0)
  override def dataType = DoubleType
}

case class GetUly(exprs: Seq[Expression]) extends RasterGetter {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.uly
  }

  override def child = exprs(0)
  override def dataType = DoubleType
}

case class GetWidth(exprs: Seq[Expression]) extends RasterGetter {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.width
  }

  override def child = exprs(0)
  override def dataType = IntegerType
}

case class GetHeight(exprs: Seq[Expression]) extends RasterGetter {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.height
  }

  override def child = exprs(0)
  override def dataType = IntegerType
}

case class GetData(exprs: Seq[Expression]) extends RasterGetter {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(child.eval(input))
    new GenericArrayData(tile.data)
  }

  override def child = exprs(0)
  override def dataType: DataType = ArrayType(ByteType)
}

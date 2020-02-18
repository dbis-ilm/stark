package dbis.stark.sql.raster

import dbis.stark.raster.{Bucket, RasterUtils, Tile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.raster._
import org.apache.spark.sql.types._

abstract class RasterFunction extends Expression
  with CodegenFallback{

  override def nullable = false
}

case class CalcTileHistogram(exprs: Seq[Expression]) extends RasterFunction {
  require(exprs.length == 1 || exprs.length == 2, s"One or two expressions allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow) = {
    val tile = TileUDT.deserialize(children.head.eval(input))
    var bucketCount = 0
    if(children.length > 1) {
      bucketCount = children(1).eval(input).asInstanceOf[Int]
    }

    val histo = tile.histogram(bucketCount).map(bucket => BucketUDT.serialize(bucket))
    new GenericArrayData(histo)
  }

  override def children = exprs
  override def dataType = ArrayType(BucketUDT)
}

case class CalcRasterHistogram() extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("tile", TileUDT) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
      StructField("buckets", ArrayType(BucketUDT)) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = ArrayType(BucketUDT)

  override def deterministic: Boolean = true

  private val bucketCount = 10

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null : Bucket[Double]
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val tile = input.getAs[Tile[Double]](0)
    val histo = tile.histogram(bucketCount)

    if(buffer(0) == null) buffer(0) = histo
    else buffer(0) = RasterUtils.combineHistograms[Double](buffer.getAs[Seq[Bucket[Double]]](0), histo)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer1.getAs[Seq[Int]](0) == null) buffer1(0) = buffer2.getAs[Seq[Int]](0)
    else if(buffer2.getAs[Seq[Int]](0) != null) buffer1(0) = RasterUtils.combineHistograms(buffer1.getAs[Seq[Bucket[Double]]](0), buffer2.getAs[Seq[Bucket[Double]]](0))
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Seq[BucketUDT]](0)
  }
}

case class HistogramValue(exprs: Seq[Expression]) extends RasterFunction {
  override def eval(input: InternalRow): Int = {
    val bucket = BucketUDT.deserialize(exprs.head.eval(input))
    bucket.values
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = exprs
}

case class HistogramLowerBounds(exprs: Seq[Expression]) extends  RasterFunction {
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = exprs

  override def eval(input: InternalRow): Double = {
    val bucket = BucketUDT.deserialize(exprs.head.eval(input))
    bucket.lowerBucketBound
  }
}

case class HistogramUpperBounds(exprs: Seq[Expression]) extends  RasterFunction {
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = exprs

  override def eval(input: InternalRow): Double = {
    val bucket = BucketUDT.deserialize(exprs.head.eval(input))
    bucket.upperBucketBound
  }
}
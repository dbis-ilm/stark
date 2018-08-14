package dbis.stark.sql.spatial

import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BindReferences, Expression, Predicate}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.spatial.{STObjectUDT, StarkSerializer}


trait STPredicate
  extends Expression
    with Predicate
    with CodegenFallback {
  override def nullable = false

  override def eval(input: InternalRow) = doEval(input, predicate)

  def left: Expression
  def right: Expression

  override def children = Seq(left, right)

  def doEval(input: InternalRow, predicate: JoinPredicate): Boolean = {

    val leftBytes = left.eval(input) //.asInstanceOf[ArrayData]
    val rightBytes = right.eval(input) //.asInstanceOf[ArrayData]

    val leftObj = StarkSerializer.deserialize(leftBytes)
    val rightObj = StarkSerializer.deserialize(rightBytes)

    val res = JoinPredicate.predicateFunction(predicate)(leftObj, rightObj)

//    println(s"${leftObj.getGeo} $predicate ${rightObj.getGeo} --> $res")

    res
  }

  def predicate: JoinPredicate
}

case class STContains(exprs: Seq[Expression]) extends STPredicate {
  require(exprs.length == 2, s"provided number of expressions must be exactly 2 (given: ${exprs.length}")
  def this(left: Expression, right: Expression) = this(Seq(left,right))

  override def left = exprs.head
  override def right = exprs.last

  override def predicate = JoinPredicate.CONTAINS
}

case class STIntersects(exprs: Seq[Expression]) extends STPredicate {
  require(exprs.length == 2, s"provided number of expressions must be exactly 2 (given: ${exprs.length}")
  def this(left: Expression, right: Expression) = this(Seq(left,right))

  override def left = exprs.head
  override def right = exprs.last

  override def predicate = JoinPredicate.INTERSECTS
}

case class STContainedBy(exprs: Seq[Expression]) extends STPredicate {
  require(exprs.length == 2, s"provided number of expressions must be exactly 2 (given: ${exprs.length}")
  def this(left: Expression, right: Expression) = this(Seq(left,right))

  override def left = exprs.head
  override def right = exprs.last

  override def predicate = JoinPredicate.CONTAINEDBY
}


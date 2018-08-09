package dbis.stark.sql.spatial

import dbis.stark.STObject
import dbis.stark.spatial.SpatialJoinRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.spatial.{STObjectUDT, StarkSerializer}

/**
  * Spatio-temporal join execution
  * @param left The left input plan
  * @param right The right input plan
  * @param pred Predicate information
  */
case class STJoinExec(left: SparkPlan,
                      right: SparkPlan,
                      pred: STPredicate
                ) extends BinaryExecNode {

  /**
    * Output schema
    * @return The attributes in the output schema
    */
  override def output = left.output ++ right.output

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execution spatial join with ${pred.predicate}")


    val leftRDD = makeSpatialRDD(left, pred.left)
    val rightRDD = makeSpatialRDD(right, pred.right)

    val leftSchema = left.schema
    val rightSchema = right.schema


    // TODO: partitioning and index
    // perform the actual Spatio-temporal join using STARK
    val joined = new SpatialJoinRDD(leftRDD, rightRDD, pred.predicate, indexConfig = None)
//    var i=0
//    joined.collect().foreach { case (leftRow, rightRow) =>
//      println(s"left fields: ${leftRow.numFields}")
//      println(s"right fields: ${rightRow.numFields}")
//      println(STObjectUDT.deserialize(leftRow.get(1, STObjectUDT).asInstanceOf[ArrayData]).getGeo)
//      println(STObjectUDT.deserialize(rightRow.get(1, STObjectUDT).asInstanceOf[ArrayData]).getGeo)
//      println(s"$i ---------")
//        i+=1
//    }

    // the join result schema is a tuple with two elements
    joined.map{ case (leftRow, rightRow) =>

//      println(STObjectUDT.deserialize(leftRow.get(1, STObjectUDT).asInstanceOf[ArrayData]).getGeo)
//      println(STObjectUDT.deserialize(rightRow.get(1, STObjectUDT).asInstanceOf[ArrayData]).getGeo)
      val result = InternalRow.fromSeq(leftRow.toSeq(leftSchema) ++ rightRow.toSeq(rightSchema))
//      println(s"result row: $result")
      result
    }
  }

  /**
    * Helper method to compute the SpatialRDD from the given input plans
    * @param plan The plan
    * @param expr The expression/column used for join
    * @return Returns an RDD[(STObject, InternalRow)] The STObject is resolved from the given expression
    */
  private def makeSpatialRDD(plan: SparkPlan, expr: Expression): RDD[(STObject, InternalRow)] =
    plan.execute().map { row =>

      val ref = BindReferences.bindReference(expr, plan.output)
      val evaled = ref.eval(row)

      val joinColumn = StarkSerializer.deserialize(evaled.asInstanceOf[ArrayData])

//      println(s"$joinColumn  -- $expr")

      (joinColumn, row)
    }
}


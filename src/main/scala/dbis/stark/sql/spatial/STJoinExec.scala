package dbis.stark.sql.spatial

import dbis.stark.STObject
import dbis.stark.spatial.SpatialJoinRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.spatial.StarkSerializer

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
//    logInfo(s"execution spatial join with ${pred.predicate}")


    val leftRDD = STJoinExec.makeSpatialRDD(left, pred.left)
    val rightRDD = STJoinExec.makeSpatialRDD(right, pred.right)

    val index = None //Some(dbis.stark.spatial.indexed.RTreeConfig(order = 5))

//    // TODO: partitioning and index
//    // perform the actual Spatio-temporal join using STARK
    val joined = new SpatialJoinRDD(leftRDD, rightRDD, pred.predicate, indexConfig = index)
    joined.map{ case (leftRow, rightRow) =>
      InternalRow.fromSeq(leftRow.toSeq(left.schema) ++ rightRow.toSeq(right.schema))
    }
  }
}

object STJoinExec {
  /**
    * Helper method to compute the SpatialRDD from the given input plans
    * @param plan The plan
    * @param expr The expression/column used for join
    * @return Returns an RDD[(STObject, InternalRow)] The STObject is resolved from the given expression
    */
  protected def makeSpatialRDD(plan: SparkPlan, expr: Expression): RDD[(STObject, InternalRow)] = {

    val ref = BindReferences.bindReference(expr, plan.output)

    plan.execute().map { row =>

      val evaled = ref.eval(row)
      val joinColumn = StarkSerializer.deserialize(evaled.asInstanceOf[ArrayData])

//      println(s"$prefix AFTER ROW: ${row.getString(0)} , \t${row.getLong(1)} \t${
//        val arr = row.get(2, STObjectUDT).asInstanceOf[ArrayData]
//        StarkSerializer.deserialize(arr).toString
//      }")

      (joinColumn, row) //.toSeq(plan.schema)
    }
  }

}

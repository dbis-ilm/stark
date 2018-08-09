package dbis.stark.sql.spatial

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan

object STJoinStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // we only want to match INNER Joins here
    case Join(left, right, Inner, Some(op)) =>
      // and the Join Op must be a spatio-temporal STARK predicate
      op match {
        case _: STIntersects | _: STContainedBy | _: STContains =>
          val leftPlan = planLater(left)
          val rightPlan = planLater(right)

          Seq(STJoinExec(leftPlan, rightPlan, op.asInstanceOf[STPredicate]))

        // if some other predicate, it's probably a normal join - ignore here
        case _ => Nil
      }

    // not a (inner) join or no predicate given
    case _ =>
      Nil
  }
}
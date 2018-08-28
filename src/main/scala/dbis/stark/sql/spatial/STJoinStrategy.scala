package dbis.stark.sql.spatial

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan

object STJoinStrategy extends Strategy {

  private def findInPlan(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.forall(plan.outputSet.contains(_))

  /**
    * Check to which of the two given subplans the two expressions belong
    *
    * Takes the "left" and "right" expressions from the given predicate and returns a tuple of two plans:
    * The first plan in this tuple is referenced by the [[STPredicate.left]] expression, whereas the second
    * plan in the tuple is referenced by [[STPredicate.right]].
    * The result tuple is empty [[None]] if at least one expression could not be found in the plans
    *
    *
    * One would think that this is automatically resolved by Spark's typesystem. But unfortunately, if we omit
    * this check, a query `FROM left, right WHERE st_intersects(right.geo2, left.geo1)` would fail as it cannot find
    * `geo1` in `right` and `geo2` in relation `left`
    *
    * @param plan1 The left sub-plan
    * @param plan2 The right sub-plan
    * @param predicate The spatial predicate used to join
    * @return The plans in order (left,right) according to the left and right predicate expressions, or None if not found
    */
  private def findExprInSubPlan(plan1: LogicalPlan, plan2: LogicalPlan, predicate: STPredicate): Option[(LogicalPlan, LogicalPlan)] = {

    val expr1 = predicate.left
    val expr2 = predicate.right

    if(findInPlan(expr1, plan1) && findInPlan(expr2, plan2))
      Some((plan1, plan2))
    else if(findInPlan(expr1, plan2) && findInPlan(expr2, plan1))
      Some((plan2, plan1))
    else
      None

  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // we only want to match INNER Joins here
    case Join(left, right, Inner, Some(op)) =>
      // and the Join Op must be a spatio-temporal STARK predicate
      op match {
        case pred: STPredicate =>

          findExprInSubPlan(left, right, pred) match {
            case Some((l,r)) =>
              val leftPlan = planLater(l)
              val rightPlan = planLater(r)

              STJoinExec(leftPlan, rightPlan, pred) :: Nil

            case None => Nil // expressions do not match referenced plans - not our spatial join
          }



        // if some other predicate, it's probably a normal join - ignore here
        case _ => Nil
      }

    // not a (inner) join or no predicate given
    case _ =>
      Nil
  }
}
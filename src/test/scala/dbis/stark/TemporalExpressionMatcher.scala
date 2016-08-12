package dbis.stark

import org.scalatest.matchers.Matcher
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.BePropertyMatcher
import org.scalatest.matchers.BePropertyMatchResult

trait TemporalExpressionMatchers {

	class LTMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left < right,
			s"not < $right"
		)
	}

	
	class GTMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left > right,
			s"not > $right"
		)
	}
	
	class LEQMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left <= right,
			s"not <= $right"
		)
	}
	
	class GEQMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left >= right,
			s"not >= $right"
		)
	}


	def lt(t: TemporalExpression) = new LTMatcher(t)
	def gt(t: TemporalExpression) = new GTMatcher(t)
	def leq(t: TemporalExpression) = new LEQMatcher(t)
	def geq(t: TemporalExpression) = new GEQMatcher(t)
	
	
	class IntersectsMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
	  def apply(left: TemporalExpression) = BePropertyMatchResult(
	    left.intersects(right),
	    s"intersected with $right"
	  )
	}
	
	class ContainsMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
	  def apply(left: TemporalExpression) = BePropertyMatchResult(
	    left.contains(right),
	    s"containing $right"
	  )
	}
	
	class ContainedByMatcher(right: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
	  def apply(left: TemporalExpression) = BePropertyMatchResult(
	    left.containedBy(right),
	    s"contained by $right"
	  )
	}
	
	def contains(t: TemporalExpression) = new ContainsMatcher(t)
	def containedBy(t: TemporalExpression) = new ContainedByMatcher(t)
	def intersects(t: TemporalExpression) = new IntersectsMatcher(t)
	
}

object TemporalExpressionMatchers extends TemporalExpressionMatchers
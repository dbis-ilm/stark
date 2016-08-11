package dbis.spark

import org.scalatest.matchers.Matcher
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.BePropertyMatcher
import org.scalatest.matchers.BePropertyMatchResult

trait TemporalExpressionMatchers {

	class LTMatcher(expected: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left < expected,
			s"not < $expected"
		)
	}

	
	class GTMatcher(expected: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left > expected,
			s"not > $expected"
		)
	}
	
	class LEQMatcher(expected: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left <= expected,
			s"not <= $expected"
		)
	}
	
	class GEQMatcher(expected: TemporalExpression) extends BePropertyMatcher[TemporalExpression] {
		def apply(left: TemporalExpression) = BePropertyMatchResult(
			left >= expected,
			s"not >= $expected"
		)
	}


	def lt(t: TemporalExpression) = new LTMatcher(t)
	def gt(t: TemporalExpression) = new GTMatcher(t)
	def leq(t: TemporalExpression) = new LEQMatcher(t)
	def geq(t: TemporalExpression) = new GEQMatcher(t)
	
}

object TemporalExpressionMatchers extends TemporalExpressionMatchers
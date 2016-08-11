package dbis.stark

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import TemporalExpressionMatchers._

class IntervalTest extends FlatSpec with Matchers {
  
  "An interval" should "have the correct < relation" in {
    
    Interval(1,2) shouldBe lt(Interval(2,3))
    Interval(1,2) shouldBe lt(Interval(3,4))
    Interval(1,3) should not be lt(Interval(2,3))
    Interval(1,4) shouldBe lt(Interval(6,9))
    Interval(1,2) should not be lt(Interval(1,2))
    Interval(4,6) should not be lt(Interval(0,1))
    Interval(Instant(3),None) should not be lt(Interval(Long.MinValue, Long.MaxValue))
    Interval(3,7) shouldBe lt(Interval(Instant(9), None))
  }
  
  it should "have the correct <= relation" in {
    
    Interval(1,2) shouldBe leq(Interval(2,3))
    Interval(1,2) shouldBe leq(Interval(3,4))
    Interval(1,3) shouldBe leq(Interval(2,3))
    Interval(1,4) shouldBe leq(Interval(6,9))
    Interval(1,2) shouldBe leq(Interval(1,2))
    Interval(4,6) should not be leq(Interval(0,1))
    Interval(Instant(3),None) should not be leq(Interval(Long.MinValue, Long.MaxValue))
    Interval(3,7) shouldBe leq(Interval(Instant(9), None))
  }
  
  it should "be comparable to instants" in {
    Instant(1) shouldBe lt(Interval(2,3))
    Instant(1) shouldBe leq(Interval(2,3))
    
    Interval(2,3) should not be lt(Instant(1))
    Interval(2,3) should not be leq(Instant(1))
    Interval(2,3) shouldBe gt(Instant(1))
    
  }
  
}

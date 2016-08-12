package dbis.stark

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import TemporalExpressionMatchers._
import org.scalatest.matchers.BePropertyMatcher

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
  
  //############################################################################
  
  it should "correctly contain a point" in {
    
    Interval(1, 10) shouldBe contains(Instant(4))
    Interval(1, 10) shouldBe contains(Instant(1))
    Interval(1, 10) shouldBe contains(Instant(10))
    
  }
  
  it should "not contain an outside point" in {
    Interval(1, 10) should not be contains(Instant(0))
    Interval(1, 10) should not be contains(Instant(11))
    Interval(1, 10) should not be contains(Instant(100))
  }
  
  it should "contain itself" in {
    Interval(1,5) shouldBe contains(Interval(1,5))
  }
  
  it should "correctly contain another interval" in {
    
    Interval(13, 34) shouldBe contains(Interval(13,33))
    Interval(13, 34) shouldBe contains(Interval(14,34))
    Interval(13, 34) shouldBe contains(Interval(20,29))
  }
  
  it should "not contain outside intervals" in {
    Interval(14,54) should not be contains(Interval(3,6))
    Interval(14,54) should not be contains(Interval(3,14))
    Interval(14,54) should not be contains(Interval(3, 15))
    Interval(14,54) should not be contains(Interval(3,54))
    Interval(14,54) should not be contains(Interval(3,100))
    Interval(14,54) should not be contains(Interval(54,78))
    Interval(14,54) should not be contains(Interval(50, 78))
    Interval(14,54) should not be contains(Interval(55, 78))
  }
  
  //############################################################################
  
  it should "correctly intersect a point" in {
    
    Interval(1, 10) shouldBe intersects(Instant(4))
    Interval(1, 10) shouldBe intersects(Instant(1))
    Interval(1, 10) shouldBe intersects(Instant(10))
    
  }
  
  it should "not intersect an outside point" in {
    Interval(1, 10) should not be intersects(Instant(0))
    Interval(1, 10) should not be intersects(Instant(11))
    Interval(1, 10) should not be intersects(Instant(100))
  }
  
  it should "intersect itself" in {
    Interval(1,5) shouldBe intersects(Interval(1,5))
  }
  
  it should "correctly intersect another interval" in {
    
    Interval(13, 34) shouldBe intersects(Interval(13,33))
    Interval(13, 34) shouldBe intersects(Interval(14,34))
    Interval(13, 34) shouldBe intersects(Interval(20,29))
    Interval(14,54) shouldBe intersects(Interval(3,14))
    Interval(14,54) shouldBe intersects(Interval(3, 15))
    Interval(14,54) shouldBe intersects(Interval(3,54))
    Interval(14,54) shouldBe intersects(Interval(3,100))
    Interval(14,54) shouldBe intersects(Interval(54,78))
    Interval(14,54) shouldBe intersects(Interval(50, 78))
  }
  
  it should "not intersect outside intervals" in {
    Interval(14,54) should not be intersects(Interval(3,6))
    Interval(14,54) should not be intersects(Interval(55, 78))
  }
  
  "An interval with no end" should "correctly intersect with another point" in {
    Interval(Instant(34), None) should not be intersects(Instant(33))
    Interval(Instant(34), None) shouldBe intersects(Instant(34))
    Interval(Instant(34), None) shouldBe intersects(Instant(45))
    Interval(Instant(34), None) shouldBe intersects(Instant(Long.MaxValue))
  }  
  
  it should "correctly intersect with other intervals" in {
    Interval(Instant(34), None) shouldBe intersects(Interval(Instant(34), None))
    Interval(Instant(34), None) shouldBe intersects(Interval(Instant(22), None))
    Interval(Instant(34), None) shouldBe intersects(Interval(22, 43))
    Interval(Instant(34), None) shouldBe intersects(Interval(43, 56))
    Interval(34, 43) shouldBe intersects(Interval(Instant(22), None))
    Interval(34, 43) shouldBe intersects(Interval(Instant(43), None))
  }
  
  it should "correctly contain another point" in {
    Interval(Instant(34), None) should not be contains(Instant(33))
    Interval(Instant(34), None) shouldBe contains(Instant(34))
    Interval(Instant(34), None) shouldBe contains(Instant(45))
    Interval(Instant(34), None) shouldBe contains(Instant(Long.MaxValue))
  }
  
  it should "correctly contain other intervals" in {
    Interval(Instant(34), None) should not be contains(Interval(Instant(34), None))
    Interval(Instant(34), None) should not be contains(Interval(Instant(22), None))
    Interval(Instant(34), None) should not be contains(Interval(22, 43))
    Interval(Instant(34), None) shouldBe contains(Interval(43, 56))
    Interval(34, 43) should not be contains(Interval(Instant(22), None))
    Interval(34, 43) should not be contains(Interval(Instant(43), None))
  }
  
  
}


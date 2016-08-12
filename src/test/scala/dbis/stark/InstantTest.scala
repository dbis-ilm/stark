package dbis.stark

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import TemporalExpressionMatchers._
import InstantTest._
  
class InstantTest extends FlatSpec with Matchers {

  "An Instant" should "have the correct < relation" in {
    
    i0 should not be lt(i0)
    i0 shouldBe lt(i1)
    i0 shouldBe lt(i2)
    i0 shouldBe lt(i22)
    i2 should not be lt(i1)
    i22 should not be lt(i2)
  }
  
  it should "have the correct <= relation" in {
    
    i0 shouldBe leq(i0)
    i0 shouldBe leq(i1)
    i0 shouldBe leq(i2)
    i0 shouldBe leq(i22)
    i2 should not be leq(i1)
    i22 shouldBe leq(i2)
  }
  
  it should "have the correct > relation" in {
    
    i0 should not be gt(i0)
    i0 should not be gt(i1)
    i0 should not be gt(i2)
    i0 should not be gt(i22)
    i2 shouldBe gt(i0)
    i2 shouldBe gt(i1)
    i22 should not be gt(i2)
  }
  
  it should "have the correct >= relation" in {
    
    i0 shouldBe geq(i0)
    i0 should not be geq(i1)
    i0 should not be geq(i2)
    i0 should not be geq(i22)
    i2 shouldBe geq(i0)
    i2 shouldBe geq(i1)
    i22 shouldBe geq(i2)
  }
  
  it should "find the correct maximum instant" in {
    
    withClue("max i0 i0") { Instant.max(i0, i0) shouldBe i0 }
    withClue("max i2 i22") { Instant.max(i2, i22) shouldBe Instant(i2) }
    withClue("max i0 i1") { Instant.max(i0, i1) shouldBe i1 }
    withClue("max i1 i0") { Instant.max(i1, i0) shouldBe i1 }
    withClue("max i0 o1") { Instant.max(Some(i0), o1) shouldBe o1 }
    withClue("max o1 i0") { Instant.max(o1, Some(i0)) shouldBe o1 }
    withClue("max i0 none") { Instant.max(Some(i0), none) shouldBe Some(i0) }
    withClue("max none i0") { Instant.max(none, Some(i0)) shouldBe Some(i0) }
    withClue("max None None") { Instant.max(None, None) shouldBe None }
  }
  
  it should "find the correct minimum instant" in {
    
    withClue("min i0 i0") { Instant.min(i0, i0) shouldBe i0 }
    withClue("min i2 i22") { Instant.min(i2, i22) shouldBe Instant(i2) }
    withClue("min i0 i1") { Instant.min(i0, i1) shouldBe i0 }
    withClue("min i1 i0") { Instant.min(i1, i0) shouldBe i0 }
    withClue("min i0 o1") { Instant.min(Some(i0), o1) shouldBe Some(i0) }
    withClue("min o1 i0") { Instant.min(o1, Some(i0)) shouldBe Some(i0) }
    withClue("min o1 none") { Instant.min(o1, none) shouldBe o1 }
    withClue("min none o1") { Instant.min(none, o1) shouldBe o1 }
    withClue("min None None") { Instant.min(None, None) shouldBe None }
  }
  
  it should "have a working copy constructor" in {
    
    val q = Instant(i0)
    q shouldBe i0
  }
  
  it should "have a working copy constructor for optionals" in {
    
    val q = Instant(o1)
    withClue("copy constructor for Some") { q shouldBe o1 }
    
    val q2 = Instant(none)
    withClue("Copy constructor for None") { q2 shouldBe None }
    
  }
  
  it should "intersect only with its self instant" in {
    i2.intersects(i22) shouldBe true
    i2.intersects(i0) shouldBe false
  }
  
  it should "contain only itself" in {
    i2.contains(i22) shouldBe true
    i2.contains(i0) shouldBe false
  }
  
  it should "be contained by itself " in {
    i2.containedBy(i22) shouldBe true
    i2.containedBy(i0) shouldBe false
    
  }
  
  it should "return the correct center" in {
    
    i2.center shouldBe Some(i2)
    i1.center shouldBe Some(i1)
    
  }
  
  it should "return the corrent length" in {
    i1.length shouldBe Some(0)
    i0.length shouldBe Some(0)
  }
}

object InstantTest {
  
  val i0 = Instant(Long.MinValue)
  val i1 = Instant(1)
  val i2 = Instant(Long.MaxValue)
  val i22 = Instant(Long.MaxValue)
  
  val o1 = Some(Instant(1))
  val none: Option[Instant] = None
  
}
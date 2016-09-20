package dbis.stark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class NRectRangeTest extends FlatSpec with Matchers {
  
  "A NRectRange" should "correctly contain a point" in {
    
    val rect = NRectRange(NPoint(0, 0), NPoint(10, 10))
    
		rect.contains(NPoint(5,5)) shouldBe true
    rect.contains(NPoint(0,0)) shouldBe true
    rect.contains(NPoint(0,5)) shouldBe true
    rect.contains(NPoint(0,10)) shouldBe false
    rect.contains(NPoint(5,0)) shouldBe true
    rect.contains(NPoint(9,0)) shouldBe true
    rect.contains(NPoint(10,0)) shouldBe false
    rect.contains(NPoint(10,10)) shouldBe false
  }
  
  it should "contain itself"  in {
    
    val rect = NRectRange(NPoint(0, 0), NPoint(10, 10))
    
    rect.contains(rect) shouldBe true
  }
  
  it should "correctly extend" in {
    
    val rect = NRectRange(NPoint(3, 3), NPoint(10, 10))
    
    
    val rect2 = NRectRange(NPoint(1, 5), NPoint(7, 14))
    
    val ex = rect.extend(rect2)
    
    ex shouldBe NRectRange(NPoint(1,3), NPoint(10,14))
    
  }
  
  it should "not extend for a smaller rect" in {
    val rect = NRectRange(NPoint(3, 3), NPoint(10, 10))
    
    
    val rect2 = NRectRange(NPoint(4, 5), NPoint(7, 8))
    
    val ex = rect.extend(rect2)
    
    ex shouldBe rect
  }
  
  it should "have the correct length" in {
    
    val rect = NRectRange(NPoint(0,0), NPoint(3,3))
    val lengths = rect.lengths
    
    lengths.size shouldBe 2
    lengths should contain only (3)
    
  }
  
  it should "have the correct length with negative ll" in {
    
    val rect = NRectRange(NPoint(-1,-1), NPoint(3,3))
    val lengths = rect.lengths
    
    lengths.size shouldBe 2
    lengths should contain only (4)
    
  }
  
  it should "have the correct length with both ll ur negative" in {
    
    val rect = NRectRange(NPoint(-4,-4), NPoint(-1,-1))
    val lengths = rect.lengths
    
    lengths.size shouldBe 2
    lengths should contain only (3)
    
  }
  
  
}
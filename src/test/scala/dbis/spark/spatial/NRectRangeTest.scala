package dbis.spark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class NRectRangeTest extends FlatSpec with Matchers {
  
  "A NRectRange" should "have the correct dimension" in {
    val p1 = NPoint(Array(1,2,3))
    
    val r = NRectRange(p1,p1)
    r.dim shouldBe 3
  }

  it should "correctly contain a point" in {
    
    val ll = NPoint(Array(1,1,1,1))
    val ur = NPoint(Array(4,4,4,4))
    
    val rect = NRectRange(ll, ur)
    
    val in = NPoint(Array(2,2,2,2))
    val out = NPoint(Array(5,5,5,5))
    
    withClue("contains ll") { rect.contains(ll) shouldBe true }
    withClue("contains ur") { rect.contains(ur) shouldBe false}
    withClue("contains other in" ) {rect.contains(in) shouldBe true}
    withClue("contains other out" ) {rect.contains(out) shouldBe false}
  }
  
  it should "contain itself" in {
    val ll = NPoint(Array(1,1,1,1))
    val ur = NPoint(Array(4,4,4,4))
    
    val rect = NRectRange(ll, ur)
    
    rect.contains(rect) shouldBe true
    
  }
  
  it should "contain a smaller rect" in {
    val ll = NPoint(Array(1,1,1,1))
    val ur = NPoint(Array(4,4,4,4))
    
    val rect = NRectRange(ll, ur)
    
    val ll2 = NPoint(Array(2,2,2,2))
    val ur2 = NPoint(Array(3,3,3,3))
    
    val rect2 = NRectRange(ll2,ur2)
    
    withClue("small in big") { rect.contains(rect2) shouldBe true }
    withClue("big in small") { rect2.contains(rect) shouldBe false }
    
  }
  
  it should "return correct lengths" in {
    
    val ll = NPoint(Array(1,1,1))
    val ur = NPoint(Array(4,4,4))
    
    val rect = NRectRange(ll,ur)
    
    withClue("length of lengths array") { rect.lengths.size shouldBe ll.dim }
    withClue("wrong values: ") {rect.lengths.deep shouldBe Array(3,3,3).deep}
    
  }
  
  it should "calculate the correct volume" in {
    val ll = NPoint(Array(1,1,1))
    val ur = NPoint(Array(4,4,4))
    
    val rect = NRectRange(ll,ur)      
    
    rect.volume shouldBe 27
    
  }
  
  
}
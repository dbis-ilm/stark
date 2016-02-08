package dbis.spark.spatial

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class NPointTest extends FlatSpec with Matchers {
  
  "An NPoint" should "return the correct dimension" in {
    val a = Array[Double](1,2,3,4,5,6)
    val p = NPoint(a)
    
    p.dim shouldBe a.size
  }
  
  it should "equal the same point" in {
    val a = NPoint(Array[Double](1,2,3))
    val b = NPoint(Array[Double](1,2,3))
    
    a shouldBe b
    a == b shouldBe true
    
  } 
  
}
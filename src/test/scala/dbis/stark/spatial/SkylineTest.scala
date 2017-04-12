package dbis.stark.spatial

import dbis.stark.STObject
import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck.Properties
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by hage on 12.04.17.
  */
class SkylineTest extends FlatSpec with Matchers {

  "A dominates relation" should "not dominate itself" in {

    val a = STObject(1,1)
    val b = a
    Skyline.centroidDominates(a,b) shouldBe false
  }

  it should "dominate if x is smaller" in {
    val a = STObject(1,2)
    val b = STObject(2,2)
    Skyline.centroidDominates(a,b) shouldBe true
  }

  it should "dominate if y is smaller" in {
    val a = STObject(2,1)
    val b = STObject(2,2)
    Skyline.centroidDominates(a,b) shouldBe true
  }

  it should "not dominate if x is greater" in {
    val a = STObject(2,2)
    val b = STObject(1,2)
    Skyline.centroidDominates(a,b) shouldBe false
  }

  it should "not dominate if y is greater" in {
    val a = STObject(2,2)
    val b = STObject(2,1)
    Skyline.centroidDominates(a,b) shouldBe false
  }
}

class SkylineCheck extends Properties("Skyline") {

  property("dominates") = forAll { (l: (Double, Double), r:(Double, Double)) =>
    (((l._1 < r._1 && l._2 <= r._2) || (l._2 < r._2 && l._1 <= r._1)) ==>
      Skyline.centroidDominates(STObject(l._1, l._2), STObject(r._1, r._2))) :| s"$l dominates $r" ||
    (((r._1 < l._1 && r._2 <= l._2) || (r._2 < l._2 && r._1 <= l._1)) ==>
      Skyline.centroidDominates(STObject(r._1, r._2), STObject(l._1, l._2))) :| s"$r dominates $l" ||
    (r._1 == l._1 && r._2 == l._2) ==> false :| s"equal points are not dominated"
  }

}
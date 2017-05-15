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

  "The skyline" should "correctly insert dominating point" in {

    val sp = (STObject(2,2),(STObject(0,0),1))
    val newSp = (STObject(1,1),(STObject(0,0),-1))

    Skyline.centroidDominates(newSp._1, sp._1) shouldBe true
    Skyline.centroidDominates(sp._1, newSp._1) shouldBe false

    val skyline = new Skyline[(STObject,Int)](List(sp), Skyline.centroidDominates)
    skyline.insert(newSp)

    skyline.skylinePoints should contain only newSp
  }

  it should "not insert dominated point" in {
    val p = (STObject(2,2),(STObject(0,0),1))
    val l = List(p)
    val skyline = new Skyline[(STObject,Int)](l, Skyline.centroidDominates)

    skyline.insert((STObject(2,3),(STObject(0,0),-1)))

    skyline.skylinePoints should contain only p

  }

  it should "add not dominated point" in {
    val o = (STObject(2,2),(STObject(0,0),1))
    val l = List(o)
    val skyline = new Skyline[(STObject,Int)](l, Skyline.centroidDominates)

    val p = (STObject(1,3),(STObject(0,0),-1))
    skyline.insert(p)

    skyline.skylinePoints should contain theSameElementsAs List(o,p)

  }

  it should "filter out dominated tuples on insert" in {
    val sp = (STObject(2,2),(STObject(0,0),1))
    val newSp = (STObject(1,1),(STObject(0,0),-1))


    val skyline = new Skyline[(STObject,Int)](List(sp), Skyline.centroidDominates)


    skyline.skylinePoints should contain only sp

    val l = skyline.skylinePoints.filterNot{ case (s,_) => Skyline.centroidDominates(newSp._1,s)}

    l shouldBe empty
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
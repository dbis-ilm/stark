package dbis.stark.spatial

import dbis.stark.{Distance, ScalarDistance}
import org.scalatest.{FlatSpec, Matchers}

class KNNTest extends FlatSpec with Matchers {

  "A KNN" should "correctly insert one elem with k = 3" in {
    val knn = new KNN[Int](3)

    knn.insert((ScalarDistance(0), 0))

    knn.m shouldBe 0
    knn.posMin shouldBe 0
    knn.posMax shouldBe 0

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(0), 0), null.asInstanceOf[(Distance, Int)], null.asInstanceOf[(Distance, Int)])
    knn.nn should contain theSameElementsInOrderAs seq
  }

  it should "correctly insert one elem with k = 1" in {
    val knn = new KNN[Int](1)

    knn.insert((ScalarDistance(0), 0))

    knn.m shouldBe 0
    knn.posMin shouldBe 0
    knn.posMax shouldBe 0

    println(knn)

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(0), 0))
    knn.nn should contain theSameElementsInOrderAs seq
  }

  it should "correctly insert two elems with k = 1" in {
    val knn = new KNN[Int](1)

    knn.insert((ScalarDistance(1), 0))

    knn.m shouldBe 0
    knn.posMin shouldBe 0
    knn.posMax shouldBe 0
println(knn)
    knn.insert((ScalarDistance(0), 0))
    knn.m shouldBe 0
    knn.posMin shouldBe 0
    knn.posMax shouldBe 0

    println(knn)

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(0), 0))
    knn.nn should contain theSameElementsInOrderAs seq
  }



  it should "correctly insert two elem with k = 3" in {
    val knn = new KNN[Int](3)

    knn.insert((ScalarDistance(0), 0))
    knn.insert((ScalarDistance(1), 1))

    knn.m shouldBe 1
    knn.posMin shouldBe 0
    knn.posMax shouldBe 1

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(0), 0), (ScalarDistance(1), 1), null.asInstanceOf[(Distance, Int)])
    knn.nn should contain theSameElementsInOrderAs seq
  }

  it should "correctly insert three elem with k = 3" in {
    val knn = new KNN[Int](3)

    knn.insert((ScalarDistance(0), 0))
    knn.insert((ScalarDistance(1), 1))
    knn.insert((ScalarDistance(2), 2))

    knn.m shouldBe 2
    knn.posMin shouldBe 0
    knn.posMax shouldBe 2

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(0), 0), (ScalarDistance(1), 1), (ScalarDistance(2), 2))
    knn.nn should contain theSameElementsInOrderAs seq
  }

  it should "correctly insert three elem with k = 3 in reverse order" in {
    val knn = new KNN[Int](3)

    knn.insert((ScalarDistance(2), 2))
    knn.insert((ScalarDistance(1), 1))
    knn.insert((ScalarDistance(0), 0))

    knn.m shouldBe 2
    knn.posMin shouldBe 2
    knn.posMax shouldBe 0

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(2), 2), (ScalarDistance(1), 1), (ScalarDistance(0), 0))
    knn.nn should contain theSameElementsInOrderAs seq
  }

  it should "correctly insert four elem with k = 3 in mixed order" in {
    val knn = new KNN[Int](3)


    knn.insert((ScalarDistance(2), 2))
    knn.insert((ScalarDistance(3), 3))
    knn.insert((ScalarDistance(1), 1))
    knn.insert((ScalarDistance(0), 0))

    knn.m shouldBe 2
    knn.posMin shouldBe 1
    knn.posMax shouldBe 0

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(2), 2), (ScalarDistance(0), 0), (ScalarDistance(1), 1))
    knn.nn should contain theSameElementsInOrderAs seq
  }

  it should "merge with an empty set" in {
    val knn = new KNN[Int](3)


    knn.insert((ScalarDistance(2), 2))
    knn.insert((ScalarDistance(3), 3))
    knn.insert((ScalarDistance(1), 1))
    knn.insert((ScalarDistance(0), 0))

    val knn2 = new KNN[Int](3)

    val res = knn.merge(knn2)

    res.nn should contain theSameElementsInOrderAs knn.nn
  }

  it should "merge with an non-empty set if empty itself" in {
    val knn = new KNN[Int](3)


    knn.insert((ScalarDistance(2), 2))
    knn.insert((ScalarDistance(3), 3))
    knn.insert((ScalarDistance(1), 1))
    knn.insert((ScalarDistance(0), 0))

    val knn2 = new KNN[Int](3)

    val res = knn2.merge(knn)

    res.nn should contain theSameElementsInOrderAs knn.nn
  }

  it should "merge with an non-empty set" in {
    val knn = new KNN[Int](3)

    knn.insert((ScalarDistance(2), 2))
    knn.insert((ScalarDistance(3), 3))
    knn.insert((ScalarDistance(1), 1))


    val knn2 = new KNN[Int](3)
    knn2.insert((ScalarDistance(0), 0))

    val res = knn.merge(knn2)

    val seq: Seq[(Distance, Int)] = Seq((ScalarDistance(2), 2), (ScalarDistance(0), 0), (ScalarDistance(1), 1))
    res.nn should contain theSameElementsInOrderAs seq
  }

}

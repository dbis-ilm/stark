package dbis.dbscan

import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class PointSpec extends FlatSpec with Matchers {
	"Point" should "be created from a vector" in {
		val p = Point(Vectors.dense(1.0, 2.0, 3.0))
		p.vec should be (Vectors.dense(1.0, 2.0, 3.0))
	}

	"ClusterPoint" should "be created from a vector" in {
		val p = ClusterPoint(Vectors.dense(1.0, 2.0, 3.0))
		p.vec should be (Vectors.dense(1.0, 2.0, 3.0))
	}

	it should "allow to assign and retrieve clusterId and flag" in {
		val p = ClusterPoint(Vectors.dense(1.0, 2.0, 3.0))
		p.clusterId = 42
		p.label = ClusterLabel.Core
		p.vec should be (Vectors.dense(1.0, 2.0, 3.0))
		p.clusterId should be (42)
		p.label should be (ClusterLabel.Core)
	
		val q = ClusterPoint(Vectors.dense(3.0, 2.0, 1.0), 5)
		q.vec should be (Vectors.dense(3.0, 2.0, 1.0))
		q.clusterId should be (5)
		q.label should be (ClusterLabel.Unclassified)
	}

	ignore should "produce a key based on the vector" in {
    val p1 = ClusterPoint(Vectors.dense(0.5, 0.9, 3.4))
    val p2 = ClusterPoint(Vectors.dense(0.5, 0.9, 3.4), 43, ClusterLabel.Core)
//    p1.key should be (p2.key)
  }

	it should "handle the payload" in {
    val p1 = ClusterPoint(Vectors.dense(0.5, 0.9, 3.4), 43, ClusterLabel.Core, Some(List("This", "is", "payload")), false)
    p1.payload should not be empty
    p1.payload should be (Some(List("This", "is", "payload")))
  }
}

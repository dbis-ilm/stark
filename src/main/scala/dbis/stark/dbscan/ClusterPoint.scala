package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vector

/**
	* An enumeration defining possible types for cluster points.
	*/
object ClusterLabel extends Enumeration {
	type ClusterLabel = Value
	val Core, Reachable, Noise, Unclassified = Value
}

import ClusterLabel._
import scala.reflect.ClassTag

/**
  * A ClusterPoint instance is a cluster point with an associated cluster identifier (an integer value),
  * a label describing the kind of point, and an optional payload for other data.
  *
  * @param id the unique ID of a point  
  * @param vec the data vector
  * @param clusterId the identifier of the cluster to which the point belongs
  * @param label the type of cluster point
  * @param payload optional data ignored for clustering but kept for the result
  * @param isMerge true if the point is a merge candidate, i.e. appears in multiple overlapping partitions
  */
case class ClusterPoint[K, T : ClassTag](
		id: K,
    override val vec: Vector,
		var clusterId: Int = 0,
    var label: ClusterLabel = Unclassified,
		payload: Option[T] = None,
		var isMerge: Boolean = false
	) extends Point(vec) with java.io.Serializable {

	/**
    * Returns a string representation of a ClusteredPoint.
    *
    * @return the string representation
    */
  override def toString: String = s"${vec.toArray.mkString(",")},$clusterId"
}

/**
  * The companion object of LabeledPoint.
  */
object ClusterPoint {
  def apply[K, T : ClassTag](p: ClusterPoint[K,T], pload: Option[T], merge: Boolean) = new ClusterPoint(p.id, p.vec, p.clusterId, p.label, payload = pload, isMerge = merge)
}
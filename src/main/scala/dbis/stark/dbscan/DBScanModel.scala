package dbis.stark.dbscan

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
  * A DBScanModel represents the result of a density-based clustering by keeping the set of clustered
  * points and partitions in RDDs. It allows to retrieve (and process) the clustered points as well
  * as check to which of an existing cluster a new point would be assigned (predict).
  *
  * @param points the RDD with the clustered (labeled) points
  * @param mbbs the RDD with the MBBs describing the partitions
  * @param distanceFun the distance function used for clustering the points
  */
class DBScanModel[K,T: ClassTag] (val points: RDD[ClusterPoint[K,T]],
                   val mbbs: RDD[MBB],
                   private val distanceFun: (Vector, Vector) => Double) extends java.io.Serializable {
  var nClusters = -1L

  /**
    * Returns the number of clusters in the clustering model. Note, that performs a distinct+count at
    * the first call, but the result is then cached.
    *
    * @return the number of clusters
    */
  def numOfClusters: Long = {
    if (nClusters == -1) {
      nClusters = points
          .filter(_.clusterId > 0)
          .map(_.clusterId)
          .distinct()
          .count()
    }
    nClusters
  }
  def predict(point: Vector): Int = {
    /* TODO:
     *  1. identify the containing partitions
     *  2. filter all points belonging to these partition
     *  3. check if the point is eps-reachable from one of the cluster points in the partition
     *  4. if yes, return the cluster id
     */
    -1
  }
}
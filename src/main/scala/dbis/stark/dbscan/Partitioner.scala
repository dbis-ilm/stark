package dbis.stark.dbscan

/**
  * Created by kai on 10.02.16.
  */
trait Partitioner {
  def computePartitioning(): List[MBB]
}

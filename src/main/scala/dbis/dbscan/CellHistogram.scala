package dbis.dbscan

import dbis.spatial.{NPoint, NRectRange}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.reflect.ClassTag

/**
  * Created by kai on 11.02.16.
  */
case class CellHistogram(buckets: Array[(NRectRange, Int)])

object CellHistogram {
  implicit def clusterPointToNPoint[T: ClassTag](cp: ClusterPoint[T]) = NPoint(cp.vec.toArray)
  implicit def vectorToNPoint(v: Vector) = NPoint(v.toArray)

  def zero[T: ClassTag](mbbs: List[MBB]): CellHistogram = {
    val buckets = new Array[(NRectRange, Int)](mbbs.size)
    mbbs.zipWithIndex.foreach{ case (mbb, i) => buckets(i) = (NRectRange(i, mbb.minVec, mbb.maxVec), 0)}
    CellHistogram(buckets)
  }

  def seq[T: ClassTag](histo1: CellHistogram, pt: ClusterPoint[T]): CellHistogram = {
    CellHistogram(histo1.buckets.map { b => if (b._1.contains(pt)) (b._1, b._2 + 1) else b })
  }

  def combine(histo1: CellHistogram, histo2: CellHistogram): CellHistogram = {
    val buckets = new Array[(NRectRange, Int)](histo1.buckets.size)
    for (i <- buckets.indices) {
      buckets(i) = (histo1.buckets(i)._1, histo1.buckets(i)._2 + histo2.buckets(i)._2)
    }
    CellHistogram(buckets)
  }
}
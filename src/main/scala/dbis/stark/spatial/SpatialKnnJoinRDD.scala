package dbis.stark.spatial

import dbis.stark.spatial.indexed.{Index, KnnIndex}
import dbis.stark.spatial.partitioner.{JoinPartition, OneToManyPartition}
import dbis.stark.{Distance, STObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

class InfiniteSingleIterator[T](elem: T) extends Iterator[T] {
  override def hasNext = true
  override def next() = elem
}
object InfiniteSingleIterator {
  def apply[T](elem: T) = new InfiniteSingleIterator(elem)
}

class SpatialKnnJoinRDD[G <: STObject : ClassTag, V : ClassTag, V2: ClassTag ](
                                                                    left: RDD[(G,V)], right: RDD[Index[V2]],
                                                                    k: Int,
                                                                    distFunc: (STObject,STObject) => Distance
                                                                ) extends RDD[(V,V2)](left.context, Nil) {

  override protected def getPartitions = {
    val leftParts:Seq[Int] = left.partitions.indices
    right.partitions.iterator.zipWithIndex.map{case (rp, i) =>
      OneToManyPartition(i, right, left, rp.index, leftParts)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(V,V2)] = {
    val p = split.asInstanceOf[OneToManyPartition]

    val indexes = right.iterator(right.partitions(p.leftIndex), context)

    indexes.flatMap { idx =>
      if(!idx.isInstanceOf[KnnIndex[_]])
        throw new IllegalArgumentException(s"must be of KnnIndex ${idx.getClass}!")

      val index = idx.asInstanceOf[KnnIndex[V2]]
      p.rightPartitions.iterator.flatMap { rp =>
        left.iterator(rp, context).flatMap { case (lg, lv) =>
          val leftIter = InfiniteSingleIterator(lv)
          val kNNs = index.kNN(lg, k, distFunc)
          leftIter.zip(kNNs)
        }
      }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case otm: OneToManyPartition =>
      (left.preferredLocations(otm.leftPartition) ++
        otm.rightPartitions.flatMap(right.preferredLocations)).distinct
    case jp: JoinPartition =>
      (left.preferredLocations(jp.leftPartition) ++ right.preferredLocations(jp.rightPartition)).distinct
  }

}

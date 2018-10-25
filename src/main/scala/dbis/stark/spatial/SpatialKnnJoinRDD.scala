package dbis.stark.spatial

import dbis.stark.spatial.indexed.{Index, KnnIndex}
import dbis.stark.spatial.partitioner.{OneToManyPartition, OneToOnePartition}
import dbis.stark.{Distance, STObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class InfiniteSingleIterator[T](elem: T) extends Iterator[T] {
  override def hasNext = true
  override def next() = elem
}
object InfiniteSingleIterator {
  def apply[T](elem: T) = new InfiniteSingleIterator(elem)
}

class SpatialKnnJoinRDD[G <: STObject : ClassTag, V : ClassTag, V2: ClassTag ](
              _left: RDD[(G,V)], _right: RDD[Index[V2]],
              k: Int,
              distFunc: (STObject,STObject) => Distance
//                                                                    oneToMany:Boolean = false
              ) extends JoinRDD[(G,V), Index[V2], (V,V2)](_left, _right, true, false) {


  /**
    * Returns [[OneToManyPartition]]. Here with one partition of the [[_right]] RDD mapped
    * to all partitions o the [[_left]] RDD
    * @return
    */
  override def getPartitions = {
    val leftParts:Seq[Int] = left.partitions.indices
    right.partitions.iterator.zipWithIndex.map{case (rp, i) =>
      OneToManyPartition(i, right, left, rp.index, leftParts)
    }.toArray
  }

  override protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext) = ???

  override protected def computeWithOneToMany(p: OneToManyPartition, context: TaskContext) = {
    val indexes = right.iterator(right.partitions(p.leftIndex), context)


    indexes.flatMap{
      case index: KnnIndex[V2] =>
//        val neighbors = ListBuffer.empty[(V,V2)]
//        val rightPartitionsIter = p.rightPartitions.iterator
//
//        while(rightPartitionsIter.hasNext && neighbors.size < k) {
//          val rp = rightPartitionsIter.next()
//          left.iterator(rp, context).foreach{ case (lg, lv) =>
//
//            val kNNs = index.kNN(lg, k, distFunc)
//            var toTake = 0
//            val toAdd = kNNs.takeWhile{ _ =>
//              toTake += 1
//              neighbors.length + toTake < k
//            }
//
//
//            neighbors ++= InfiniteSingleIterator(lv).zip(toAdd)
//          }
//        }
//
//        neighbors.iterator

        p.rightPartitions.iterator.flatMap { rp =>
          left.iterator(rp, context).flatMap {
            case (lg, lv) =>
              val leftIter = InfiniteSingleIterator(lv)
              val kNNs = index.kNN(lg, k, distFunc).map{ case (v,_) => v}
              leftIter.zip(kNNs)
            case b =>
              println(b)
              ???
          }
        }
      case idx@ _ =>
        throw new IllegalArgumentException(s"must be of KnnIndex ${idx.getClass}!")
    }
  }
}

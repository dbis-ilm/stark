package dbis.stark.spatial

import dbis.stark.spatial.partitioner.{OneToManyPartition, OneToOnePartition, GridPartitioner}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

abstract class JoinRDD[L,R, RES:ClassTag](var left: RDD[L], var right: RDD[R], oneToMany: Boolean,
                                          checkParties: Boolean)
  extends RDD[RES](left.context, Nil) {

  protected val numPartitionsInRight = right.getNumPartitions

  lazy val leftPartitioner = left.partitioner.flatMap {
    case r: GridPartitioner => Some(r)
    case _ => None
  }

  lazy val rightPartitioner = right.partitioner.flatMap {
    case r: GridPartitioner => Some(r)
    case _ => None
  }

  protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext): Iterator[RES]
  protected def computeWithOneToMany(partition: OneToManyPartition, context: TaskContext): Iterator[RES]

  override final def compute(s: Partition, context: TaskContext): Iterator[RES] = new InterruptibleIterator(context, s match {
    case oto: OneToOnePartition => computeWithOneToOnePartition(oto, context)
    case otm: OneToManyPartition => computeWithOneToMany(otm, context)
  })

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case otm: OneToManyPartition =>
      (left.preferredLocations(otm.leftPartition) ++
        otm.rightPartitions.flatMap(right.preferredLocations)).distinct
    case jp: OneToOnePartition =>
      (left.preferredLocations(jp.leftPartition) ++ right.preferredLocations(jp.rightPartition)).distinct
  }

  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }

  override def getDependencies: Seq[Dependency[_]] = if(oneToMany) {

    List(new NarrowDependency(left) {
      def getParents(id: Int): Seq[Int] = List(partitions(id).asInstanceOf[OneToManyPartition].leftIndex)
    },
    new NarrowDependency(right) {
      def getParents(id: Int): Seq[Int] = partitions(id).asInstanceOf[OneToManyPartition].rightIndex
    })


  } else {
    List(
      new NarrowDependency(left) {
        def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRight)
      },
      new NarrowDependency(right) {
        def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRight)
      }
    )
  }

//  override protected def getPartitions = {
//    val parts = ArrayBuffer.empty[JoinPartition]
//
//    val checkPartitions = leftPartitioner.isDefined && rightPartitioner.isDefined
//    var idx = 0
//
//    for (
//      s1 <- left.partitions;
//      s2 <- right.partitions
//      if !checkPartitions || leftPartitioner.get.partitionExtent(s1.index).intersects(rightPartitioner.get.partitionExtent(s2.index))) {
//
//      val p = JoinPartition(idx, left, right, s1.index, s2.index)//, leftContainsRight, rightContainsLeft)
//      parts += p
//      idx += 1
//
//    }
//    parts.toArray
//  }

  override def getPartitions: Array[Partition] = {

    val parts = ListBuffer.empty[Partition]

    if (leftPartitioner.isDefined && leftPartitioner == rightPartitioner) {
      left.partitions.iterator.zip(right.partitions.iterator).foreach { case (l, r) =>
        if (oneToMany)
          parts += OneToManyPartition(l.index, left, right, l.index, Seq(r.index))
        else
          parts += OneToOnePartition(l.index, left, right, l.index, r.index)
      }

    } else {
      val checkPartitions = checkParties && leftPartitioner.isDefined && rightPartitioner.isDefined

      if (oneToMany) {

        val pairs = mutable.Map.empty[Int, ListBuffer[Int]]
        for (
          s1 <- left.partitions;
          s2 <- right.partitions
          if !checkPartitions || leftPartitioner.get.partitionExtent(s1.index).intersects(rightPartitioner.get.partitionExtent(s2.index))) {

          if (pairs.contains(s1.index)) {
            pairs(s1.index) += s2.index
          } else
            pairs += s1.index -> ListBuffer(s2.index)
        }

        pairs.iterator.zipWithIndex.foreach { case ((lIdx, rights), idx) =>
          parts += OneToManyPartition(idx, left, right, lIdx, rights)
        }

      } else { // "normal" join partition

        var idx = 0
        for (
          s1 <- left.partitions;
          s2 <- right.partitions
          if !checkPartitions || leftPartitioner.get.partitionExtent(s1.index).intersects(rightPartitioner.get.partitionExtent(s2.index))) {

          val p = OneToOnePartition(idx, left, right, s1.index, s2.index) //, leftContainsRight, rightContainsLeft)
          parts += p
          idx += 1
        }
      }
    }

    parts.toArray
  }

}

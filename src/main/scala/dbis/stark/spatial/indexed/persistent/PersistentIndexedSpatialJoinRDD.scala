package dbis.stark.spatial.indexed.persistent

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.indexed.Index
import dbis.stark.spatial.partitioner.{JoinPartition, SpatialPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


class PersistentIndexedSpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag](
    var left: RDD[Index[(G,V)]],
    var right: RDD[(G,V2)],
    pred: JoinPredicate.JoinPredicate
    )  extends RDD[(V,V2)](left.context, Nil) {
  
  val numPartitionsInright = right.partitions.length

  val predicateFunction = JoinPredicate.predicateFunction(pred)

  private lazy val rightParti = right.partitioner.map {
    case sp: SpatialPartitioner => sp
  }

  private lazy val leftParti = left.partitioner.map {
    case sp: SpatialPartitioner => sp
  }


  override def getPartitions: Array[Partition] = {
    val parts = ListBuffer.empty[JoinPartition]

    val checkPartitions = leftParti.isDefined && rightParti.isDefined
    var idx = 0
    for (
      s1 <- left.partitions;
      s2 <- right.partitions
      if !checkPartitions || leftParti.get.partitionExtent(s1.index).intersects(rightParti.get.partitionExtent(s2.index))) {

      parts += JoinPartition(idx, left, right, s1.index, s2.index)
      idx += 1
    }
    parts.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[JoinPartition]
    (left.preferredLocations(currSplit.leftPartition) ++ right.preferredLocations(currSplit.rightPartition)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(V, V2)] = {
    val currSplit = split.asInstanceOf[JoinPartition]

//    val rightArray = right.iterator(currSplit.rightPartition, context).toArray

    val resultIter = left.iterator(currSplit.leftPartition, context).flatMap{ tree =>
      
      /*
       * Returns:
  		 * an Envelope (for STRtrees), an Interval (for SIRtrees), or other object 
  		 * (for other subclasses of AbstractSTRtree)
  		 * 
  		 * http://www.atetric.com/atetric/javadoc/com.vividsolutions/jts-core/1.14.0/com/vividsolutions/jts/index/strtree/Boundable.html#getBounds--
       */

      right.iterator(currSplit.rightPartition, context).flatMap { case (rg,rv) =>
        tree.query(rg)
          .filter { case (lg,_) => predicateFunction(lg,rg)}
          .map{ case (_,lv) => (lv,rv) }
      }
    }
    
    resultIter
    
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(left) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInright)
    },
    new NarrowDependency(right) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInright)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }  
}


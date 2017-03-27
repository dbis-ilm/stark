package dbis.stark.spatial.plain



import java.io.{IOException, ObjectOutputStream}

import dbis.stark.STObject
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * A partition that represents the two partitions that have to be joined
  * @param idx The index of the partition
  * @param rdd1 The left RDD
  * @param rdd2 The right RDD
  * @param s1Index The index of the partition in the left RDD
  * @param s2Index The index of the partition in the right RDD
  */
protected[stark] class JoinPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1: Partition = rdd1.partitions(s1Index)
  var s2: Partition = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }

  override def toString: String = s"JoinPartition[idx=$idx, leftIdx=$s1Index, rightIdx=$s2Index, s1=$s1, s2=$s2]"
}

/**
  * A join implementation as a cartesian product.
  *
  * This should be used if the predicate function is not known so that we cannot decide which partitions
  * have to be joined
  * @param sc The [[org.apache.spark.SparkContext]] to use. The two RDDs may be created by different contexts
  * @param rdd1 The left RDD
  * @param rdd2 The right TDD
  * @param predicate The predicate function
  * @tparam G The class to used for representing spatio-temporal objects in the left RDD
  * @tparam G2 The class to used for representing spatio-temporal objects in the right RDD
  * @tparam V The class to used for representing payload data in the left RDD
  * @tparam V2 The class to used for representing payload data in the left RDD
  */
private[stark] class CartesianSpatialJoinRDD[G <: STObject : ClassTag, G2 <: STObject: ClassTag, V: ClassTag, V2: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[(G,V)],
    var rdd2 : RDD[(G2,V2)],
    predicate: (G,G2) => Boolean)
  extends RDD[(V, V2)](sc, Nil)
  with Serializable {

  // number of partitions in the right RDD
  val numPartitionsInRdd2: Int = rdd2.partitions.length

  /**
    * Get the partitions of this RDD.
    *
    * This method will return an array of [[dbis.stark.spatial.plain.JoinPartition]]
    * @return The list of join partitions
    */
  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new JoinPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[JoinPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(V, V2)] = {
    val currSplit = split.asInstanceOf[JoinPartition]
    for (l <- rdd1.iterator(currSplit.s1, context);
         r <- rdd2.iterator(currSplit.s2, context)
         if predicate(l._1, r._1)) yield (l._2, r._2) // apply predicate function and return result
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}

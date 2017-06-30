package dbis.stark.spatial

import dbis.stark.STObject
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.partitioner.{JoinPartition, SpatialPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * A spatio-temporal join implementation.
  *
  * Currently this is a nested loop implementation
  *
  * @param left The left input RDD
  * @param right The right input RDD
  * @param predicateFunc The predicate to apply in the join (join condition)
  * @param treeOrder The (optional) order of the tree. <= 0 to not apply indexing
  * @param checkParties Perform partition check
  * @tparam G The type representing spatio-temporal data
  * @tparam V The type representing payload data in left RDD
  * @tparam V2 The type representing payload data in right RDD
  */
class SpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag] private (
  var left: RDD[(G,V)],
  var right: RDD[(G,V2)],
  predicateFunc: (G,G) => Boolean,
  treeOrder: Int,
  private val checkParties: Boolean)  extends RDD[(V,V2)](left.context, Nil) {

  private val numPartitionsInRight = right.getNumPartitions

  /**
    * Create a new join operator with the given predicate.
    *
    * Use this constuctor if live indexing is desired (but it still is optional)
    * @param left The left RDD
    * @param right The right RDD
    * @param predicate The predicate to use
    * @param capacity The optional capacity (order) of the index tree. If <= 0 no indexing is applied
    */
  def this(left: RDD[(G,V)], right: RDD[(G,V2)],
           predicate: JoinPredicate.JoinPredicate,
           capacity: Int = -1) =
    this(left, right, JoinPredicate.predicateFunction(predicate), capacity, checkParties = true)

  /**
    * Create a new join operator with the given predicate function.
    *
    * With this constructor, no indexing is applied
    * @param left The left RDD
    * @param right The right RDD
    * @param predicate The predicate function
    */
  def this(left: RDD[(G,V)], right: RDD[(G,V2)],
           predicate: (G,G) => Boolean) =
    this(left, right, predicate, -1, checkParties = false)

  override def getPartitions: Array[Partition] = {
    val parts = ArrayBuffer.empty[JoinPartition]

    val checkPartitions = checkParties && leftParti.isDefined && rightParti.isDefined
    var idx = 0

    // Which of the following two loop constructs are fastest? Is there a significant difference?

//    var s1Index = 0
//    var s2Index = 0
//
//    while(s1Index < left.partitions.length) {
//      s2Index = 0
//      while(s2Index < right.partitions.length) {
//
//        if(!checkPartitions || leftParti.get.partitionExtent(s1Index).intersects(rightParti.get.partitionExtent(s2Index))) {
//          parts += new JoinPartition(idx, left, right, s1Index, s2Index)
//          idx += 1
//        }
//
//        s2Index += 1
//      }
//      s1Index +=1
//    }

    for (
      s1 <- left.partitions;
      s2 <- right.partitions
      if !checkPartitions || leftParti.get.partitionExtent(s1.index).intersects(rightParti.get.partitionExtent(s2.index))) {

//        val lExtent = leftParti.get.partitionExtent(s1.index)
//        val rExtent = rightParti.get.partitionExtent(s2.index)
//
//        println(lExtent)
//        println(lExtent.wkt)
//
//        println
//
//        println(rExtent)
//        println(rExtent.wkt)
//
//        val i = lExtent.intersects(rExtent)
//        println(i)
//
//        if(i) {
          val p = new JoinPartition(idx, left, right, s1.index, s2.index)
//          println(s"marking $p for processing")
          parts += p
          idx += 1
//        }

    }
    parts.toArray
  }

  private[stark] lazy val leftParti = left.partitioner.map{
    case sp: SpatialPartitioner => sp
  }

  private[stark] lazy val rightParti = right.partitioner.map{
    case sp: SpatialPartitioner => sp
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(V, V2)] = {

    // in getPartitions we created JoinPartition that link to two partitions that have to be joined
    val split = s.asInstanceOf[JoinPartition]

    // if treeOrder is <= 0 we do not use indexing
    if(treeOrder <= 0) {
      // collect the right partition into an array
      val rightList = right.iterator(split.rightPartition, context).toArray

      // loop over the left partition and check join condition on every element in the right partition's array
      left.iterator(split.leftPartition, context).flatMap{ case (lg, lv) =>
        rightList.filter{ case (rg, _) =>
          val res = predicateFunc(lg,rg)
//          println(s"check ($predicateFunc) $lg -- $rg --> $res")
            res
          }.map{ case (_,rv) => (lv,rv) }

      }
    } else { // we should apply indexing

      // the index
      val tree = new RTree[G,(G,V2)](capacity = treeOrder)

      // insert everything into the tree
      right.iterator(split.rightPartition, context).foreach{ case (g, v) => tree.insert(g, (g,v)) }

      // build the tree
      tree.build()

      // loop over every element in the left partition and query tree.
      // For the results of a query we have to perform candidates check
      left.iterator(split.leftPartition, context).flatMap { case (lg, lv) =>
        tree.query(lg) // index query
          .filter{ case (rg, _) => predicateFunc(lg, rg) } // candidate check and apply join condidion
          .map{ case (_,rv) => (lv,rv)} // result is the combined tuple of the payload items
      }


    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[JoinPartition]
    (left.preferredLocations(currSplit.leftPartition) ++ right.preferredLocations(currSplit.rightPartition)).distinct
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(left) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRight)
    },
    new NarrowDependency(right) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRight)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    left = null
    right = null
  }

}

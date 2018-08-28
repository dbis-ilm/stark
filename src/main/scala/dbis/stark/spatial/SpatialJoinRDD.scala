package dbis.stark.spatial

import dbis.stark.STObject
import dbis.stark.spatial.indexed.{IndexConfig, IndexFactory}
import dbis.stark.spatial.partitioner.{JoinPartition, OneToManyPartition, SpatialPartitioner}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * A spatio-temporal join implementation.
  *
  * Currently this is a nested loop implementation
  *
  * @param left The left input RDD
  * @param right The right input RDD
  * @param predicateFunc The predicate to apply in the join (join condition)
  * @param indexConfig The (optional) configuration for indexing
  * @param checkParties Perform partition check
  * @tparam G The type representing spatio-temporal data
  * @tparam V The type representing payload data in left RDD
  * @tparam V2 The type representing payload data in right RDD
  */
class SpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag] private (
  var left: RDD[(G,V)],
  var right: RDD[(G,V2)],
  predicateFunc: (G,G) => Boolean,
  indexConfig: Option[IndexConfig],
  private val checkParties: Boolean,
  oneToMany: Boolean)  extends RDD[(V,V2)](left.context, Nil) {

  private val numPartitionsInRight = right.getNumPartitions

  /**
    * Create a new join operator with the given predicate.
    *
    * Use this constuctor if live indexing is desired (but it still is optional)
    * @param left The left RDD
    * @param right The right RDD
    * @param predicate The predicate to use
    * @param indexConfig The optional configuration of the index to use. If [None] no index will be used
    */
  def this(left: RDD[(G,V)], right: RDD[(G,V2)],
           predicate: JoinPredicate.JoinPredicate,
           indexConfig: Option[IndexConfig] = None, oneToMany: Boolean = false) =
    this(left, right, JoinPredicate.predicateFunction(predicate), indexConfig, checkParties = true, oneToMany)

  /**
    * Create a new join operator with the given predicate function.
    *
    * With this constructor, no indexing is applied
    * @param left The left RDD
    * @param right The right RDD
    * @param predicate The predicate function
    */
  def this(left: RDD[(G,V)], right: RDD[(G,V2)],
           predicate: (G,G) => Boolean, oneToMany: Boolean) =
    this(left, right, predicate, None, checkParties = false, oneToMany)

  override def getPartitions: Array[Partition] = {

    val parts = ListBuffer.empty[Partition]

    if (leftParti.isDefined && leftParti == rightParti) {

      left.partitions.iterator.zip(right.partitions.iterator).foreach { case (l, r) =>
        if (oneToMany)
          parts += OneToManyPartition(l.index, left, right, l.index, Seq(r.index))
        else
          parts += JoinPartition(l.index, left, right, l.index, r.index)
      }

    } else {
      val checkPartitions = checkParties && leftParti.isDefined && rightParti.isDefined

      if (oneToMany) {

        val pairs = mutable.Map.empty[Int, ListBuffer[Int]]
        for (
          s1 <- left.partitions;
          s2 <- right.partitions
          if !checkPartitions || leftParti.get.partitionExtent(s1.index).intersects(rightParti.get.partitionExtent(s2.index))) {

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
          if !checkPartitions || leftParti.get.partitionExtent(s1.index).intersects(rightParti.get.partitionExtent(s2.index))) {

          val p = JoinPartition(idx, left, right, s1.index, s2.index) //, leftContainsRight, rightContainsLeft)
          parts += p
          idx += 1
        }
      }
    }

    parts.toArray
  }

  private[stark] lazy val leftParti = left.partitioner.flatMap{
    case sp: SpatialPartitioner => Some(sp)
    case _ => None
  }

  private[stark] lazy val rightParti = right.partitioner.flatMap{
    case sp: SpatialPartitioner => Some(sp)
    case _ => None
  }

  private def computeWithJoinPartition(partition: JoinPartition, context: TaskContext): Iterator[(V,V2)] = {

    // if treeOrder is <= 0 we do not use indexing
    if(indexConfig.isEmpty) {

      // loop over the left partition and check join condition on every element in the right partition's array
      left.iterator(partition.leftPartition, context).flatMap { case (lg, lv) =>
        right.iterator(partition.rightPartition, context).filter { case (rg, _) =>
          predicateFunc(lg, rg)
        }.map { case (_, rv) => (lv, rv) }
      }

    } else { // we should apply indexing

      // the index
      val tree = IndexFactory.get[G, (G,V)](indexConfig.get)

      // insert everything into the tree
      left.iterator(partition.leftPartition, context).foreach{ case (g, v) => tree.insert(g, (g,v)) }

      // build the tree
      tree.build()

      // loop over every element in the left partition and query tree.
      // For the results of a query we have to perform candidates check
      right.iterator(partition.rightPartition, context).flatMap { case (rg, rv) =>
        tree.query(rg) // index query
          .filter { case (lg, _) =>
          predicateFunc(lg, rg) // candidate check and apply join condidion
        }
          .map { case (_, lv) => (lv, rv) } // result is the combined tuple of the payload items
      }
    }
  }

  private def computeWithOneToMany(partition1: OneToManyPartition, context: TaskContext): Iterator[(V,V2)] = {
    val split = partition1

    // if treeOrder is <= 0 we do not use indexing
    if(indexConfig.isEmpty) {
      // loop over the left partition and check join condition on every element in the right partition's array
      left.iterator(split.leftPartition, context).flatMap { case (lg, lv) =>

        split.rightPartitions.flatMap { rp =>

          right.iterator(rp, context).filter { case (rg, _) =>
            predicateFunc(lg, rg)
          }.map { case (_, rv) => (lv, rv) }
        }
      }
    } else { // we should apply indexing

      // the index
      val tree = IndexFactory.get[G, (G,V)](indexConfig.get)

      // insert everything into the tree
      left.iterator(split.leftPartition, context).foreach{ case (g, v) => tree.insert(g, (g,v)) }

      // build the tree
      tree.build()

      // loop over every element in the left partition and query tree.
      // For the results of a query we have to perform candidates check
      split.rightPartitions.iterator.flatMap { rp =>
        right.iterator(rp, context).flatMap { case (rg, rv) =>
          tree.query(rg) // index query
            .filter { case (lg, _) =>
            predicateFunc(lg, rg) // candidate check and apply join condidion
          }
            .map { case (_, lv) => (lv, rv) } // result is the combined tuple of the payload items
        }
      }
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(V, V2)] = s match {
    case jp: JoinPartition => computeWithJoinPartition(jp, context)
    case otm: OneToManyPartition => computeWithOneToMany(otm, context)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case otm: OneToManyPartition =>
      (left.preferredLocations(otm.leftPartition) ++
        otm.rightPartitions.flatMap(right.preferredLocations)).distinct
    case jp: JoinPartition =>
      (left.preferredLocations(jp.leftPartition) ++ right.preferredLocations(jp.rightPartition)).distinct
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

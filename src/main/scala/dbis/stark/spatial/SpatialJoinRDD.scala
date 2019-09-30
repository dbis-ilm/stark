package dbis.stark.spatial

import dbis.stark.STObject
import dbis.stark.spatial.indexed.{IndexConfig, IndexFactory}
import dbis.stark.spatial.partitioner.{OneToOnePartition, OneToManyPartition}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * A spatio-temporal join implementation.
  *
  * Currently this is a nested loop implementation
  *
  * @param _left The left input RDD
  * @param _right The right input RDD
  * @param predicateFunc The predicate to apply in the join (join condition)
  * @param indexConfig The (optional) configuration for indexing
  * @param checkParties Perform partition check
  * @tparam G The type representing spatio-temporal data
  * @tparam V The type representing payload data in left RDD
  * @tparam V2 The type representing payload data in right RDD
  */
class SpatialJoinRDD[G <: STObject : ClassTag, V: ClassTag, V2: ClassTag] private (_left: RDD[(G,V)],
                                                                                   _right: RDD[(G,V2)],
                                                                                   predicateFunc: (G,G) => Boolean,
                                                                                   indexConfig: Option[IndexConfig],
                                                                                   private val checkParties: Boolean,
                                                                                   oneToMany: Boolean)  extends JoinRDD[(G,V),(G,V2),(V,V2)](_left, _right, oneToMany, checkParties) { // RDD[(V,V2)](left.context, Nil) {

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



  protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext): Iterator[(V,V2)] = {

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
      val tree = IndexFactory.get[(G,V)](indexConfig.get)

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

  protected def computeWithOneToMany(split: OneToManyPartition, context: TaskContext): Iterator[(V,V2)] = {

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
      val tree = IndexFactory.get[(G,V)](indexConfig.get)

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
}

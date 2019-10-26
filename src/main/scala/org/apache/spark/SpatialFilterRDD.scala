package org.apache.spark

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{IndexConfig, IndexFactory}
import dbis.stark.spatial.partitioner.{GridPartitioner, SpatialPartition, TemporalPartitioner}
import dbis.stark.spatial.{JoinPredicate, StarkUtils}
import dbis.stark.{Interval, STObject}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * A stpatio-temporal filter implementation
  *
  * @param parent The parent RDD serving as input
  * @param qry The query object used in the filter evaluation
  * @param predicateFunc The predicate to apply in the filter
  * @param indexConfig The (optional) indexing configuration
  * @param checkParties Perform partition check
  * @tparam G The type representing spatio-temporal data
  * @tparam V The payload data
  */
class SpatialFilterRDD[G <: STObject : ClassTag, V : ClassTag] protected[spark] (@transient private val parent: RDD[(G,V)],
                                                                        qry: G, predicate: JoinPredicate, predicateFunc: (G,G) => Boolean,
                                                                        indexConfig: Option[IndexConfig], private val checkParties: Boolean)
  extends SpatialRDD[G,V](parent) {

  def this(parent: RDD[(G,V)], qry: G, predicateFunc: (G,G) => Boolean) =
    this(parent, qry,null, predicateFunc, None, false)

  def this(parent: RDD[(G,V)], qry: G, predicate: JoinPredicate, indexConfig: Option[IndexConfig] = None) =
    this(parent, qry, predicate, parent.context.clean(JoinPredicate.predicateFunction(predicate)), indexConfig, true)

  /**
    * The partitioner of this RDD.
    *
    * This will always be set to the parent's partitioner
    */
  //  override val partitioner: Option[Partitioner] = parent.partitioner

  /**
    * Return the partitions that have to be processed.
    *
    * We apply partition pruning in this step: If a [[dbis.stark.spatial.partitioner.GridPartitioner]]
    * is present, we check if the partition can contain candidates. If not, it is not returned
    * here.
    *
    * @return The list of partitions of this RDD
    */
  override def getPartitions: Array[Partition] = {
    val parent = firstParent[(G,V)]
    partitioner.map{
      // check if this is a spatial partitioner
      case sp: GridPartitioner if checkParties =>

        val spatialParts = ListBuffer.empty[Partition]

        val qryEnv = qry.getGeo.getEnvelopeInternal
        var parentPartiId = 0
        var spatialPartId = 0
        val numParentParts = parent.getNumPartitions

        // loop over all partitions in the parent
        while (parentPartiId < numParentParts) {
          val cell = sp.partitionBounds(parentPartiId)
          // check if partitions intersect
          if (StarkUtils.toEnvelope(cell.extent).intersects(qryEnv)) {
            // create a new "spatial partition" pointing to the parent partition
            spatialParts += SpatialPartition(spatialPartId, parentPartiId, parent)
            spatialPartId += 1
          }

          parentPartiId += 1
        }

//        println(s"spatial parts: ${spatialParts.size}")
//        spatialParts.foreach(println)
        spatialParts.toArray
      case tp: TemporalPartitioner =>
        val spatialParts = ListBuffer.empty[Partition]
        //        val qryEnv = qry.getGeo.getEnvelopeInternal
        var i = 0
        var cnt = 0
        val numParentParts = parent.getNumPartitions
        while (i < numParentParts) {
          predicate match {
            case JoinPredicate.INTERSECTS =>
              if (tp.partitionBounds(i).intersects(qry.getTemp.get)) {
                spatialParts += SpatialPartition(cnt, i, parent)
                cnt += 1
              }
            case JoinPredicate.CONTAINEDBY =>
              val newi = {
                if (i == tp.numPartitions - 1) {
                  tp.partitionBounds(i)
                } else {
                  Interval(tp.partitionBounds(i).start, tp.partitionBounds(i + 1).start)
                }
              }
              if (newi.intersects(qry.getTemp.get)) {
                spatialParts += SpatialPartition(cnt, i, parent)
                cnt += 1
              }
            case JoinPredicate.CONTAINS =>
              //println(tp.partitionBounds(i).contains(qry.getTemp.get))
              if (tp.partitionBounds(i).contains(qry.getTemp.get)) {
                spatialParts += SpatialPartition(cnt, i, parent)
                cnt += 1
              }
            case _ =>
              spatialParts += SpatialPartition(cnt, i, parent)
              cnt += 1
          }
          i += 1

        }
        spatialParts.toArray
      case _ =>
        // other (unknown) partitioner
        parent.partitions

    }.getOrElse{
      // no partitioner
      parent.partitions
    }
  }



  @DeveloperApi
  override def compute(inputSplit: Partition, context: TaskContext): Iterator[(G, V)] = {

    /* determine the split to process. If a spatial partitioner was applied, the actual
     * partition/split is encapsulated
     */

    val split = inputSplit match {
      case sp: SpatialPartition =>
        sp.parentPartition
      case _ => inputSplit
    }

    val inputIter = firstParent[(G,V)].iterator(split, context)

    val resultIter = if (indexConfig.isDefined) {

      val tree = IndexFactory.get[(G, V)](indexConfig.get)
      // insert everything into the tree
      inputIter.foreach { case (g, v) => tree.insert(g, (g, v)) }

      // query tree and perform candidates check
      tree.query(qry).filter { case (g, _) => predicateFunc(g, qry) }

    } else {
      inputIter.filter { case (g, _) =>
        val res = predicateFunc(g, qry)
//        println(s"checking $predicate $g -- $qry : $res")
        res
      }
    }
    new InterruptibleIterator(context, resultIter)
//    resultIter
  }
}

package dbis.stark.spatial.indexed.persistent

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.Index
import dbis.stark.spatial.partitioner.{GridPartitioner, SpatialPartition}
import dbis.stark.spatial.{JoinPredicate, StarkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class SpatialIndexedRDD[G <: STObject : ClassTag, V : ClassTag](@transient private val parent: RDD[Index[(G,V)]],
                                                                qry: G, predicate: JoinPredicate, predicateFunc: (G,G) => Boolean,
                                               private val checkParties: Boolean) extends RDD[(G,V)](parent) {
  def this(parent: RDD[Index[(G,V)]], qry: G, predicate: JoinPredicate) =
    this(parent, qry, predicate, JoinPredicate.predicateFunction(predicate), true)

  override def compute(split: Partition, context: TaskContext): Iterator[(G, V)] = {

    val thePartition = split match {
      case sp: SpatialPartition => sp.parentPartition
      case _ => split
    }

    val inputIter = firstParent[Index[(G,V)]].iterator(thePartition, context)
    inputIter.flatMap{ tree =>
      tree.query(qry).filter { case (g, _) => predicateFunc(g, qry) }
    }
  }

  override def getPartitions: Array[Partition] = {
    val parent = firstParent[Index[(G, V)]]
    partitioner.map {
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

        spatialParts.toArray
      case _ =>
        parent.partitions
    }.getOrElse(parent.partitions)
  }
}

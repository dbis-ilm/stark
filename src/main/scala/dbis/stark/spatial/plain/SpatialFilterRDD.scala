package dbis.stark.spatial.plain

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial._
import dbis.stark.spatial.indexed.RTree
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
/**
  * Created by hg on 24.02.17.
  */
class SpatialFilterRDD[G <: STObject : ClassTag, V : ClassTag](
  private val parent: RDD[(G,V)],
  qry: G,
  predicateFunc: (G,G) => Boolean,
  treeOrder: Int,
  private val checkParties: Boolean) extends SpatialRDD[G,V](parent) {


  def this(parent: RDD[(G,V)], qry: G, predicateFunc: (G,G) => Boolean) =
    this(parent, qry, predicateFunc, -1, false)

  def this(parent: RDD[(G,V)], qry: G, predicate: JoinPredicate, treeOrder: Int = -1) =
    this(parent, qry, JoinPredicate.predicateFunction(predicate), treeOrder, true)

  /**
    * Return the partitions that have to be processed.
    *
    * We apply partition pruning in this step: If a [[dbis.stark.spatial.SpatialPartitioner]]
    * is present, we check if the partition can contain candidates. If not, it is not returned
    * here.
    *
    *
    * @return The list of partitions of this RDD
    */
  override def getPartitions: Array[Partition] = partitioner.map{
      case sp: SpatialPartitioner if checkParties =>

        val spatialParts = ListBuffer.empty[Partition]

        val qryEnv = qry.getGeo.getEnvelopeInternal
        var parentPartiId = 0
        var spatialPartId = 0
        val numParentParts = parent.getNumPartitions
        while (parentPartiId < numParentParts) {

          val cell = sp.partitionBounds(parentPartiId)

          if (Utils.toEnvelope(cell.extent).intersects(qryEnv)) {
            spatialParts += SpatialPartition(spatialPartId, parentPartiId, parent)
            spatialPartId += 1
          }

          parentPartiId += 1
        }

        spatialParts.toArray

      case _ =>
        parent.partitions
    }.getOrElse(parent.partitions)


  @DeveloperApi
  override def compute(inputSplit: Partition, context: TaskContext): Iterator[(G, V)] = {

    /* determine the split to process. If a spatial partitioner was applied, the actual
     * partition/split is encapsulated
     */
    val split = inputSplit match {
      case sp: SpatialPartition => sp.split
      case _ => inputSplit
    }

    // distinguish between no and live indexing
    if(treeOrder <= 0) {
      parent.iterator(split, context).filter { case (g, _) => predicateFunc(g, qry) }
    } else {
      val tree = new RTree[G,(G,V)](treeOrder)

      // insert everything into the tree
      parent.iterator(split, context).foreach{ case (g, v) => tree.insert(g, (g,v)) }

      // build the tree
      tree.build()

      // query tree and perform candidates check
      tree.query(qry).filter{ case (g, _) => predicateFunc(g, qry) }
    }

  }
}

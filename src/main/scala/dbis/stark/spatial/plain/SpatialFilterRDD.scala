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
  qry: STObject,
  predicate: JoinPredicate,
  treeOrder: Int = -1) extends SpatialRDD[G,V](parent) {

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
      case sp: SpatialPartitioner => {

        val spatialParts = ListBuffer.empty[Partition]

        val qryEnv = qry.getGeo.getEnvelopeInternal
        var i = 0
        var cnt = 0
        val numParentParts = parent.getNumPartitions
        while (i < numParentParts) {

          val cell = sp.partitionBounds(i)

          if (Utils.toEnvelope(cell.extent).contains(qryEnv)) {
            spatialParts += SpatialPartition(cnt, i, parent)
            cnt += 1
          }

          i += 1
        }

        spatialParts.toArray
//        parent.partitions
      }

      case _ =>
        parent.partitions
    }.getOrElse(parent.partitions)


  @DeveloperApi
  override def compute(inputSplit: Partition, context: TaskContext): Iterator[(G, V)] = {

    // get the function behind the enum value
    val predicateFunc = JoinPredicate.predicateFunction(predicate)

    /* determine the split to process. If a spatial partitoner was applied, the actual
     * partition/split is encapsuled
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

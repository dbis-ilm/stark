package dbis.stark.spatial

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.partitioner.{SpatialPartition, SpatialPartitioner}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * A stpatio-temporal filter implementation
  *
  * @param parent The parent RDD serving as input
  * @param qry The query object used in the filter evaluation
  * @param predicateFunc The predicate to apply in the filter
  * @param treeOrder The (optional) order of the tree. <= 0 to not apply indexing
  * @param checkParties Perform partition check
  * @tparam G The type representing spatio-temporal data
  * @tparam V The payload data
  */
class SpatialFilterRDD[G <: STObject : ClassTag, V : ClassTag] private (
  private val parent: RDD[(G,V)],
  qry: G,
  predicateFunc: (G,G) => Boolean,
  treeOrder: Int,
  private val checkParties: Boolean) extends RDD[(G,V)](parent) {

  /**
    * A stpatio-temporal filter implementation
    *
    * @param parent The parent RDD serving as input
    * @param qry The query object used in the filter evaluation
    * @param predicateFunc The predicate to apply in the filter
    * @return Creates a new instance of this class
    */
  def this(parent: RDD[(G,V)], qry: G, predicateFunc: (G,G) => Boolean) =
    this(parent, qry, predicateFunc, -1, false)

  /**
    * A stpatio-temporal filter implementation
    *
    * @param parent The parent RDD serving as input
    * @param qry The query object used in the filter evaluation
    * @param predicate The predicate to apply in the filter
    * @param treeOrder The (optional) order of the tree. <= 0 to not apply indexing
    * @return Creates a new instance of this class
    */
  def this(parent: RDD[(G,V)], qry: G, predicate: JoinPredicate, treeOrder: Int = -1) =
    this(parent, qry, JoinPredicate.predicateFunction(predicate), treeOrder, true)

  /**
    * The partitioner of this RDD.
    *
    * This will always be set to the parent's partitioner
    */
  override val partitioner: Option[Partitioner] = parent.partitioner

  /**
    * Return the partitions that have to be processed.
    *
    * We apply partition pruning in this step: If a [[SpatialPartitioner]]
    * is present, we check if the partition can contain candidates. If not, it is not returned
    * here.
    *
    *
    * @return The list of partitions of this RDD
    */
  override def getPartitions: Array[Partition] = partitioner.map{
      // check if this is a spatial partitioner
      case sp: SpatialPartitioner if checkParties =>

        val spatialParts = ListBuffer.empty[Partition]

        val qryEnv = qry.getGeo.getEnvelopeInternal
        var parentPartiId = 0
        var spatialPartId = 0
        val numParentParts = parent.getNumPartitions

        // loop over all partitions in the parent
        while (parentPartiId < numParentParts) {

          val cell = sp.partitionBounds(parentPartiId)

          // check if partitions intersect
          if (Utils.toEnvelope(cell.extent).intersects(qryEnv)) {
            // create a new "spatial partition" pointing to the parent partition
            spatialParts += SpatialPartition(spatialPartId, parentPartiId, parent)
            spatialPartId += 1
          }

          parentPartiId += 1
        }

        spatialParts.toArray

      // if it's not a spatial partitioner, we cannot apply partition pruning
      case _ =>
        parent.partitions

    }.getOrElse(parent.partitions) // no partitioner


  @DeveloperApi
  override def compute(inputSplit: Partition, context: TaskContext): Iterator[(G, V)] = {

    /* determine the split to process. If a spatial partitioner was applied, the actual
     * partition/split is encapsulated
     */
    val split = inputSplit match {
      case sp: SpatialPartition => sp.parentPartition
      case _ => inputSplit
    }

    /* distinguish between no and live indexing
     * of tree order is zero or smaller, indexing is _not_ applied.
     *
     * It it is greater than 0, we will perform live indexing
     */
    if(treeOrder <= 0) {

      // simply iterate over all items in this partition and apply the predicate function
      parent.iterator(split, context).filter { case (g, _) => predicateFunc(g, qry) }
    } else {

      // create tree object
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

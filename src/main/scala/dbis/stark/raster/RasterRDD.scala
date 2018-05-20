package dbis.stark.raster

import dbis.stark.STObject
import org.apache.spark.{Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD

/**
  * A base class for representing raster data as RDD
  * @param _parent The parent (input) RDD
  * @param _partitioner The optional partitioner that _was_ used
  */
class RasterRDD[U](@transient private val _parent: RDD[Tile[U]],
                private val _partitioner: Option[Partitioner]) extends RDD[Tile[U]](_parent) {

  /**
    * Create new Raster RDD from a parent (without partitioner)
    * @param _parent The parent
    * @return Returns a new instance of RasterRDD
    */
  def this(_parent: RDD[Tile[U]]) = this(_parent, None)

  /**
    * The partitioner that was used on this RDD
    */
  override val partitioner = _partitioner

  /**
    * Own implementation of partitionBy for RasterRDD.
    *
    * In Spark, partititionBy is only available for Pair-RDDs. Thus, here we simulate this
    * by enhancing it to (tile, 0x0) i.e. we append a zero byte to create a pair RDD.
    * On this we perform the partitioning and then re-transform it back to only tile
    *
    * @param partitioner The partitioner to use
    * @return Returns a partitioned RasterRDD
    */
  def partitionBy(partitioner: RasterGridPartitioner): RasterRDD[U] = {
    val zero: Byte = 0.toByte // the zero byte
    // transform to key-value form where Tile is key and the zero byte our aux-value
    val res = this.map(t => (t, zero))
                  .partitionBy(partitioner) // partition
                  .map(_._1) // transform back to remove the aux byte

    // make it a raster RDD with the given partitioner information
    new RasterRDD(res, Some(partitioner))
  }


  // methods needed to implement - otherwise RasterRDD must be abstract
  override def compute(split: Partition, context: TaskContext) = firstParent[Tile[U]].compute(split, context)
  override protected def getPartitions = firstParent.partitions

  /**
    * Filter the RasterRDD so that it contains only Tiles intersecting with the given region
    * @param qry The query region
    * @return Returns
    */
  def filter(qry: STObject) = new RasterFilterVectorRDD(qry, this)
}

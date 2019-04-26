package dbis.stark.raster

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.Index
import dbis.stark.spatial.partitioner.{OneToManyPartition, OneToOnePartition}
import dbis.stark.spatial.{JoinPredicate, JoinRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

case class RasterJoinIndexedVectorRDD[U<% Ordered[U]: ClassTag, P: ClassTag]
  (_left: RasterRDD[U], _right: RDD[Index[P]],
  predicate: JoinPredicate, pixelDefault: U, oneToMany: Boolean = false)
  extends JoinRDD[Tile[U], Index[P], (Tile[U], P)](_left,_right, oneToMany, true) { //RDD[(Tile[U],P)](left.context, Nil) {


  private def doCompute(leftP: Partition, rightP: Partition, context: TaskContext) = {
    right.iterator(rightP,context).flatMap{ index =>
      // put all tiles into the tree
      left.iterator(leftP, context).flatMap{ tile =>
        index.query(RasterUtils.tileToGeo(tile))
          .map{p =>
            //            val matchingTilePart = RasterUtils.getPixels(tile, rg.getGeo, isIntersects, pixelDefault)
            (tile, p)}

      }
    }
  }


  override protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext) =
    doCompute(partition.leftPartition, partition.rightPartition, context)


  override protected def computeWithOneToMany(partition: OneToManyPartition, context: TaskContext) =
    partition.rightPartitions.iterator.flatMap { rp =>
      doCompute(partition.leftPartition, rp, context)
    }
}

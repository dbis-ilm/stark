package dbis.stark.raster

import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.indexed.{IndexConfig, IndexFactory}
import dbis.stark.spatial.partitioner.{OneToManyPartition, OneToOnePartition}
import dbis.stark.spatial.{JoinPredicate, JoinRDD}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RasterJoinVectorRDD[U<% Ordered[U]: ClassTag, P: ClassTag] (_left: RasterRDD[U],
                                                     _right: RDD[(STObject, P)],
                                                     predicate: JoinPredicate,
                                                     pixelDefault: U,
                                                     indexConfig: Option[IndexConfig],
                                                     oneToMany: Boolean = false)
  extends JoinRDD[Tile[U], (STObject, P), (Tile[U],P)](_left, _right, oneToMany, true) { //RDD[(Tile[U],P)](left.context, Nil) {

  private val isIntersects = predicate == JoinPredicate.INTERSECTS
  private val predicateFunc = JoinPredicate.spatialPredicateFunction(predicate)


  override protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext) = {

    if(indexConfig.isEmpty) {
      // loop over the left partition and check join condition on every element in the right partition's array
      left.iterator(partition.leftPartition, context).flatMap { t =>

        val tileGeom = RasterUtils.tileToGeo(t)

        right.iterator(partition.rightPartition, context).filter { case (rg, _) =>
          predicateFunc(tileGeom, rg)
        }.map { case (rg, rv) =>
          // get only covered/intersected pixels
          val matchingTilePart = RasterUtils.getPixels(t, rg.getGeo, isIntersects, pixelDefault)
          (matchingTilePart, rv)
        }
      }

    } else {
      val index = IndexFactory.get[Tile[U]](indexConfig.get)

      left.iterator(partition.leftPartition, context).foreach{ t =>
        val tileMBRGeo = RasterUtils.tileToGeo(t)
        index.insert(tileMBRGeo, t)
      }

      index.build()

      right.iterator(partition.rightPartition,context).flatMap{ case (rg, rv) =>
        // put all tiles into the tree
        index.query(rg).map{t =>
          val matchingTilePart = RasterUtils.getPixels(t, rg.getGeo, isIntersects, pixelDefault)
          (matchingTilePart, rv)}
      }

    }

  }

  override protected def computeWithOneToMany(partition: OneToManyPartition, context: TaskContext) = {
//    List.empty[(Tile[U],P)].iterator
    if(indexConfig.isEmpty) {

      left.iterator(partition.leftPartition, context).flatMap{ t =>
        val tileGeom = RasterUtils.tileToGeo(t)

        partition.rightPartitions.iterator.flatMap{ rp =>
          right.iterator(rp, context).filter { case (rg, _) =>
            predicateFunc(STObject(tileGeom), rg)
          }.map { case (rg, rv) =>
            // get only covered/intersected pixels
            val matchingTilePart = RasterUtils.getPixels(t, rg.getGeo, isIntersects, pixelDefault)
            (matchingTilePart, rv)
//            (t,rv)
          }
        }
      }
    } else {
      val index = IndexFactory.get[Tile[U]](indexConfig.get)

      left.iterator(partition.leftPartition, context).foreach{ t =>
        val tileMBRGeo = RasterUtils.tileToGeo(t)
        index.insert(tileMBRGeo, t)
      }

      index.build()

      partition.rightPartitions.iterator.flatMap { rp =>
        right.iterator(rp,context).flatMap{ case (rg, rv) =>
          // put all tiles into the tree
          index.query(rg).map { t =>
//              val matchingTilePart = RasterUtils.getPixels(t, rg.getGeo, isIntersects, pixelDefault)
//              (matchingTilePart, rv)
            (t, rv)
          }
        }
      }

//      List.empty[(Tile[U], P)].iterator

    }
  }
}

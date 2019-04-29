package dbis.stark.raster

import dbis.stark.spatial.JoinPredicate.JoinPredicate
import dbis.stark.spatial.JoinRDD
import dbis.stark.spatial.partitioner.{OneToManyPartition, OneToOnePartition}
import org.apache.spark.TaskContext

import scala.reflect.ClassTag

case class RasterJoinRDD[U : ClassTag,P : ClassTag,R : ClassTag](@transient private val _left: RasterRDD[U],
                                                                 @transient private val _right: RasterRDD[P],
                                                                 predicate: JoinPredicate, combine: (U, P) => R,
                                                                 oneToMany: Boolean)(implicit val ord1: Ordering[U],
                                                                                     implicit val ord2: Ordering[P],
                                                                                     implicit val ord3: Ordering[R])
  extends JoinRDD[Tile[U], Tile[P], Tile[R]](_left, _right, oneToMany, checkParties = true) {


  override protected def computeWithOneToOnePartition(partition: OneToOnePartition, context: TaskContext): Iterator[Tile[R]] = {

    left.iterator(partition.leftPartition, context).flatMap{ lTile =>

      right.iterator(partition.rightPartition, context).map { rTile =>
        val intersection = RasterUtils.tileToMBR(lTile).intersection(RasterUtils.tileToMBR(rTile))

        def computePixelValue(posX: Double, posY: Double): R = {

          val lValue = lTile.value(posX, posY)
          val rValue = rTile.value(posX, posY)

          combine(lValue, rValue)
        }



        val intersectionTile = RasterUtils.mbrToTile(intersection, computePixelValue _, lTile.pixelWidth)
        //        //TODO: maybe use Array.tabulate?
//        var i = 0
//        var j = 0
//        while(i < intersectionTile.width) {
//
//          j = 0
//          while(j < intersectionTile.height) {
//
//            val (posX, posY) = intersectionTile.posFromColRow(i,j)
//
//            val lValue = lTile.value(posX, posY)
//            val rValue = rTile.value(posX, posY)
//
//            intersectionTile.setArray(i,j, combine(lValue, rValue))
//
//            j += 1
//          }
//
//          i += 1
//        }

        intersectionTile
      }

    }


  }

  override protected def computeWithOneToMany(partition: OneToManyPartition, context: TaskContext): Iterator[Tile[R]] = ???
}

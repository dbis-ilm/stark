package dbis.stark

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import dbis.stark.spatial._
import dbis.stark.spatial.partitioner.CellHistogram
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.{AbstractNode, ItemBoundable, RTree}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class SkylineSerializer extends Serializer[Skyline[Any]] {
  val stobjectSerializer = new STObjectSerializer

  override def write(kryo: Kryo, output: Output, skyline: Skyline[Any]): Unit = {
    output.writeInt(skyline.skylinePoints.length, true)
    skyline.skylinePoints.foreach { case (so,v) =>
      kryo.writeObject(output, so, stobjectSerializer)
      kryo.writeClassAndObject(output, v)
    }
    kryo.writeClassAndObject(output, skyline.dominatesFunc)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Skyline[Any]]): Skyline[Any] = {
    val num = input.readInt(true)
    var i = 0
    val l = ListBuffer.empty[(STObject, Any)]
    while(i < num) {
      val so = kryo.readObject(input, classOf[STObject], stobjectSerializer)
      val v = kryo.readClassAndObject(input)
      l += ((so,v))

      i += 1
    }
    val func = kryo.readClassAndObject(input).asInstanceOf[(STObject,STObject) => Boolean]

    new Skyline[Any](l.toList, func)
  }
}

object DistanceSerializer {
  private val SCALARDIST: Byte = 0x0
  private val INTERVALDIST: Byte = 0x1
}

class DistanceSerializer extends Serializer[Distance] {
  override def write(kryo: Kryo, output: Output, dist: Distance): Unit = dist match {
    case sd: ScalarDistance =>
      output.writeByte(DistanceSerializer.SCALARDIST)
      output.writeDouble(sd.value)
    case IntervalDistance(min,max) =>
      output.writeByte(DistanceSerializer.INTERVALDIST)
      output.writeDouble(min)
      output.writeDouble(max)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Distance]): Distance = input.readByte() match {
    case DistanceSerializer.SCALARDIST =>
      val v = input.readDouble()
      ScalarDistance(v)
    case DistanceSerializer.INTERVALDIST =>
      val min = input.readDouble()
      val max = input.readDouble()
      IntervalDistance(min, max)
  }
}

class KnnSerializer extends Serializer[KNN[Any]] {
  val distSerializer = new DistanceSerializer
  override def write(kryo: Kryo, output: Output, knn: KNN[Any]): Unit = {

    output.writeInt(knn.k, true)
    output.writeInt(knn.posMax, true)
    output.writeInt(knn.posMin, true)
    output.writeInt(knn.m, true)

    var firstNull = knn.nn.indexWhere(_ == null)
    firstNull = if(firstNull < 0) knn.k else firstNull

    output.writeInt(firstNull, true)

    var i = 0
    while(i < firstNull) {
      val (dist,v) = knn.nn(i)
      kryo.writeObject(output, dist, distSerializer)
      kryo.writeClassAndObject(output, v)

      i += 1
    }

  }

  override def read(kryo: Kryo, input: Input, `type`: Class[KNN[Any]]): KNN[Any] = {
    val k = input.readInt(true)
    val posMax = input.readInt(true)
    val posMin = input.readInt(true)
    val m = input.readInt(true)

    var firstNull = input.readInt(true)
    firstNull = if(firstNull < 0) k else firstNull

    val objs = new Array[(Distance,Any)](k)

    var i = 0
    while(i < firstNull) {
      val dist = kryo.readObject(input, classOf[Distance], distSerializer)
      val v = kryo.readClassAndObject(input).asInstanceOf[Any]

      objs(i) = (dist,v)
      i += 1
    }


    val knn = new KNN[Any](k)
    knn.m = m
    knn.posMin = posMin
    knn.posMax = posMax
    knn.nn = objs

//    println(s"\ndeserialied $knn")

    knn
  }
}

//class RTreeSerializer extends Serializer[RTree[Any]] {
//  val soSerializer = new STObjectSerializer
//  override def write(kryo: Kryo, output: Output, tree: RTree[Any]): Unit = {
//    tree.build()
//
//    output.writeInt(tree.getNodeCapacity, true)
//    output.writeInt(tree.size(), true)
//
//    tree._items.foreach{ d =>
//      kryo.writeObject(output, d.so, soSerializer)
//      kryo.writeClassAndObject(output, d.data)
//    }
//  }
//
//  override def read(kryo: Kryo, input: Input, dType: Class[RTree[Any]]): RTree[Any] = {
//    val capacity = input.readInt(true)
//    val size = input.readInt(true)
//
//    val tree = new RTree[Any](capacity)
//
//    var i = 0
//    while(i < size) {
//      val so = kryo.readObject(input, classOf[STObject], soSerializer)
//      val data = kryo.readClassAndObject(input).asInstanceOf[Any]
//
//      tree.insert(so, data)
//      i += 1
//    }
//
//    tree.build()
//    tree
//  }
//}


class RTreeSerializer extends Serializer[RTree[Any]] {

  private def writeTree(kryo: Kryo, output: Output, node: AbstractNode): Unit = {
    output.writeInt(node.getLevel, true)
    import scala.collection.JavaConverters._
    val children = node.getChildBoundables.asScala
    output.writeInt(children.size, true)

    if(children.nonEmpty) {
      /*
       * here we have two cases for the children, they are either
       *  1. inner nodes : AbstractNode
       *  2. leaf nodes  : ItemBoundable
       *
       * We can decide based on the first child.
       *
       * For each inner node, we have to make a recursive call to this method and write all its children
       */

      val isInnerNode = children.head.isInstanceOf[AbstractNode]
      output.writeBoolean(isInnerNode)

      if(isInnerNode) {
        // recursively write all of its children
        children.foreach(child => writeTree(kryo, output, child.asInstanceOf[AbstractNode]))
      } else {
        // write leaf nodes
        children.foreach{ child =>
          val ib = child.asInstanceOf[ItemBoundable]
          kryo.writeObject(output, ib.getBounds, new EnvelopeSerializer)
          kryo.writeClassAndObject(output, ib.getItem)
        }
      }
    }
  }

  override def write(kryo: Kryo, output: Output, tree: RTree[Any]): Unit = {
//    println("kryo write tree")
    output.writeInt(tree.getNodeCapacity, true)
    val empty = tree.isEmpty
    output.writeBoolean(empty)

    if(!empty) {
      // ensure tree is built
      tree.build()

      // recursively write tree
      writeTree(kryo, output, tree.getRoot)
    }
  }

  private def readNode(kryo: Kryo, input: Input, tree: RTree[Any]): AbstractNode = {
    val level = input.readInt(true)
    val numChildren = input.readInt(true)

    val isInnerNode = input.readBoolean()
    val n = tree.createInnerNode(level)


    if(isInnerNode) {
      // recursively read every child node
      var i = 0
      while(i < numChildren) {
        val child = readNode(kryo, input, tree)
        n.addChildBoundable(child)
        i += 1
      }
    } else {
      // read the envelope and data of each leaf child
      var i = 0
      while(i < numChildren) {
        val env = kryo.readObject(input, classOf[Envelope], new EnvelopeSerializer)
        val data = kryo.readClassAndObject(input)
        val ib = new ItemBoundable(env, data)
        n.addChildBoundable(ib)
        i += 1
      }
    }

    n
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[RTree[Any]]): RTree[Any] = {
//    println("kryo read tree")
    val capacity = input.readInt(true)
    val empty = input.readBoolean()
    val tree = new RTree[Any](capacity)
    if(!empty) {
      val root = readNode(kryo, input, tree)
      tree.setRoot(root)
    }
    tree.build()
    tree
  }
}


class NPointSerializer extends Serializer[NPoint] {
  override def write(kryo: Kryo, output: Output, p: NPoint): Unit = {
    output.writeInt(p.dim, true)
    var i = 0
    while(i < p.dim) {
      output.writeDouble(p(i))
      i += 1
    }
  }

  override def read(kryo: Kryo, input: Input, dType: Class[NPoint]): NPoint = {
    val dim = input.readInt(true)
    var i = 0
    val arr = new Array[Double](dim)
    while(i < dim) {
      arr(i) = input.readDouble()
      i += 1
    }

    NPoint(arr)
  }
}

class NRectSerializer extends Serializer[NRectRange] {
  val pointSer = new NPointSerializer()

  override def write(kryo: Kryo, output: Output, rect: NRectRange): Unit = {
    pointSer.write(kryo, output, rect.ll)
    pointSer.write(kryo, output, rect.ur)
  }

  override def read(kryo: Kryo, input: Input, dType: Class[NRectRange]): NRectRange = {
    val ll = pointSer.read(kryo, input, classOf[NPoint])
    val ur = pointSer.read(kryo, input, classOf[NPoint])
    NRectRange(ll, ur)
  }
}

class CellSerializer extends Serializer[Cell] {
  val rectSerializer = new NRectSerializer
  override def write(kryo: Kryo, output: Output, cell: Cell) = {
    output.writeInt(cell.id, true)
    kryo.writeObject(output, cell.range, rectSerializer)
    val same = cell.range == cell.extent
    output.writeBoolean(same)
    if(!same)
      kryo.writeObject(output, cell.extent, rectSerializer)
  }

  override def read(kryo: Kryo, input: Input, dType: Class[Cell]) = {
    val id = input.readInt(true)
    val range = kryo.readObject(input, classOf[NRectRange], rectSerializer)
    val same = input.readBoolean()
    if(!same) {
      val extent = kryo.readObject(input, classOf[NRectRange], rectSerializer)
      Cell(id, range, extent)
    }
    else
      Cell(id, range)
  }
}

class HistogramSerializer extends Serializer[CellHistogram] {
  val cellSerializer = new CellSerializer
  override def write(kryo: Kryo, output: Output, histo: CellHistogram) = {

    output.writeInt(histo.buckets.size, true)

    histo.buckets.iterator.foreach{ case (cellId, (cell, cnt)) =>
      cell.id = cellId
      kryo.writeObject(output, cell, cellSerializer)
      output.writeInt(cnt, true)
    }

//    output.writeInt(histo.nonEmptyCells.size)
//    histo.nonEmptyCells.iterator.foreach( cellId => output.writeInt(cellId, true))

  }

  override def read(kryo: Kryo, input: Input, `type`: Class[CellHistogram]) = {
    val num = input.readInt(true)
    val buckets = mutable.Map.empty[Int, (Cell, Int)]

    var i = 0
    while(i < num) {
      val cell = kryo.readObject(input, classOf[Cell], cellSerializer)
      val cnt = input.readInt(true)

      buckets += cell.id -> (cell, cnt)

      i += 1
    }

//    val numNonEmptyCells = input.readInt(true)
//    val nonEmptyCells = mutable.Set.empty[Int]
//    i = 0
//    while(i < numNonEmptyCells) {
//      val cellId = input.readInt(true)
//      nonEmptyCells += cellId
//      i += 1
//    }

    CellHistogram(buckets)
  }
}

class EnvelopeSerializer extends Serializer[Envelope] {
  override def write(kryo: Kryo, output: Output, obj: Envelope): Unit = {
    output.writeDouble(obj.getMinX)
    output.writeDouble(obj.getMinY)
    output.writeDouble(obj.getMaxX)
    output.writeDouble(obj.getMaxY)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Envelope]): Envelope = {
    val minX = input.readDouble()
    val minY = input.readDouble()
    val maxX = input.readDouble()
    val maxY = input.readDouble()

    new Envelope(minX, maxX, minY, maxY)
  }
}

object GeometrySerializer {
  val POINT: Byte = 0
  val POLYGON: Byte = 1
  val LINESTRING: Byte = 2
  val MULTIPOLYGON: Byte = 3
  val RECTANGLE: Byte = 4
}

//class GeometryAsStringSerializer extends Serializer[Geometry] {
//  override def write(kryo: Kryo, output: Output, geom: Geometry) = {
//    val writer = new WKTWriter(2)
//    val offset = geom match {
//      case _:Point =>
//        output.writeByte(GeometrySerializer.POINT)
//        "point".length
//      case _:LineString =>
//        output.writeByte(GeometrySerializer.LINESTRING)
//        "linestring".length
//      case _:Polygon =>
//        output.writeByte(GeometrySerializer.POLYGON)
//        "polygon".length
//    }
//
//    val txt = writer.write(geom)
//    val toWrite = txt.substring(offset, txt.length - 1)
//    output.writeString(toWrite)
//  }
//
//  override def read(kryo: Kryo, input: Input, `type`: Class[Geometry]) = {
//    val reader = new WKTReader()
//
//    val geoType = input.readByte()
//    val str = input.readString()
//
//    val strType = geoType match {
//      case GeometrySerializer.POINT => "POINT("
//      case GeometrySerializer.LINESTRING => "LINESTRING("
//      case GeometrySerializer.POLYGON => "POLYGON("
//    }
//
//    reader.read(s"$strType$str)")
//  }
//}

class PointSerializer extends Serializer[Point] {

  private val geometryFactory = new GeometryFactory()
  override def write(kryo: Kryo, output: Output, point: Point): Unit = {
    val p = point.getCoordinate
    output.writeDouble(p.x)
    output.writeDouble(p.y)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Point]): Point = {
    val x = input.readDouble()
    val y = input.readDouble()

    geometryFactory.createPoint(new Coordinate(x,y))
  }
}

class LineStringSerializer extends Serializer[LineString] {

  private val geometryFactory = new GeometryFactory()
  override def write(kryo: Kryo, output: Output, line: LineString): Unit = {
    val points = line.getCoordinates
    var i = 0
    output.writeInt(points.length, true)
    while(i < points.length) {
      val p = points(i)
      output.writeDouble(p.x)
      output.writeDouble(p.y)

      i += 1
    }
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[LineString]): LineString = {
    val num = input.readInt(true)

    val coords = new Array[Coordinate](num)

    var i = 0
    while(i < num) {

      val x = input.readDouble()
      val y = input.readDouble()

      coords(i) = new Coordinate(x,y)

      i += 1
    }

    geometryFactory.createLineString(coords)
  }
}

class PolygonSerializer extends Serializer[Polygon] {

  private val geometryFactory = new GeometryFactory()
  override def write(kryo: Kryo, output: Output, polygon: Polygon): Unit = {

    val extRing = polygon.getExteriorRing

    val points = extRing.getCoordinates

    output.writeInt(points.length, true)

    var i = 0
    while(i < points.length) {
      val p = points(i)
      output.writeDouble(p.x)
      output.writeDouble(p.y)

      i += 1
    }

//    var ring = 0
//    output.writeInt(polygon.getNumInteriorRing, true)
//    while(ring < polygon.getNumInteriorRing) {
//      val innerRing = polygon.getInteriorRingN(ring)
//
//      kryo.writeObject(output, innerRing)
//
//      ring += 1
//    }

  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Polygon]): Polygon = {

    val num = input.readInt(true)

    val coords = new Array[Coordinate](num)

    var i = 0
    while(i < num) {
      val x = input.readDouble()
      val y = input.readDouble()

      coords(i) = new Coordinate(x,y)

      i += 1
    }

    geometryFactory.createPolygon(coords)
  }
}

class GeometrySerializer extends Serializer[Geometry] {

  private val geometryFactory = new GeometryFactory()

  private def writePoint(x: Double, y: Double, output: Output): Unit = {
    output.writeDouble(x)
    output.writeDouble(y)
  }

  private def writeRectangle(r: Rectangle, output: Output) = {
    val pts = r.points
    output.writeDouble(pts(0).x)
    output.writeDouble(pts(0).y)
    output.writeDouble(pts(1).x)
    output.writeDouble(pts(1).y)
  }

  private def writeLineString(l: LineString, output: Output) = {
    val nPoints = l.getNumPoints
    output.writeInt(nPoints, true)
    var i = 0
    while(i < nPoints) {
      val p = l.getPointN(i)
      writePoint(p.getX, p.getY, output)
      i += 1
    }
  }

  private def readPoint(input: Input): Point = {
    val c = readPointInternal(input)
    geometryFactory.createPoint(c)
  }

  private def readRectangle(input: Input): Rectangle = {
    val llx = input.readDouble()
    val lly = input.readDouble()
    val urx = input.readDouble()
    val ury = input.readDouble()

    new Rectangle(new Coordinate(llx, lly), new Coordinate(urx, ury))
  }


  private def readPointInternal(input: Input): Coordinate = {
    val x = input.readDouble()
    val y = input.readDouble()
    new Coordinate(x,y)
  }


  override def write(kryo: Kryo, output: Output, obj: Geometry): Unit = obj match {
    case p: Point =>
      output.writeByte(GeometrySerializer.POINT)
      writePoint(p.getX, p.getY, output)
    case l: LineString =>
      output.writeByte(GeometrySerializer.LINESTRING)
      writeLineString(l, output)
    case r: Rectangle =>
      output.writeByte(GeometrySerializer.RECTANGLE)
      writeRectangle(r, output)
    case p: Polygon =>
      output.writeByte(GeometrySerializer.POLYGON)
      val extRing = p.getExteriorRing
      writeLineString(extRing, output)
    case mp: MultiPolygon =>
      output.writeByte(GeometrySerializer.MULTIPOLYGON)
      output.writeInt(mp.getNumGeometries,true)
      var i = 0
      while(i < mp.getNumGeometries) {
        val polyExt = mp.getGeometryN(i).asInstanceOf[Polygon].getExteriorRing
        writeLineString(polyExt, output)

        i += 1
      }
  }

  private def readPoints(input: Input): Array[Coordinate] = {
    val nPoints = input.readInt(true)
    val points = new Array[Coordinate](nPoints)
    var i = 0
    while(i < nPoints) {
      val c = readPointInternal(input)
      points(i) = c
      i += 1
    }

    points
  }

  override def read(kryo: Kryo, input: Input, dType: Class[Geometry]): Geometry = input.readByte() match {
    case GeometrySerializer.POINT =>
      readPoint(input)

    case GeometrySerializer.LINESTRING =>
      geometryFactory.createLineString(readPoints(input))

    case GeometrySerializer.RECTANGLE =>
      readRectangle(input)

    case GeometrySerializer.POLYGON =>
      geometryFactory.createPolygon(readPoints(input))
    case GeometrySerializer.MULTIPOLYGON =>
      val num = input.readInt(true)
      var i = 0
      val polies = new Array[Polygon](num)
      while(i < num) {
        polies(i) = geometryFactory.createPolygon(readPoints(input))
        i += 1
      }
      geometryFactory.createMultiPolygon(polies)
  }
}

object TemporalSerializer {
  private val INSTANT: Byte = 0x0
  private val INTERVAL: Byte = 0x1
}

class TemporalSerializer extends Serializer[TemporalExpression] {
  override def write(kryo: Kryo, output: Output, obj: TemporalExpression): Unit = obj match {
    case Instant(t) =>
      output.writeByte(TemporalSerializer.INSTANT)
      output.writeLong(t, true)
    case Interval(start, end) =>
      output.writeByte(TemporalSerializer.INTERVAL)
      kryo.writeObject(output, start, this)
      output.writeBoolean(end.isDefined)
      if(end.isDefined)
        kryo.writeObject(output, end.get, this)

  }

  override def read(kryo: Kryo, input: Input, dType: Class[TemporalExpression]): TemporalExpression = {
    input.readByte() match {
      case TemporalSerializer.INSTANT =>
        val l = input.readLong(true)
        Instant(l)
      case TemporalSerializer.INTERVAL =>
        val start = kryo.readObject(input, classOf[Instant], this)
        val hasEnd = input.readBoolean()
        val end: Option[Instant] = if(hasEnd) {
          val theEnd = kryo.readObject(input, classOf[Instant], this)
          Some(theEnd)
        }
        else {
          None
        }
        Interval(start, end)

    }
  }
}

//class DistSeqSerializer extends Serializer[List[(Distance, (STObject, Any))]] {
//
//  val distanceSerializer = new DistanceSerializer
//  val soSerializer = new STObjectSerializer
//
//  override def write(kryo: Kryo, output: Output, seq: List[(Distance, (STObject, Any))]) = {
//    println("write seq")
//    output.writeInt(seq.length, true)
//    seq.foreach{ case (d, (g,v)) =>
//      kryo.writeObject(output, d, distanceSerializer)
//      kryo.writeObject(output, g, soSerializer)
//      kryo.writeClassAndObject(output, v)
//    }
//  }
//
//  override def read(kryo: Kryo, input: Input, `type`: Class[List[(Distance, (STObject, Any))]]) = {
//    println("read seq")
//    val n = input.readInt(true)
//
//    val l = ListBuffer.empty[(Distance, (STObject, Any))]
//    var i = 0
//    while(i < n) {
//      val dist = kryo.readObject(input, classOf[Distance], distanceSerializer)
//      val so = kryo.readObject(input, classOf[STObject], soSerializer)
//      val v = kryo.readClassAndObject(input).asInstanceOf[Any]
//
//      l += ((dist, (so,v)))
//
//      i += 1
//    }
//
//    l.toList
//  }
//}

class STObjectSerializer extends Serializer[STObject] {

  val geometrySerializer = new GeometrySerializer()
  val temporalSerializer = new TemporalSerializer
  override def write(kryo: Kryo, output: Output, obj: STObject): Unit = {
    kryo.writeObject(output, obj.getGeo)

    output.writeBoolean(obj.getTemp.isDefined)
    if(obj.getTemp.isDefined)
      kryo.writeObject(output, obj.getTemp.get, temporalSerializer)
  }

  override def read(kryo: Kryo, input: Input, dType: Class[STObject]): STObject = {
    val geo = kryo.readObject(input, classOf[Geometry], geometrySerializer)

    val time = if(input.readBoolean()) {
      val t = kryo.readObject(input, classOf[TemporalExpression], temporalSerializer)
      Some(t)
    } else
      None

    STObject(geo, time)
  }
}

class StarkSerializer extends Serializer[(STObject, Any)] {

  val soSerializer = new STObjectSerializer

  override def write(kryo: Kryo, output: Output, obj: (STObject, Any)): Unit = {

    kryo.writeObject(output, obj._1, soSerializer)
    kryo.writeClassAndObject(output, obj._2)
//    output.writeInt(obj._2)
  }

  override def read(kryo: Kryo, input: Input, dType: Class[(STObject,Any)]): (STObject,Any) = {
    val so = kryo.readObject(input, classOf[STObject], soSerializer)
    kryo.reference(so)
    val payload = kryo.readClassAndObject(input)
//    val payload = input.readInt()

    (so, payload)
  }
}

class GenericSerializer[U : ClassTag] extends Serializer[U] {
  override def write(kryo: Kryo, output: Output, obj: U) = {
    kryo.writeClassAndObject(output, obj)
  }


  override def read(kryo: Kryo, input: Input, dType: Class[U]) = {
    kryo.readClassAndObject(input).asInstanceOf[U]
  }
}

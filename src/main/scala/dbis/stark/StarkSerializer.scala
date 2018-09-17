package dbis.stark

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import dbis.stark.spatial.indexed.RTree
import dbis.stark.spatial.{NPoint, NRectRange}
import org.locationtech.jts.geom._

import scala.reflect.ClassTag

class RTreeSerializer extends Serializer[RTree[Any]] {
  val soSerializer = new STObjectSerializer
  override def write(kryo: Kryo, output: Output, tree: RTree[Any]): Unit = {
    tree.build()

    output.writeInt(tree.getNodeCapacity, true)
    output.writeInt(tree.size(), true)

    tree._items.foreach{ d =>
      kryo.writeObject(output, d.so, soSerializer)
      kryo.writeClassAndObject(output, d.data)
    }
  }

  override def read(kryo: Kryo, input: Input, dType: Class[RTree[Any]]): RTree[Any] = {
    val capacity = input.readInt(true)
    val size = input.readInt(true)

    val tree = new RTree[Any](capacity)

    var i = 0
    while(i < size) {
      val so = kryo.readObject(input, classOf[STObject], soSerializer)
      val data = kryo.readClassAndObject(input).asInstanceOf[Any]

      tree.insert(so, data)
      i += 1
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
  private val POINT: Byte = 0x0
  private val POLYGON: Byte = 0x1
  private val LINESTRING: Byte = 0x2
}

class GeometrySerializer extends Serializer[Geometry] {

  private val geometryFactory = new GeometryFactory()

  private def writePoint(x: Double, y: Double, output: Output): Unit = {
    output.writeDouble(x)
    output.writeDouble(y)
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
    case p: Polygon =>
      output.writeByte(GeometrySerializer.POLYGON)
      val extRing = p.getExteriorRing
      writeLineString(extRing, output)
  }

  override def read(kryo: Kryo, input: Input, dType: Class[Geometry]): Geometry = input.readByte() match {
    case GeometrySerializer.POINT =>
      readPoint(input)

    case GeometrySerializer.LINESTRING =>
      val nPoints = input.readInt(true)
      val points = new Array[Coordinate](nPoints)
      var i = 0
      while(i < nPoints) {
        val c = readPointInternal(input)
        points(i) = c
        i += 1
      }

      geometryFactory.createLineString(points)

    case GeometrySerializer.POLYGON =>
      val nPoints = input.readInt(true)
      val points = new Array[Coordinate](nPoints)
      var i = 0
      while(i < nPoints) {
        val c = readPointInternal(input)
        points(i) = c
        i += 1
      }

      geometryFactory.createPolygon(points)
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
      output.writeLong(t)
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
        val l = input.readLong()
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

class STObjectSerializer extends Serializer[STObject] {

  val geometrySerializer = new GeometrySerializer()
  val temporalSerializer = new TemporalSerializer
  override def write(kryo: Kryo, output: Output, obj: STObject): Unit = {
    kryo.writeObject(output, obj.getGeo, geometrySerializer)

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

  override def write(kryo: Kryo, output: Output, obj: (STObject, Any)): Unit = {
    kryo.writeObject(output, obj._1, new STObjectSerializer())
    kryo.writeClassAndObject(output, obj._2)
  }

  override def read(kryo: Kryo, input: Input, dType: Class[(STObject,Any)]): (STObject,Any) = {
    val so = kryo.readObject(input, classOf[STObject])
    val payload = kryo.readClassAndObject(input).asInstanceOf[Any]

    (so, payload)
  }
}

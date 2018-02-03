package dbis.stark.visualization

import java.awt.{Color, Graphics2D}
import java.awt.image.BufferedImage
import java.io._
import javax.imageio.ImageIO

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Point, Polygon}
import dbis.stark.STObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


@SerialVersionUID(1L)
class Visualization extends Serializable {
  private var scaleX = .0
  private var scaleY = .0
  private var imageWidth = 0
  private var imageHeight = 0
  private val pixelColor = new Color(255, 0, 0).getRGB
  private val pointSize = 1
  private val fillPolygon = false
  private var flipImageVert = false

  @throws[Exception]
  def visualize[G <: STObject, T](rdd: RDD[(G, T)], imageWidth: Int, imageHeight: Int, envelope: Envelope, flipImageVert: Boolean, outputPath: String, outputType: String): Boolean = {

    this.imageHeight = imageHeight
    this.imageWidth = imageWidth
    this.flipImageVert = flipImageVert

    this.scaleX = imageWidth.toDouble / envelope.getWidth
    this.scaleY = imageHeight.toDouble / envelope.getHeight

    val env = rdd.sparkContext.broadcast(envelope)

    draw(rdd, env, outputPath, outputType)
  }

  private def draw[G <: STObject, T](rdd: RDD[(G, T)], env: Broadcast[Envelope], outputPath: String, outputType: String) = {

    val imgRDD = rdd.mapPartitions{ iter =>
      val imagePartition = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB)
      val imageGraphic = imagePartition.createGraphics
      imageGraphic.setColor(new Color(pixelColor))
      val envelope = env.value

      iter.map{ case (stobj, _) =>
        drawSTObject(stobj, imageGraphic, envelope)
        new ImageSerializableWrapper(imagePartition)
      }
    }

    val finalImage = imgRDD.reduce{ case (v1, v2) =>
      val combinedImage = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB)
      val graphics = combinedImage.getGraphics
      graphics.drawImage(v1.image, 0, 0, null)
      graphics.drawImage(v2.image, 0, 0, null)
      new ImageSerializableWrapper(combinedImage)
    }

    saveImageAsLocalFile(finalImage.image, outputPath, outputType)
  }

  private def saveImageAsLocalFile(finalImage: BufferedImage, outputPath: String, outputType: String) = {
    val outputImage = new File(outputPath + "." + outputType)
    outputImage.getParentFile.mkdirs
    try
      ImageIO.write(finalImage, outputType, outputImage)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    true
  }

  @throws[Exception]
  private def drawSTObject(obj: STObject, g: Graphics2D, env: Envelope): Unit = {
    val geo = obj.getGeo
    geo match {
      case point: Point =>
        val p = getImageCoordinates(point.getCoordinate, env)
        if (p != null) drawPoint(g, p)
      case polygon: Polygon =>
        val coordinates = polygon.getCoordinates
        val poly = new java.awt.Polygon
        for (c <- coordinates) {
          val p = getImageCoordinates(c, env)
          if (p != null) poly.addPoint(p._1, p._2)
        }
        if (fillPolygon) g.fillPolygon(poly)
        else g.drawPolygon(poly)
      case _ => throw new Exception("Unsupported spatial object type. Only supports: Point, Polygon!")
    }
  }

  private def drawPoint(g: Graphics2D, p: Tuple2[Integer, Integer]): Unit = {
    g.fillRect(p._1, p._2, pointSize, pointSize)
  }

  private def getImageCoordinates(p: Coordinate, envelope: Envelope): Tuple2[Integer, Integer] = {
    val x = p.x
    var y = p.y
    if (!envelope.contains(x, y)) return null
    if (flipImageVert) y = envelope.centre.y - (y - envelope.centre.y)
    val resultX = ((x - envelope.getMinX) * scaleX).toInt
    val resultY = ((y - envelope.getMinY) * scaleY).toInt
    new Tuple2[Integer, Integer](resultX, resultY)
  }

  @SerialVersionUID(1L)
  private[visualization] class ImageSerializableWrapper(var image: BufferedImage) extends Serializable {
    // Serialization method.
    @throws[IOException]
    def writeObject(out: ObjectOutputStream): Unit = {
      out.defaultWriteObject()
      ImageIO.write(image, "png", out)
    }

    // Deserialization method.
    @throws[IOException]
    @throws[ClassNotFoundException]
    def readObject(in: ObjectInputStream): Unit = {
      in.defaultReadObject()
      image = ImageIO.read(in)
    }

    def getImage: BufferedImage = this.image
  }

}
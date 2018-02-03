package dbis.stark.visualization;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import dbis.stark.STObject;
import scala.Tuple2;

public class JVisualization implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double scaleX, scaleY;
	private int imageWidth, imageHeight;
	private int pixelColor = new Color(255, 0, 0).getRGB();
	private int pointSize = 1;
	private boolean fillPolygon;
	private boolean flipImageVert;

	public <G extends STObject, T> boolean visualize(JavaSparkContext sparkContext, JavaRDD<Tuple2<G, T>> rdd,
			int imageWidth, int imageHeight, Envelope envelope, boolean flipImageVert, String outputPath, String outputType) throws Exception {
		
		this.imageHeight = imageHeight;
		this.imageWidth = imageWidth;
		this.flipImageVert = flipImageVert;

		scaleX = (double) imageWidth / envelope.getWidth();
		scaleY = (double) imageHeight / envelope.getHeight();

		Broadcast<Envelope> env = sparkContext.broadcast(envelope);

		draw(rdd, env, outputPath, outputType);

		return true;
	}

	private <G extends STObject,T> boolean draw(JavaRDD<Tuple2<G, T>> rdd, Broadcast<Envelope> env, String outputPath, String outputType) {
		JavaRDD<ImageSerializableWrapper> imgRdd = rdd.mapPartitions(new FlatMapFunction<Iterator<Tuple2<G, T>>, ImageSerializableWrapper>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<ImageSerializableWrapper> call(Iterator<Tuple2<G, T>> it) throws Exception {
				BufferedImage imagePartition = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
				Graphics2D imageGraphic = imagePartition.createGraphics();
				imageGraphic.setColor(new Color(pixelColor));
				Envelope envelope = env.value();

				ArrayList<ImageSerializableWrapper> result = new ArrayList<ImageSerializableWrapper>();

				while(it.hasNext()) {
					Tuple2<G, T> tupel = it.next();
					drawSTObject(tupel._1, imageGraphic, envelope);
				}

				result.add(new ImageSerializableWrapper(imagePartition));

				return result.iterator();
			}
		});

		BufferedImage finalImage = imgRdd.reduce(new Function2<ImageSerializableWrapper, ImageSerializableWrapper, ImageSerializableWrapper>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public ImageSerializableWrapper call(ImageSerializableWrapper v1, ImageSerializableWrapper v2) throws Exception {
                BufferedImage combinedImage = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
                Graphics graphics = combinedImage.getGraphics();
                graphics.drawImage(v1.image, 0, 0, null);
                graphics.drawImage(v2.image, 0, 0, null);
				return new ImageSerializableWrapper(combinedImage);
			}
		}).image;

		return saveImageAsLocalFile(finalImage, outputPath, outputType);
	}

	private boolean saveImageAsLocalFile(BufferedImage finalImage, String outputPath, String outputType) {
		File outputImage = new File(outputPath + "." + outputType);
		System.out.println("write to file: "+outputImage);
//		outputImage.getParentFile().mkdirs();
		try {
			ImageIO.write(finalImage, outputType, outputImage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	private void drawSTObject(STObject obj, Graphics2D g, Envelope env) throws Exception {
		Geometry geo = obj.getGeo();

		if(geo instanceof Point) {
			Tuple2<Integer, Integer> p = getImageCoordinates(((Point) geo).getCoordinate(), env);
			if(p != null) drawPoint(g, p);
		} else if(geo instanceof Polygon) {
			Coordinate[] coordinates = ((Polygon) geo).getCoordinates();
			java.awt.Polygon poly = new java.awt.Polygon();
			for(Coordinate c : coordinates) {
				Tuple2<Integer, Integer> p = getImageCoordinates(c, env);
				if(p != null) poly.addPoint(p._1, p._2);
			}

			if(fillPolygon)	g.fillPolygon(poly);
			else g.drawPolygon(poly);
		} else {
			throw new Exception("Unsupported spatial object type. Only supports: Point, Polygon!");
		}
	}

	private void drawPoint(Graphics2D g, Tuple2<Integer, Integer> p) {
		g.fillRect(p._1, p._2, pointSize, pointSize);
	}

	private Tuple2<Integer, Integer> getImageCoordinates(Coordinate p, Envelope envelope) {
		double x = p.x;
		double y = p.y;

		if(!envelope.contains(x, y)) return null;

		if(flipImageVert) {
			y = envelope.centre().y - (y - envelope.centre().y);
		}

		int resultX = (int) ((x - envelope.getMinX()) * scaleX);
		int resultY = (int) ((y - envelope.getMinY()) * scaleY);

		return new Tuple2<Integer, Integer>(resultX, resultY);
	}

	class ImageSerializableWrapper implements Serializable {
		  
		  /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

		  protected transient BufferedImage image;
		  
		  public ImageSerializableWrapper(BufferedImage image) {
		      this.image = image;
		  }

		  // Serialization method.
		  private void writeObject(ObjectOutputStream out) throws IOException {
		      out.defaultWriteObject();
		      ImageIO.write(image, "png", out);
		  }

		  // Deserialization method.
		  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		      in.defaultReadObject();
		      image = ImageIO.read(in);
		  }

		  public BufferedImage getImage() {
		      return this.image;
		  }
		}
}
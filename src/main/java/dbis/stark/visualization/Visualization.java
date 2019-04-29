package dbis.stark.visualization;

import dbis.stark.raster.Tile;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import dbis.stark.STObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Collections;
import java.util.stream.StreamSupport;

public class Visualization implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private double scaleX, scaleY;
    private int imageWidth, imageHeight;
    private int pixelColor = new Color(255, 0, 0).getRGB();
    private boolean fillPolygon;
    private boolean flipImageVert;
    private boolean worldProj;

    public <G extends STObject, T> boolean visualize(JavaSparkContext sparkContext, JavaRDD<Tuple2<G, T>> rdd,
                                                     int imageWidth, int imageHeight, Envelope envelope,
                                                     boolean flipImageVert, String outputPath, String outputType) {

        return visualize(sparkContext, rdd, imageWidth, imageHeight, envelope, flipImageVert, outputPath, outputType, null, 1, false, false);
    }

    public <G extends STObject, T> boolean visualize(JavaSparkContext sparkContext, JavaRDD<Tuple2<G, T>> rdd,
                                                     int imageWidth, int imageHeight, Envelope envelope,
                                                     boolean flipImageVert, String outputPath, String outputType,
                                                     String bgImagePath, int pointSize, boolean fillPolygon, boolean worldProj) {

        this.imageHeight = imageHeight;
        this.imageWidth = imageWidth;
        this.flipImageVert = flipImageVert;
        this.fillPolygon = fillPolygon;
        this.worldProj = worldProj;

        scaleX = (double) imageWidth / envelope.getWidth();
        scaleY = (double) imageHeight / envelope.getHeight();

        Broadcast<Envelope> env = sparkContext.broadcast(envelope);

        return draw(rdd, env, outputPath, outputType, bgImagePath, pointSize);
    }

    public boolean visualize(JavaSparkContext sparkContext, JavaRDD<Tile<Double>> rdd, int imageWidth, int imageHeight, Envelope envelope, String outputPath) {
        this.imageWidth = imageWidth;
        this.imageHeight = imageHeight;

        Broadcast<Envelope> env = sparkContext.broadcast(envelope);

        this.flipImageVert = false;

        this.scaleX = this.imageWidth / envelope.getWidth();
        this.scaleY = this.imageHeight / envelope.getHeight();

        return drawRaster(rdd, env, outputPath);
    }

    public boolean visualizeInt(JavaSparkContext sparkContext, JavaRDD<Tile<Integer>> rdd, int imageWidth, int imageHeight, Envelope envelope, String outputPath) {
        this.imageWidth = imageWidth;
        this.imageHeight = imageHeight;

        Broadcast<Envelope> env = sparkContext.broadcast(envelope);

        this.flipImageVert = false;

        this.scaleX = this.imageWidth / envelope.getWidth();
        this.scaleY = this.imageHeight / envelope.getHeight();

        return drawRasterInt(rdd, env, outputPath);
    }

    private <G extends STObject,T> boolean draw(JavaRDD<Tuple2<G, T>> rdd, Broadcast<Envelope> env, String outputPath, String outputType, String bgImagePath, int pointSize) {
        BufferedImage finalImage = rdd.mapPartitions(iter -> {
            BufferedImage imagePartition = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics2D imageGraphic = imagePartition.createGraphics();
            imageGraphic.setColor(new Color(pixelColor));
            Envelope envelope = env.value();

            Iterable<Tuple2<G,T>> iterable = () -> iter;

            StreamSupport.stream(iterable.spliterator(),false) // make iterator a stream (in Java iterator does not support map function
                .forEach(tuple -> { // draw every tuple
                    try {
                        drawSTObject(tuple._1, imageGraphic, envelope, pointSize);
                    } catch(Exception ignored) { System.err.println("could not println SO: "+ignored.getMessage()); } // in case of an error just ignore
                });
            return Collections.singletonList(new ImageSerializableWrapper(imagePartition)).iterator();

        })
        .reduce((v1,v2) -> { // merge the RDD of images into a single image
            BufferedImage combinedImage = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics graphics = combinedImage.getGraphics();
            graphics.drawImage(v1.image, 0, 0, null);
            graphics.drawImage(v2.image, 0, 0, null);
            return new ImageSerializableWrapper(combinedImage);
        })
        .getImage(); // return that image

        if(bgImagePath != null) { // add background image if specified
            BufferedImage bg = null;
            try {
                bg = ImageIO.read(new File(bgImagePath));

                BufferedImage combinedImage = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
                Graphics graphics = combinedImage.getGraphics();
                graphics.drawImage(bg, 0, 0, null);
                graphics.drawImage(finalImage, 0, 0, null);
                finalImage = combinedImage;
            } catch (IOException e) {}
        }

        return saveImageAsLocalFile(finalImage, outputPath, outputType);
    }

    private boolean drawRaster(JavaRDD<Tile<Double>> rdd, Broadcast<Envelope> envelope, String outputPath) {

        BufferedImage theImage = rdd.mapPartitions( iter -> {
            BufferedImage img = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g = img.createGraphics();
            Iterable<Tile<Double>> iterable = () -> iter;

            StreamSupport.stream(iterable.spliterator(), false)
                .forEach(tile -> {

                    for (int x = 0; x < tile.width(); x++) {
                        for (int y = 0; y < tile.height(); y++) {

                            double v = tile.valueArray(x,y); //.data[x * tile.width + y]
                            if (v == 99999.0)
                                g.setColor(Color.black);
                            else {
                                int idx = (int) (v + 25.0) * 255 / 70;
                                g.setColor(colorMap[idx]);
                            }

                            g.drawRect(x, y, 1, 1);
                        }
                    }

                });

            return Collections.singletonList(new ImageSerializableWrapper(img)).iterator();
        }).reduce((img1, img2) -> {
            BufferedImage combinedImage = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics graphics = combinedImage.getGraphics();
            graphics.drawImage(img1.image, 0, 0, null);
            graphics.drawImage(img2.image, 0, 0, null);
            return new ImageSerializableWrapper(combinedImage);
        })
        .image;

        return saveImageAsLocalFile(theImage, outputPath, "png");
    }

    private boolean drawRasterInt(JavaRDD<Tile<Integer>> rdd, Broadcast<Envelope> envelope, String outputPath) {

        BufferedImage theImage = rdd.mapPartitions( iter -> {
            BufferedImage img = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics2D g = img.createGraphics();
            Iterable<Tile<Integer>> iterable = () -> iter;

            StreamSupport.stream(iterable.spliterator(), false)
                    .forEach(tile -> {

                        for (int x = 0; x < tile.width(); x++) {
                            for (int y = 0; y < tile.height(); y++) {

                                int v = tile.valueArray(x,y); //.data[x * tile.width + y]
                                g.setColor(new Color(v));
//                                if (v == 99999.0)
//                                    g.setColor(Color.black);
//                                else {
//                                    int idx = (int) (v + 25.0) * 255 / 70;
//                                    g.setColor(colorMap[idx]);
//                                }

                                g.drawRect(x, y, 1, 1);
                            }
                        }

                    });

            return Collections.singletonList(new ImageSerializableWrapper(img)).iterator();
        }).reduce((img1, img2) -> {
            BufferedImage combinedImage = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
            Graphics graphics = combinedImage.getGraphics();
            graphics.drawImage(img1.image, 0, 0, null);
            graphics.drawImage(img2.image, 0, 0, null);
            return new ImageSerializableWrapper(combinedImage);
        })
                .image;

        return saveImageAsLocalFile(theImage, outputPath, "png");
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

    private void drawSTObjectPolygon(Polygon geo, Envelope env, Graphics2D g) {
        Coordinate[] coordinates = geo.getCoordinates();
        java.awt.Polygon poly = new java.awt.Polygon();
        for(Coordinate c : coordinates) {
            Tuple2<Integer, Integer> p = null;
            if(worldProj) p = getMercatorProjection(c, env);
            else p = getImageCoordinates(c, env);
            if(p != null) poly.addPoint(p._1, p._2);
        }

        if(fillPolygon)	g.fillPolygon(poly);
        else g.drawPolygon(poly);
    }

    private void drawSTObject(STObject obj, Graphics2D g, Envelope env, int pointSize) throws Exception {
        Geometry geo = obj.getGeo();

        if(geo instanceof Point) {
            Tuple2<Integer, Integer> p = null;
            if(worldProj) p = getMercatorProjection(geo.getCoordinate(), env);
            else p = getImageCoordinates(geo.getCoordinate(), env);
            if(p != null) drawPoint(g, p, pointSize);
        } else if(geo instanceof Polygon) {
            drawSTObjectPolygon((Polygon)geo, env, g);
        } else if(geo instanceof MultiPolygon) {
            MultiPolygon mp = (MultiPolygon)geo;
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                drawSTObjectPolygon((Polygon)mp.getGeometryN(i),env,g);
            }
        } else {
            throw new Exception("Unsupported spatial object type. Only supports: Point, Polygon!");
        }
    }

    private void drawPoint(Graphics2D g, Tuple2<Integer, Integer> p, int pointSize) {
        g.fillRect(p._1, p._2, pointSize, pointSize);
    }

    private Tuple2<Integer, Integer> getMercatorProjection(Coordinate p, Envelope envelope) {
        double lat = p.y;
        double lng = p.x;

        //values for mercator map
        double mapLatBottom = -82.05;
        double mapLngRight = 180;
        double mapLngLeft = -180.85;

        double mapLatBottomRad = mapLatBottom * Math.PI / 180;
        double latitudeRad = lat * Math.PI / 180;
        double mapLngDelta = (mapLngRight - mapLngLeft);

        double worldMapWidth = ((imageWidth / mapLngDelta) * 360) / (2 * Math.PI);
        double mapOffsetY = (worldMapWidth / 2 * Math.log((1 + Math.sin(mapLatBottomRad)) / (1 - Math.sin(mapLatBottomRad))));

        double x = (lng - mapLngLeft) * (imageWidth / mapLngDelta);
        double y = imageHeight - ((worldMapWidth / 2 * Math.log((1 + Math.sin(latitudeRad)) / (1 - Math.sin(latitudeRad)))) - mapOffsetY);

        return new Tuple2<Integer, Integer>((int) x, (int) y);
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

        return new Tuple2<>(resultX, resultY);
    }

    class ImageSerializableWrapper implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        transient BufferedImage image;

        ImageSerializableWrapper(BufferedImage image) {
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

        BufferedImage getImage() {
            return this.image;
        }
    }

    private  Color[] colorMap = {
        new Color(196, 247, 245),
        new Color(184, 242, 243),
        new Color(170, 236, 241),
        new Color(154, 230, 238),
        new Color(139, 223, 235),
        new Color(121, 216, 231),
        new Color(103, 209, 229),
        new Color(85, 200, 226),
        new Color(67, 192, 222),
        new Color(50, 185, 219),
        new Color(34, 177, 216),
        new Color(20, 170, 213),
        new Color(10, 164, 212),
        new Color(2, 159, 210),
        new Color(2, 156, 210),
        new Color(2, 153, 210),
        new Color(2, 151, 210),
        new Color(2, 149, 210),
        new Color(2, 147, 210),
        new Color(2, 144, 210),
        new Color(2, 142, 210),
        new Color(2, 140, 210),
        new Color(2, 138, 210),
        new Color(2, 137, 210),
        new Color(2, 134, 210),
        new Color(2, 132, 210),
        new Color(2, 131, 210),
        new Color(2, 129, 210),
        new Color(2, 128, 210),
        new Color(2, 125, 210),
        new Color(2, 123, 210),
        new Color(2, 122, 210),
        new Color(2, 120, 210),
        new Color(2, 118, 210),
        new Color(2, 116, 210),
        new Color(2, 114, 210),
        new Color(2, 113, 210),
        new Color(2, 111, 210),
        new Color(2, 108, 210),
        new Color(2, 106, 210),
        new Color(2, 104, 209),
        new Color(2, 102, 208),
        new Color(2, 101, 208),
        new Color(2, 99, 207),
        new Color(3, 97, 205),
        new Color(2, 95, 204),
        new Color(2, 93, 202),
        new Color(2, 92, 201),
        new Color(2, 89, 200),
        new Color(2, 87, 197),
        new Color(2, 85, 196),
        new Color(2, 83, 194),
        new Color(2, 81, 192),
        new Color(2, 80, 190),
        new Color(2, 77, 189),
        new Color(2, 76, 187),
        new Color(2, 73, 185),
        new Color(2, 72, 183),
        new Color(2, 69, 181),
        new Color(2, 67, 179),
        new Color(2, 65, 178),
        new Color(2, 63, 175),
        new Color(2, 61, 174),
        new Color(2, 59, 172),
        new Color(2, 57, 169),
        new Color(2, 55, 168),
        new Color(2, 53, 167),
        new Color(3, 52, 165),
        new Color(4, 50, 163),
        new Color(6, 49, 161),
        new Color(6, 46, 160),
        new Color(8, 45, 159),
        new Color(10, 44, 158),
        new Color(12, 41, 156),
        new Color(14, 40, 155),
        new Color(16, 39, 154),
        new Color(18, 38, 153),
        new Color(21, 36, 153),
        new Color(23, 35, 152),
        new Color(26, 34, 151),
        new Color(29, 33, 152),
        new Color(32, 32, 151),
        new Color(35, 31, 151),
        new Color(39, 29, 151),
        new Color(43, 29, 151),
        new Color(47, 28, 151),
        new Color(50, 27, 151),
        new Color(54, 25, 151),
        new Color(59, 25, 152),
        new Color(62, 24, 152),
        new Color(67, 22, 152),
        new Color(71, 22, 152),
        new Color(75, 21, 152),
        new Color(80, 20, 153),
        new Color(84, 20, 153),
        new Color(89, 19, 154),
        new Color(94, 18, 153),
        new Color(98, 17, 154),
        new Color(102, 16, 154),
        new Color(107, 16, 154),
        new Color(111, 15, 154),
        new Color(115, 14, 154),
        new Color(120, 14, 154),
        new Color(124, 13, 154),
        new Color(129, 12, 154),
        new Color(133, 11, 153),
        new Color(137, 11, 153),
        new Color(141, 9, 152),
        new Color(145, 10, 152),
        new Color(148, 9, 151),
        new Color(152, 8, 151),
        new Color(156, 8, 149),
        new Color(159, 7, 149),
        new Color(162, 7, 147),
        new Color(166, 6, 145),
        new Color(168, 6, 144),
        new Color(171, 5, 142),
        new Color(173, 4, 140),
        new Color(176, 4, 139),
        new Color(177, 3, 137),
        new Color(179, 3, 134),
        new Color(182, 2, 131),
        new Color(183, 1, 129),
        new Color(185, 1, 127),
        new Color(187, 0, 123),
        new Color(189, 0, 121),
        new Color(190, 0, 117),
        new Color(191, 0, 114),
        new Color(193, 0, 111),
        new Color(195, 0, 107),
        new Color(196, 0, 104),
        new Color(197, 0, 100),
        new Color(199, 0, 97),
        new Color(200, 0, 94),
        new Color(201, 0, 89),
        new Color(202, 0, 86),
        new Color(203, 0, 82),
        new Color(205, 0, 78),
        new Color(205, 0, 74),
        new Color(206, 0, 70),
        new Color(208, 0, 66),
        new Color(209, 0, 63),
        new Color(210, 0, 59),
        new Color(211, 0, 55),
        new Color(211, 0, 52),
        new Color(212, 0, 48),
        new Color(213, 0, 44),
        new Color(214, 0, 41),
        new Color(215, 0, 38),
        new Color(216, 0, 34),
        new Color(217, 0, 31),
        new Color(217, 0, 28),
        new Color(218, 0, 24),
        new Color(219, 0, 21),
        new Color(220, 0, 18),
        new Color(220, 0, 16),
        new Color(221, 0, 12),
        new Color(222, 0, 10),
        new Color(223, 0, 8),
        new Color(223, 0, 6),
        new Color(224, 0, 4),
        new Color(225, 0, 2),
        new Color(226, 0, 0),
        new Color(226, 1, 0),
        new Color(227, 3, 0),
        new Color(228, 5, 0),
        new Color(228, 6, 0),
        new Color(228, 8, 0),
        new Color(229, 11, 0),
        new Color(230, 13, 0),
        new Color(230, 15, 0),
        new Color(231, 17, 0),
        new Color(231, 20, 0),
        new Color(231, 22, 0),
        new Color(232, 25, 0),
        new Color(233, 28, 0),
        new Color(232, 31, 0),
        new Color(233, 33, 0),
        new Color(233, 37, 0),
        new Color(234, 40, 0),
        new Color(235, 43, 0),
        new Color(234, 46, 0),
        new Color(235, 49, 0),
        new Color(235, 52, 0),
        new Color(235, 55, 0),
        new Color(236, 59, 0),
        new Color(237, 62, 0),
        new Color(237, 66, 0),
        new Color(237, 70, 0),
        new Color(238, 73, 0),
        new Color(238, 76, 0),
        new Color(238, 80, 0),
        new Color(239, 84, 0),
        new Color(239, 88, 0),
        new Color(239, 91, 0),
        new Color(240, 95, 0),
        new Color(239, 99, 0),
        new Color(240, 102, 0),
        new Color(240, 105, 0),
        new Color(240, 110, 0),
        new Color(241, 114, 0),
        new Color(241, 117, 0),
        new Color(241, 121, 0),
        new Color(242, 125, 0),
        new Color(242, 129, 0),
        new Color(242, 132, 0),
        new Color(243, 136, 0),
        new Color(242, 139, 0),
        new Color(243, 142, 0),
        new Color(243, 146, 0),
        new Color(243, 150, 0),
        new Color(244, 153, 0),
        new Color(244, 157, 0),
        new Color(243, 160, 0),
        new Color(244, 164, 0),
        new Color(244, 167, 0),
        new Color(244, 170, 0),
        new Color(245, 173, 0),
        new Color(245, 176, 0),
        new Color(246, 180, 0),
        new Color(245, 183, 0),
        new Color(245, 185, 0),
        new Color(246, 189, 0),
        new Color(246, 191, 0),
        new Color(246, 193, 0),
        new Color(247, 196, 0),
        new Color(247, 199, 0),
        new Color(247, 202, 0),
        new Color(247, 204, 0),
        new Color(247, 206, 0),
        new Color(247, 209, 0),
        new Color(247, 211, 0),
        new Color(247, 212, 0),
        new Color(248, 214, 0),
        new Color(247, 216, 0),
        new Color(248, 220, 2),
        new Color(249, 224, 5),
        new Color(249, 227, 7),
        new Color(249, 230, 11),
        new Color(250, 230, 15),
        new Color(250, 230, 19),
        new Color(250, 230, 24),
        new Color(250, 230, 29),
        new Color(250, 230, 33),
        new Color(250, 230, 38),
        new Color(250, 230, 44),
        new Color(250, 230, 48),
        new Color(250, 230, 53),
        new Color(250, 230, 59),
        new Color(250, 230, 64),
        new Color(250, 230, 68),
        new Color(250, 230, 73),
        new Color(250, 230, 77),
        new Color(250, 230, 82),
        new Color(250, 230, 86),
        new Color(0, 0, 0)
    };
}
package dbis.stark.raster

case class Bucket[U](val values : Int, val lowerBucketBound : U, val upperBucketBound : U) {}
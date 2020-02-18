package dbis.stark.raster

case class Bucket[U](values : Int, lowerBucketBound : U, upperBucketBound : U)
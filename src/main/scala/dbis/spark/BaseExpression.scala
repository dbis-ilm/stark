package dbis.spark

trait BaseExpression[T <: BaseExpression[T]] extends Serializable {
  def isValidAt(t: T): Boolean
  
  def intersects(t: T): Boolean
  
  def contains(t: T): Boolean
  
  def containedBy(t: T): Boolean
}
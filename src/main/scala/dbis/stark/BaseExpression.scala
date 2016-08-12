package dbis.stark

trait BaseExpression[T <: BaseExpression[T]] extends Serializable {
  def intersects(t: T): Boolean
  
  def contains(t: T): Boolean
  
  def containedBy(t: T): Boolean
}
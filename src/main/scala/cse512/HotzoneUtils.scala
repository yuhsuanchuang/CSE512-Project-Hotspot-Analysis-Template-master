package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // CHANGE PART
    val rectangle = queryRectangle.split(",").map(_.toDouble)
    val point = pointString.split(",").map(_.toDouble)
    val rect_min_x = math.min(rectangle(0),rectangle(2))
    val rect_max_x = math.max(rectangle(0),rectangle(2))
    val rect_min_y = math.min(rectangle(1),rectangle(3))
    val rect_max_y = math.max(rectangle(1),rectangle(3))
    // check whether the point is in the rectangle, if yes, return true; otherwise, return false.
    if(rect_min_x <= point(0) && rect_max_x >= point(0) && rect_min_y <= point(1) && rect_max_y >= point(1)) {
      return true
    }
    else {
      return false
    }
  }
}

package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

  // check whether two points are neighbor
  def is_neighbor(i: Int, j: Int): Boolean =
  {
    if(math.abs(i-j) <= 1)
      return true

    return false
  }

  // find the number of neighbor
  def number_of_neighbor(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int =
  {
    var min_max = 0

    if(x == minX || x == maxX)
      min_max += 1
    if(y == minY || y == maxY)
      min_max += 1
    if(z == minZ || z == maxZ)
      min_max += 1

    min_max match
    {
      case 0 => return 27
      case 1 => return 18
      case 2 => return 12
      case 3 => return 8
    }

    return 0
  }

  // calculate z score
  def calculate_Z_score(X_bar: Double, S: Double, num_of_neighbor: Int, sum_neighbor_point_count: Int, numCells: Int): Double =
  {
    val numerator = sum_neighbor_point_count.toDouble - X_bar*num_of_neighbor
    val denominator_num = (numCells.toDouble * num_of_neighbor.toDouble) - math.pow(num_of_neighbor.toDouble,2)
    val denominator = S * math.sqrt(denominator_num / (numCells.toDouble-1.0))

    val z_score = numerator/denominator
    return z_score
  }
}

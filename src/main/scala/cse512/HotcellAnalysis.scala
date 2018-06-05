
package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    //  pickupInfo.show()
    pickupInfo.createOrReplaceTempView("data")

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART


    //////////find the points in the space-time cube
    val in_cube_data = spark.sql("select x, y, z from data " +
      "where x>="+minX+" and x<="+ maxX+" and y>="+minY+" and y<="+ maxY+" and z>="+minZ+" and z<="+ maxZ);
    in_cube_data.createOrReplaceTempView("in_cube")
    printf("in_cube_data\n")

    /////////group and count the data
    val cell_data = spark.sql("select x, y, z, count(*) as point_count from in_cube group by z, y, x").persist();
    cell_data.createOrReplaceTempView("cellpoint_count")
    //  cell_data.show()
    printf("cell_data\n")

    ///////////find the sum of Xij and sum of square Xij
    val count_sum = spark.sql("select sum(point_count), sum(power(point_count,2)) from cellpoint_count").persist()
    val sum_X = count_sum.first().getLong(0);
    val sum_X_square = count_sum.first().getDouble(1);
    val X_bar = sum_X.toDouble/numCells.toDouble;
    val S = math.sqrt((sum_X_square.toDouble/numCells.toDouble)-math.pow(X_bar,2));
    printf("S!!\n")

    ///////////cross join the cell data
    spark.udf.register("is_neighbor",(i: Int, j:Int)=>((HotcellUtils.is_neighbor(i, j))))
    var cross_join = spark.sql("select i.x as i_x, i.y as i_y, i.z as i_z," +
      "j.x as j_x, j.y as j_y, j.z as j_z, " +
      "j.point_count as j_point_count " +
      "from cellpoint_count as i cross join cellpoint_count as j " +
      "where ( is_neighbor(i.x, j.x) and is_neighbor(i.y, j.y) and is_neighbor(i.z, j.z) )")
    cross_join.createOrReplaceTempView("cross_join_neighbor")
    printf("cross_join\n")

    ///////////group the cross join output and find the number of neighbors
    spark.udf.register("number_of_neighbor",(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int)=>((HotcellUtils.number_of_neighbor(x, y, z, minX, maxX, minY, maxY, minZ, maxZ))))
    var count_neighbor = spark.sql("select i_x as x, i_y as y, i_z as z, "+
      "sum(j_point_count) as sum_neighbor_point_count, " +
      "number_of_neighbor(i_x, i_y, i_z, "+minX+","+maxX+","+minY+","+maxY+","+minZ+","+maxZ+") as num_of_neighbor " +
      "from cross_join_neighbor group by i_x, i_y, i_z").persist()
    count_neighbor.createOrReplaceTempView("neighbor_count")
    printf("count_neighbor\n")

    /////////////////////calculate z score for each cell and sort it desc
    spark.udf.register("calculate_Z_score",(X_bar: Double, S: Double, num_of_neighbor: Int, sum_neighbor_point_count: Int, numCells: Int)=>((
      HotcellUtils.calculate_Z_score(X_bar, S, num_of_neighbor, sum_neighbor_point_count, numCells)
      )))
    val result = spark.sql("select x, y, z " +
      "from neighbor_count order by " +
      "calculate_Z_score("+X_bar+","+S+", num_of_neighbor, sum_neighbor_point_count,"+numCells+") desc");
    result.createOrReplaceTempView("Z_score")
    printf("result\n")

    return result

  }
}

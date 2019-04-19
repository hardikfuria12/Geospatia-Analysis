package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection._
//import sqlContext.implicits._



object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    pickupInfo.createOrReplaceTempView("pickupInfo")
    val Rectangle = "%d,%d,%d,%d".format(minX.toInt, maxX.toInt, minY.toInt, maxY.toInt)

    spark.udf.register("ST_Contains", (x: Int, y: Int, minX: Int, maxX: Int, minY: Int, maxY: Int) =>
      HotcellUtils.ST_Contains(x, y, minX, maxX, minY, maxY))

    pickupInfo = spark.sql("select x,y,z from pickupInfo where ST_contains(x,y, " + Rectangle + ")")
    pickupInfo.createOrReplaceTempView("pickupInfo")
    pickupInfo = spark.sql("select x,y,z,count(*) as count from pickupInfo GROUP BY x,y,z")


    val all_list = pickupInfo.collect()

    var map: Map[String, Long] = Map()

    var samexyz: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()

    var lX = scala.collection.mutable.Set[Int]()
    var lY = scala.collection.mutable.Set[Int]()
    var lZ = scala.collection.mutable.Set[Int]()

    var csquare = 0l
    var ccell = 0l

    for (row <- all_list) {
      var x = row.getInt(0)
      var y = row.getInt(1)
      var z = row.getInt(2)

      var ccount = row.getLong(3)
      var dataKey = "%d%d%d".format(x, y, z)
      map += (dataKey -> ccount)
      csquare += ccount * ccount
      ccell += ccount

      var minKey = "%d%d%d".format(x - minX.toInt, y - minY.toInt, z - minZ)
      samexyz.update(minKey, dataKey)
      lX += x
      lY += y
      lZ += z

    }
    val average = ccell.toDouble / numCells.toDouble
    val val_S = Math.sqrt(csquare.toDouble / numCells.toDouble - average * average)

    var mx = minX.toInt
    var my = minY.toInt
    var mz = minZ.toInt


    var fMap: Map[(Int, Int, Int), Double] = Map()

    for (i <- lX; j <- lY; k <- lZ) {
      val (wijxj, wij) = getW(samexyz, map, i - mx, j - my, k - mz)
      val num = wijxj - (wij * average)
      val den = val_S * math.sqrt((numCells * wij - (wij * wij)) / (numCells - 1))
      fMap += (i, j, k) -> num / den
    }

    //val res = fMap.toSeq//.sortBy(-_._2)
    import spark.implicits._

    val res = fMap.toSeq.toDF("XYZ","getis_score")//.sort($"getis_score".desc)//.sortBy(-_._2)
    //res.show(50)
    //val r = res.map { case (k, v) => k ,}
    /*
    val newDf = res.withColumn("_tmp", split($"XYZ", "\\,")).select(
      $"_tmp".getItem(0).as("X"),
      $"_tmp".getItem(1).as("Y"),
      $"_tmp".getItem(2).as("Z")
    ).drop("_tmp")
    */
    //val newDf = res.select($"XYZ"(0) as "X", $"XYZ"(1) as "Y", $"XYZ"(2) as "Z", $"getis_score")
    val newDf = res.withColumn("X", $"XYZ._1")
      .withColumn("Y", $"XYZ._2")
      .withColumn("Z", $"XYZ._3").drop("XYZ")
    newDf.createOrReplaceTempView("newDf")

    val outDf=spark.sql("select X,Y,Z from (select * from newDf ORDER BY getis_score DESC) ")
    //newDf.show(20)
    //val outputDf = newDf.sort($"getis_score".desc)
    //val outDf = outputDf.drop("getis_score")
    //outDf.show(50)

    //val finalResult = res//.take(50)
    //val r = finalResult.map { case (k, v) => k }

    //val rdf = r.toDF()
    //rdf.show()
    return outDf.limit(50)



    //return pickupInfo // YOU NEED TO CHANGE THIS PART


  }
  def getW(sameMap:scala.collection.mutable.Map[String,String],map:Map[String,Long],x:Int,y:Int,z:Int):(Long,Int)=
  {
    var sum=0.toLong
    var ncount=0
    for(i<- -1 to 1;j<- -1 to 1;k<- -1 to 1){
      var key ="%d%d%d".format(x+k,y+j,z+i)
      sum+=getValue(sameMap,map,key)
      ncount+=1


    }
    (sum,ncount)
  }
  def getValue(sameMap:scala.collection.mutable.Map[String,String],map:Map[(String),Long],key:String):Long=
  {
    var c=0l
    if(sameMap.contains(key))
    {
      var cell=sameMap(key)
      c=map(cell)
    }
    c
  }

}

package cse512

import org.apache.commons.io.filefilter.FalseFileFilter

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);

    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(({

      val point = pointString.split(",")


      val pointX = point(0).toDouble

      val pointY = point(1).toDouble
      val c=Array(pointX,pointY)
      val Rect =  queryRectangle.split(",")

      val RectX1 = Rect(0).toDouble


      val RectY1 = Rect(1).toDouble
      val low=Array(RectX1,RectY1)


      val RectX2 = Rect(2).toDouble

      val RectY2 = Rect(3).toDouble

      val high=Array(RectX2,RectY2)
      var dist=0.0

      for ( i <- 0 to (low.length - 1)) {
        if (c(i)< low(i))
          dist += math.pow((low(i)-c(i)),2)
        else if (c(i)>high(i))
          dist+=math.pow((c(i)-high(i)),2)


      }
      if(dist==0)
        true
      else
        false

    })))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")

    resultDf.show()

    return resultDf.count()

  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);

    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);

    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(({

      val point = pointString.split(",")
      val pointX = point(0).toDouble

      val pointY = point(1).toDouble
      val c=Array(pointX,pointY)
      val Rect =  queryRectangle.split(",")

      val RectX1 = Rect(0).toDouble
      val RectY1 = Rect(1).toDouble
      val low=Array(RectX1,RectY1)
      val RectX2 = Rect(2).toDouble
      val RectY2 = Rect(3).toDouble

      val high=Array(RectX2,RectY2)
      var dist=0.0

      for ( i <- 0 to (low.length - 1)) {
        if (c(i)< low(i))
          dist += math.pow((low(i)-c(i)),2)
        else if (c(i)>high(i))
          dist+=math.pow((c(i)-high(i)),2)
      }
      if(dist==0)
        true
      else
        false


    })))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")

    resultDf.show()

    return resultDf.count()

  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);

    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(({
      val p1 = pointString1.split(",")
      val p2 = pointString2.split(",")
      val d = distance
      val x1 = p1(0).toDouble
      val x2 = p2(0).toDouble
      val y1 = p1(1).toDouble
      val y2 = p2(1).toDouble
      var r = 0.0;
      val r1=(x1-x2)*(x1-x2)+(y1-y2)*(y1-y2)-d*d
      if(r1<=0)
        true
      else
        false
    })))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")

    resultDf.show()

    return resultDf.count()

  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);

    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);

    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(({
      val p1 = pointString1.split(",")
      val p2 = pointString2.split(",")
      val d = distance
      val x1 = p1(0).toDouble
      val x2 = p2(0).toDouble
      val y1 = p1(1).toDouble
      val y2 = p2(1).toDouble
      var r = 0.0;
      val r1=(x1-x2)*(x1-x2)+(y1-y2)*(y1-y2)-d*d
      if(r1<=0)
        true
      else
        false
    })))

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")

    resultDf.show()

    return resultDf.count()

  }

}



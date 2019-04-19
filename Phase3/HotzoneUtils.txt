package cse512

object HotzoneUtils {


  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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

  }


}

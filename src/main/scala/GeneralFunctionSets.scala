import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
//import scala.actors.migration.ActWithStash.continue
import scala.collection.mutable.ArrayBuffer


object GeneralFunctionSets {
    // 输入一个字符串的时间,计算时间戳(单位S)
    // 输入如 2019-06-15 21:12:46
    // 返回一个时间戳,如1615546989
    def transTimeToTimestamp(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }

    // 输入一个字符串的时间,计算时间戳(单位S)
    // 输入如 2016-06-02T01:19:57.000Z
    // 返回一个时间戳,如1615546989
    def transTimeV2ToTimestamp(timeString: String): Long = {
        val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }

    // 输入一个字符串的时间,计算时间戳(单位S)
    // 输入如 2016-06-02T01:19:57.000Z
    // 返回一个minutesOfDay,如79
    def minutesOfDayV2(timeString: String): Int = {
        val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time: Date = dateFormat.parse(timeString)
        val calendar: Calendar = Calendar.getInstance()
        calendar.setTime(time)
        val H: Int = calendar.get(Calendar.HOUR_OF_DAY)
        val M: Int = calendar.get(Calendar.MINUTE)
        H * 60 + M
    }
    // 输入一个long类型的时间,计算时间戳(单位毫秒)
    // 输入如 1464801597000
    // 返回一个minutesOfDay,如79
    def minutesOfDayV2(t: Long): Int = {
        val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t)
        val time: Date = dateFormat.parse(timeString)
        val calendar: Calendar = Calendar.getInstance()
        calendar.setTime(time)
        val H: Int = calendar.get(Calendar.HOUR_OF_DAY)
        val M: Int = calendar.get(Calendar.MINUTE)
        H * 60 + M
    }

    // 输入一个时间戳,得对应的字符串
    // 输入如:1615546989(单位MS)
    // 返回一个字符串,如2019-06-15 21:12:46
    def transTimeToString(timeStamp: Long): String = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.format(timeStamp * 1000)
        time
    }
    // 输入一个字符串的时间,是一个月里面的哪一天
    // 输入如 2019-06-15 21:12:46
    // 返回一个日期的日,如 15
    def dayOfMonth_string(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_MONTH)
    }

    // 输入一个时间戳,返回是一个月里面的哪一天
    // 输入如1615546989
    // 返回一个日期的日,如 15
    def dayOfMonth_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.DAY_OF_MONTH)
    }

    def hourOfDay(timeString: String): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    def hourOfDay_long(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.HOUR_OF_DAY)
    }

    // 获取时间戳对应当天的秒数
    def secondsOfDay(t: Long): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        val H = calendar.get(Calendar.HOUR_OF_DAY)
        val M = calendar.get(Calendar.MINUTE)
        val S = calendar.get(Calendar.SECOND)
        (H * 60 + M) * 60 + S
    }

    // 获取以半小时为单位的时间段序号当月的第几个半小时，0为起点
    def halfHourOfDay(t: Long): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        (calendar.get(Calendar.DAY_OF_MONTH) - 1) * 48 + calendar.get(Calendar.HOUR_OF_DAY) * 2 + calendar.get(Calendar.MINUTE) / 30
    }

    def deleteFilePath(dirFile:File):Boolean = {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) return false

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (file <- dirFile.listFiles()) {
                deleteFilePath(file);
            }
        }
        dirFile.delete();
    }
    def deletePath(dirFile:String):Boolean = {
        val file1 = new File(dirFile)
        deleteFilePath(file1)
    }

    //根据经纬度计算两点距离
    def getDistatce(longitude1: Double, latitude1: Double, longitude2: Double, latitude2: Double,precision:Int=2) = {

        val R = 6378.137;

        // 纬度
        val lat1: Double = Math.toRadians(latitude1)
//        println(lat1)
        val lat2: Double = Math.toRadians(latitude2)
        // 经度
        val lng1: Double = Math.toRadians(longitude1)
        val lng2: Double = Math.toRadians(longitude2)
        // 纬度之差
        val a: Double = lat1 - lat2
        // 经度之差
        val b: Double = lng1 - lng2
        // 计算两点距离的公式
        var s: Double = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
          Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(b / 2), 2)))
        // 弧长乘地球半径, 返回单位: 千米
        s = s * R
        s

    }
//def getDistatce(lon1: Double, lat1: Double, lat2: Double, lon2: Double,precision:Int=2) = {
//    if (lat1 != 0 && lon1 != 0 && lat2 != 0 && lon2 != 0) {
//        val R = 6378.137
//        val radLat1: Double = lat1 * Math.PI / 180
//        val radLat2: Double = lat2 * Math.PI / 180
//        val a: Double = radLat1 - radLat2
//        val b: Double = lon1 * Math.PI / 180 - lon2 * Math.PI / 180
//        val s: Double = 2 * Math.sin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
//        BigDecimal.decimal(s * R).setScale(precision, BigDecimal.RoundingMode.HALF_UP).doubleValue()
//    } else {
//        BigDecimal.decimal(0).setScale(precision, BigDecimal.RoundingMode.HALF_UP).doubleValue()
//    }
//}

    /**
     * 返回一个点是否在一个多边形区域内
     *
     * @param mPoints 多边形坐标点列表,要求这些点按照顺序能绘制成一个区域
     * @param point   待判断点
     * @return true 多边形包含这个点,false 多边形未包含这个点。
     */
    def isPolygonContainsPointArr(mPoints: ArrayBuffer[ArrayBuffer[Double]], point: ArrayBuffer[Double]): Boolean = {
        var nCross = 0
        val arrSize: Int = mPoints.length
        val yIndex = 0
        val xIndex = 1
        for (i <- 0 until arrSize) {
            val p1: ArrayBuffer[Double] = mPoints(i)
            val p2: ArrayBuffer[Double] = mPoints((i + 1) % arrSize)
            // 取多边形任意一个边,做点point的水平延长线,求解与当前边的交点个数
            // p1p2是水平线段,要么没有交点,要么有无限个交点
            if (p1(yIndex) != p2(yIndex)) {
                // point 在p1p2 底部 --> 无交点
                if (point(yIndex) >= Math.min(p1(yIndex), p2(yIndex))) {
                    // point 在p1p2 顶部 --> 无交点
                    if (point(yIndex) < Math.max(p1(yIndex), p2(yIndex))) {
                        // 求解 point点水平线与当前p1p2边的交点的 X 坐标 通过前面几个if条件筛选,这里的如果求出来有交点一定在p1p2连接线上,而不是延长线上.
                        val x: Double = (point(yIndex) - p1(yIndex)) * (p2(xIndex) - p1(xIndex)) / (p2(yIndex) - p1(yIndex)) + p1(xIndex)
                        if (x > point(xIndex)) { // 当x=point.x时,说明point在p1p2线段上
                            nCross += 1 // 只统计单边交点
                        }
                    }// else continue
                }// else continue
            } // else continue
        }
        // 单边交点为偶数，点在多边形之外 ---
        nCross % 2 == 1
    }

    /**
     * 返回一个点所在的区域编号,如果没找到区域，则返回距离这个点最近的一个区域的中心点所在的区域编号
     *
     * @param mPoints 多边形坐标点列表,要求这些点按照顺序能绘制成一个区域
     * @param point   待判断点
     * @return true 多边形包含这个点,false 多边形未包含这个点。
     *
     */
    def PolygonNum(mPoints: ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]],
                   minMaxRegion:Array[Array[Array[Double]]],
                   point: ArrayBuffer[Double],
                   Centers:Array[Array[Double]]): Int = {
        var Index: Int = -1

//        val inner = new Breaks
//        inner.breakable {
//            for (i <- minMaxRegion.indices) {
//                //            var InThisRegiion = false
//                if (minMaxRegion(i)(0)(0) <= point(0)
//                  && minMaxRegion(i)(1)(0) >= point(0)
//                  && minMaxRegion(i)(0)(1) <= point(1)
//                  && minMaxRegion(i)(1)(1) >= point(1)
//                ) {
//                    //                InThisRegiion = true
//                    val In: Boolean = isPolygonContainsPointArr(mPoints(i), point)
//                    if (In) {
//                        Index = i
//                        inner.break()
//                    }
//                }
//            }
//        }

        var find = false
        var i = 0
        while (i < minMaxRegion.length && !find) {
            //            var InThisRegiion = false
            if (minMaxRegion(i)(0)(0) <= point(0)
              && minMaxRegion(i)(1)(0) >= point(0)
              && minMaxRegion(i)(0)(1) <= point(1)
              && minMaxRegion(i)(1)(1) >= point(1)
            ) {
                //                InThisRegiion = true
                val In: Boolean = isPolygonContainsPointArr(mPoints(i), point)
//                val In: Boolean = false
                if (In) {
                    find = true
                    Index = i
                }
            }
            i += 1
        }
        // 都没有找到的情况下就找最近的一个区域
        if (Index == -1) {
            Index = 0
            var minDistance: Double = getDistatce(Centers(0)(0),Centers(0)(1),point(0),point(1))
            for (i <- 1 until Centers.length) {
                val distance: Double = getDistatce(Centers(i)(0), Centers(i)(1), point(0), point(1))
                if (distance < minDistance) {
                    minDistance = distance
                    Index = i
                }
            }
        }
        Index
    }

    def transformlat(lng: Double,lat:Double): Double = {
        val pi = 3.14159265358979324
        var ret: Double = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(math.abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
          math.sin(2.0 * lng * pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * pi) + 40.0 *
          math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
          math.sin(lat * pi / 30.0)) * 2.0 / 3.0
        ret
    }


    def transformlng(lng: Double,lat:Double): Double = {
        val pi = 3.14159265358979324
        var ret: Double = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(math.abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
          math.sin(2.0 * lng * pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * pi) + 40.0 *
          math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
          math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
        ret
    }
    /**
     * WGS84转GCJ02(火星坐标系)
     *
     * @param param lng:WGS84坐标系的经度
     * @param lat:WGS84坐标系的纬度
     * @return true 多边形包含这个点,false 多边形未包含这个点。
     *
     */
    def wgs84togcj02(lng: Double,lat:Double): (Double,Double) ={
        val pi = 3.14159265358979324
        val ee = 0.00669342162296594323  // 扁率
        val a = 6378245.0  // 长半轴
        var dlat: Double = transformlat(lng - 105.0, lat - 35.0)
        var dlng: Double = transformlng(lng - 105.0, lat - 35.0)
        val radlat: Double = lat / 180.0 * pi
        var magic: Double = math.sin(radlat)
        magic = 1 - ee * magic * magic
        val sqrtmagic: Double = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
        dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
        val mglat: Double = lat + dlat
        val mglng: Double = lng + dlng
        (lng * 2 - mglng, lat * 2 - mglat)
    }

}

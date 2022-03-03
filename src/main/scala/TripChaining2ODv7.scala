import GeneralFunctionSets.{deletePath, getDistatce, transTimeV2ToTimestamp}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
//import scala.tools.nsc.classpath.FileUtils.FileOps
import java.util.Date

/**
 * 功能:将出行链转换成OD, 如从(a,b) -> (b,c) -> (c,d) => (a,d)
 * 输入为每天的记录的文件夹
 * 方法:根据ID分组后,对起点时间进行排序,匹配i+1的起点时间跟i的终点时间之间间距是否小于20分钟
 * 如果是的情况下,则找下一条记录,否则则将i的终点作为OD的终点,i+1的起点作为新的OD起点
 */

object TripChaining2ODv7 {
  def containLoop(odBuf:ArrayBuffer[Array[String]]):Int = {
    val size: Int = odBuf.size
    var distances = new Array[Array[Double]](size) // 声明
    for(i <- 0 until distances.length){ // 初始化
      distances(i) = new Array[Double](size)
    }


    for (i <- 0 until size) {
      for (j <- i until size) {
        val distance: Double = getDistatce(odBuf(i)(3).toDouble, odBuf(i)(4).toDouble,
          odBuf(j)(13).toDouble, odBuf(j)(14).toDouble)
        distances(i)(j) = distance
        distances(j)(i) = distance
      }
    }
//    var preDistances = new Array[Double](size)
//    preDistances(0) = distances(0)(1)
//    for (i <- 1 until size-1) {
//      preDistances(i) = preDistances(i-1) + distances(i)(i+1)
//    }

    var result = -1
    val inner = new Breaks
    inner.breakable {
      for (i <- 0 until (size - 1)) {
        var maxDistance: Double = distances(i)(i)
        //      var maxDistanceIndex: Int = i
        for (j <- (i + 1) until size) {
//          if (distances(i)(j) < 2.0 // (a,b)(b,c), (a,b) > 2, (a,c) < 2 , + 公式
//            && (maxDistance > 2.0)
//            && (maxDistance > (2 * distances(i)(j) + 0.5))) {
          if (distances(i)(j) < 0.5 // (a,b)(b,c), (a,b) > 2, (a,c) < 2 , + 公式
            && (maxDistance > 0.7)
          ){
//            && (maxDistance > (2 * distances(i)(j) + 0.5))) {
            result = i
            inner.break
          }
          if (maxDistance < distances(i)(j)) {
            maxDistance = distances(i)(j)
          }
        }
//        // 假设出行链的第i和
//        if (10 * distances(0)(i+1) < preDistances(i)) {
//          result = i
//          inner.break
//        }
      }
    }
    result
  }
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TripChaining2ODv2")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    // 这里用2020年之后的数据测试会不会好一点? 不行,这里仍然存在的问题是ramdom文件夹仍然存在.
    val BusDataPath : String ="G:\\subwayData\\mutiOD\\BusD"
//    val BusDataPath = "D:\\subwayData\\mutiOD\\BusD_test"

    val SubDataPath = "D:\\subwayData\\mutiOD\\SubwayODOut"
//    val SubDataPath = "D:\\subwayData\\mutiOD\\SubwayODOutTest"

    val savePathPre : String ="D:\\subwayData\\mutiOD\\SubAndBusDataOut"
//    val savePathPre : String ="D:\\subwayData\\mutiOD\\testOut"


//    val HomeAndWorkPath : String ="G:\\subwayData\\mutiOD\\HomeAndWork\\Day"
    val HomeAndWorkPath : String ="G:\\subwayData\\mutiOD\\HomeAndWork\\Month"

// id,起点时间，起点站点，起点经度，起点纬度，终点时间，终点站点，终点经度，终点纬度，（换乘站点序列），换乘次数，家经度，家纬度，工作地经度，工作地纬度，（详细记录序列，每条记录的字段用逗号分隔，记录和记录间用分号分隔）
    val file = new File(BusDataPath)
    val iterator: Iterator[File] = file.listFiles().iterator


    while (iterator.hasNext) {


      var start_time =new Date().getTime

      val dataPath: String = iterator.next().toString
      val strings: Array[String] = dataPath.split('\\')
      val dataName: String = strings(strings.length - 1)
      // 仅仅计算2018年的数据
//      if (dataPath.contains("2018") && dataName.toInt == 20180615) {
//            if (dataPath.contains("2018") && dataName.toInt == 20181220) {
      if (dataPath.contains("2018") && dataName.toInt >= 20180915) {

        //      val strArray: Array[String] = Array("common", "last", "passengerflow", "random", "stationflow")
        val strArray: Array[String] = Array("common", "last", "passengerflow", "stationflow") // 去掉random
        var strArrayBuf = new ArrayBuffer[String]
        for (i <- 0 until strArray.length) {
          val path: String = dataPath + "/" + strArray(i)
          val file1 = new File(path)
          if (file1.isDirectory)
            strArrayBuf.append(path + "/withtime/part-r-00000")
          else {
            println(path + " not exist")
          }
        }




        //      字段,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
        //      bus,id,ot,oStation,oLat,oLon,起始车牌,线路,车牌,方向,班次ID,-,dt,dStation,dLat,dLon,终点车次,Bus,-
        //      sub,id,ot,oStation,oLat,oLon,入站闸机,-,-,-,-,进站状态,dt,dStation,dLat,dLon,出站闸机,Sub,出站状态


        val subFileName: String = SubDataPath + "//" + dataName + "//part-0*"
        val SubFile: RDD[String] = sc.textFile(subFileName)

        var path = HomeAndWorkPath + "//" + dataName + "//Home"
        val file2 = new File(path)

        // 空RDD
        var HomeRDD: RDD[String] = sc.makeRDD(Array("0,0,0,0,0,0"))

        // 路径存在则创建rdd
        if (file2.isDirectory) {
          HomeRDD = sc.textFile(path = path + "//part-r-00*")
        } else {
          println("Home data not exit")
        }

        val HomeRDData: RDD[(String, (Long, Long, Long, String))] = HomeRDD.map(line => {
          val datas: Array[String] = line.split(",")
          val id: String = datas(0)
          (id,(0L,0L,0L,line))
        })
//        val HomeMapData: Array[(String, (String, String))] = HomeRDD.map(x => {
//          val datas: Array[String] = x.split(",")
//          val id: String = datas(0)
//          val Lat: String = datas(7)
//          val Lon: String = datas(8)
//          (id, (Lat,Lon))
//        }).collect()
//        val HomeBroadcastValue: Broadcast[Array[(String, (String, String))]] = sc.broadcast(HomeMapData)
//

        // 空RDD
        var WorkRDD: RDD[String] = sc.makeRDD(Array("0,0,0,0,0,0"))

        path = HomeAndWorkPath + "//" + dataName + "//Work"
        val file3 = new File(path)
        // 路径存在则创建rdd
        if (file3.isDirectory) {
          WorkRDD = sc.textFile(path = path + "//part-r-00*")
        } else {
          println("Home data not exit")
        }

//        val WorkRDD: RDD[String] = sc.textFile(path = HomeAndWorkPath + "//" + dataName + "//Work//part-r-00*")

        val WorkRDDData: RDD[(String, (Long, Long, Long, String))] = WorkRDD.map(line => {
          val datas: Array[String] = line.split(",")
          val id: String = datas(0)
          (id,(1L,1L,1L,line))
        })
//        val WorkMapData: Array[(String, (String, String))] = WorkRDD.map(x => {
//          val datas: Array[String] = x.split(",")
//          val id: String = datas(0)
//          val Lat: String = datas(7)
//          val Lon: String = datas(8)
//          (id, (Lat,Lon))
//        }).collect()
//        val WorkBroadcastValue: Broadcast[Array[(String, (String, String))]] = sc.broadcast(WorkMapData)


        //      val idIndex:Int = 0
        val BusFile: RDD[String] = sc.textFile(strArrayBuf.mkString(",")).map(line => {
          var result = new ArrayBuffer[String] //排序后原始的od记录
          //为了与地铁数据进行关联,只提取需要的字段
          val str: String = line.replace("\t", "") //直接删除\t字段
          val datas: Array[String] = str.split(",")
          val id: String = datas(0)
          val ot: String = datas(5)
          val oPlate: String = datas(6)
          val oStation: String = datas(7)
          val oLat: String = datas(9)
          val oLon: String = datas(10)
          val dt: String = datas(11)
          val dPlate: String = datas(12)
          val dStation: String = datas(13)
          val dLat: String = datas(15)
          val dLon: String = datas(16)


          result.append(id) //0
          result.append(ot) //1
          result.append(oStation) //2
          result.append(oLat) //3
          result.append(oLon) //4
          result.append(oPlate) //5
          result.append(datas(1)) //6
          result.append(datas(2))
          result.append(datas(3))
          result.append(datas(4))
          result.append("-")
          result.append(dt)
          result.append(dStation)
          result.append(dLat)
          result.append(dLon)
          result.append(dPlate)
          result.append("Bus")
          result.append("-")

          result.mkString(",")
        })

        val idIndex = 0
        val otIndex = 1
        val dtIndex = 11
        val modelIndex = 16
        val trainNumIndex = 9 //车次

        val Rdd1 = SubFile.union(BusFile)
          .map(line => {

            val fields: Array[String] = line.split(',')
            val id: String = fields(idIndex)
            //        val state: String = fields(3)
            val ot: Long = transTimeV2ToTimestamp(fields(otIndex)) //(单位S)
            //        val oName: String = fields(7)
            //        val oLat: String = fields(9)
            //        val oLong: String = fields(10)

            val dt: Long = transTimeV2ToTimestamp(fields(dtIndex)) //(单位S)
            //        val dName: String = fields(13)
            //        val dLat: String = fields(15)
            //        val dLong: String = fields(16)
            val trvaltime: Long = dt - ot


            //        (id, (trvaltime,ot, dt))
            //(id, (trvaltime,ot, dt,(id,ot,oName,oLat,oLong)))
            (id, (trvaltime, ot, dt, line))
          })
          .filter(line => {
            val trvaltime: Long = line._2._1
            (
              (trvaltime > 60 * 2) //OD时间少于2分钟,应该没有上车下车时间少于2分钟的吧? 不好意思,真的有,手动狗头
                && (trvaltime < 60 * 200) //200分钟
              )
          }) // 去掉明显出问题的数据
          .cache()


//        val targetIds = Rdd1
//          .groupByKey()
//
//          .map(line => {
//            val id: String = line._1
//            val data: Array[(Long, Long, Long, String)] = line._2.toArray
//            val dataSortByOt: Array[(Long, Long, Long, String)] = data.sortBy(item => item._2) //原始数组不变,排序后多一个新的
//
//            //          val maxTripTime = 30;
//            var i = 0;
//            var BusTripChain = new ArrayBuffer[String]
//            var org = new ArrayBuffer[String] //排序后原始的od记录
//            var trip: Array[String] = dataSortByOt(0)._4.split(",")
//            var lastIndex = 0; // 用于计算换乘
//
//            var lastMode: String = trip(modelIndex)
//
//
//            var lastTrainNum: String = trip(trainNumIndex)
//
//            var odBuf = new ArrayBuffer[String]
//            //          odBuf.append(dataSortByOt(0)._4) //i从0开始就不增加这句话
//
//            while (i < dataSortByOt.length - 1) {
//              org.append(dataSortByOt(i)._4)
//              val origin: Array[String] = dataSortByOt(i + 1)._4.split(',')
//              var destination: Array[String] = dataSortByOt(i)._4.split(',') //得到终点的字串
//              val distanceKM: Double = getDistatce(origin(3).toDouble, origin(4).toDouble,
//                destination(13).toDouble, destination(14).toDouble)
//
//              val distanceKM2: Double = getDistatce(destination(3).toDouble, destination(4).toDouble,
//                origin(13).toDouble, origin(14).toDouble) //当前起点与下一次的终点距离
//
//              val distanceKM3: Double = getDistatce(destination(3).toDouble, destination(4).toDouble,
//                destination(13).toDouble, destination(14).toDouble) //本次的距离
//
//              val ot: Long = dataSortByOt(i + 1)._2
//              val dt: Long = dataSortByOt(i)._3
//
//              var distanceKM4: Double = 0;
//              if (odBuf.size > 2)
//                distanceKM4 = getDistatce(odBuf(0).split(",")(3).toDouble, odBuf(0).split(",")(4).toDouble,
//                  odBuf(odBuf.length - 1).split(",")(13).toDouble, odBuf(odBuf.length - 1).split(",")(14).toDouble)
//              var ruleFlag = false
//              //            目前的规则：
//              //            （1）两次乘车之间的时间间隔大于20分钟则切断
//              //            （2）两次乘车前一次D与下一次O的距离大于0.7km则切断
//              //            （3）不存在一次出行中包括两次地铁记录。遇到i,和i+1都是地铁则切断
//              //            （4）不存在一次出行中包括同一趟公交线路的数据。遇到之前的记录中存在同一趟公交ID的则切断
//              //            （5）最终出行链中的出行起点和目的地之间的距离，远小于起点距离中间换乘站，或者终点距离中间换乘站
//              //            （5）一次OD记录超过三次,而且第一次O与最后一次D的距离少于2KM则切断为两个OD
//              //            （6）如果是两条记录,(a,b)(b,c),dis(a,c)少于2KM而且dis(a,b)大于2*dis(a,c)+0.5则切断为2条记录
//              //            （7）出行链最后一条记录
//              //            （8）其他
//              if (
//                ot - dt > 60 * 20 //这次下车和上次上车时间超过20分钟
//              ) {
//                ruleFlag = true
//                // 终点
//                for (j <- 11 until destination.length) {
//                  trip(j) = destination(j)
//                }
//                trip(modelIndex) = lastMode + "_rule1" //保存数据
//                trip(trainNumIndex) = lastTrainNum //保存数据
//                val transfer: Int = i - lastIndex //换乘次数
//                BusTripChain.append(trip.mkString(",") + "," + transfer)
//
//              }
//              //            else if(
//              //              distanceKM > 0.7 //或者这次下车和上次上车距离超过0.7KM,则认为这次出行链已经结束
//              //            ){
//              //              ruleFlag = true
//              //              // 终点
//              //              for (j <- 11 until destination.length) {
//              //                trip(j) = destination(j)
//              //              }
//              //              trip(modelIndex) = lastMode + "_rule2" //保存数据
//              //              trip(trainNumIndex) = lastTrainNum //保存数据
//              //              val transfer: Int = i - lastIndex //换乘次数
//              //              BusTripChain.append(trip.mkString(",") + "," + transfer)
//              //            }
//              //            else if(
//              //              (origin(modelIndex).equals("Sub") && lastMode.contains("Sub")) //不存在两次地铁记录
//              //            ){
//              //              ruleFlag = true
//              //              // 终点
//              //              for (j <- 11 until destination.length) {
//              //                trip(j) = destination(j)
//              //              }
//              //              trip(modelIndex) = lastMode + "_rule3" //保存数据
//              //              trip(trainNumIndex) = lastTrainNum //保存数据
//              //              val transfer: Int = i - lastIndex //换乘次数
//              //              BusTripChain.append(trip.mkString(",") + "," + transfer)
//              //            }
//              //            else if(
//              //              (origin(modelIndex).equals("Bus") && lastTrainNum.contains(origin(trainNumIndex))) //一个出行链不可能有两次车次是一样的
//              //            ){
//              //              ruleFlag = true
//              //              // 终点
//              //              for (j <- 11 until destination.length) {
//              //                trip(j) = destination(j)
//              //              }
//              //              trip(modelIndex) = lastMode + "_rule4" //保存数据
//              //              trip(trainNumIndex) = lastTrainNum //保存数据
//              //              val transfer: Int = i - lastIndex //换乘次数
//              //              BusTripChain.append(trip.mkString(",") + "," + transfer)
//              //            }else if(
//              //            //本次起点与下次终点很近,那就划分为两个出行链,如(a,b),(b,c)如果是ac很近,ab却是很远,则划分两次出行
//              //              (distanceKM2 < 2.0 && (distanceKM3 > (2 * distanceKM2 + 0.5)))
//              //            ){
//              //              ruleFlag = true
//              //              // 终点
//              //              for (j <- 11 until destination.length) {
//              //                trip(j) = destination(j)
//              //              }
//              //              trip(modelIndex) = lastMode + "_rule6" //保存数据
//              //              trip(trainNumIndex) = lastTrainNum //保存数据
//              //              val transfer: Int = i - lastIndex //换乘次数
//              //              BusTripChain.append(trip.mkString(",") + "," + transfer)
//              //            } else if(
//              //            //一个出行链换乘超过2次而且第一个出行地跟第最后一个目的地很近,如(a,b),(b,c),(
//              //              (odBuf.size > 2 && distanceKM4 < 2)
//              //            // 存在(a,b,地铁)(b,c,公交)(c,a,公交)怎么处理? 如果换乘本来就是很近怎么办?
//              //            ) {
//              //                ruleFlag = true
//              //                val firstOD: Array[String] = odBuf(0).split(",")
//              //
//              //                var maxDistance: Double =getDistatce(firstOD(3).toDouble, firstOD(4).toDouble,
//              //                  firstOD(13).toDouble, firstOD(14).toDouble) //本次的距离
//              //                var maxDistanceIndex = 0
//              //                for (k <- 1 until  odBuf.size-1) {
//              //                  val cur: Array[String] = odBuf(k).split(",")
//              //                  val CurDisstance: Double = getDistatce(firstOD(3).toDouble, firstOD(4).toDouble,
//              //                    cur(13).toDouble, cur(14).toDouble) //第一条记录的起点到本条记录的终点的距离
//              //                  if (CurDisstance > maxDistance){
//              //                    maxDistance = CurDisstance;
//              //                    maxDistanceIndex = k;
//              //                  }
//              //                }
//              //                for (j <- 0 until 11) { //第一个OD的起点
//              //                  trip(j) = firstOD(j)
//              //                }
//              //                val FirstDestination: Array[String] = odBuf(maxDistanceIndex).split(",")
//              //                // 终点
//              //                for (j <- 11 until FirstDestination.length) {
//              //                  trip(j) = FirstDestination(j)
//              //                }
//              //                lastMode = firstOD(modelIndex) + "_rule51"
//              //                for (k <- 1 until maxDistanceIndex+1)
//              //                  lastMode = lastMode + "_" + odBuf(k).split(",")(modelIndex) + "_rule51"
//              //                trip(modelIndex) = lastMode //保存数据
//              //                var transfer: Int = maxDistanceIndex - 0
//              //                BusTripChain.append(trip.mkString(",") + "," + transfer)
//              //
//              //                val secondOD: Array[String] = odBuf(maxDistanceIndex+1).split(",")
//              //                for (j <- 0 until 11) { //第一个OD的起点
//              //                  trip(j) = secondOD(j)
//              //                }
//              //                val secondDestination: Array[String] = odBuf(odBuf.size-1).split(",")
//              //                // 终点
//              //                for (j <- 11 until secondDestination.length) {
//              //                  trip(j) = secondDestination(j)
//              //                }
//              //                lastMode = secondOD(modelIndex+1)+ "_rule52"
//              //                for (k <- maxDistanceIndex+1 until odBuf.length)
//              //                  lastMode = lastMode + "_" + odBuf(k).split(",")(modelIndex)+ "_rule52"
//              //                trip(modelIndex) = lastMode //保存数据
//              //                transfer = odBuf.length - 1 - maxDistanceIndex
//              //                BusTripChain.append(trip.mkString(",") + "," + transfer)
//              //
//              //              }
//              else {
//                val value: Array[String] = dataSortByOt(i + 1)._4.split(",") //得到终点的字
//                lastMode = lastMode + "_" + value(modelIndex) + "_rule0"
//                lastTrainNum = lastTrainNum + "_" + value(trainNumIndex)
//              }
//              if (ruleFlag) {
//                //起点
//                for (j <- 0 until 11) { //新的起点
//                  trip(j) = origin(j)
//                }
//
//                lastMode = origin(modelIndex)
//                lastTrainNum = origin(trainNumIndex)
//                lastIndex = i + 1
//                odBuf.clear() //新的数据开始保存,需要先清空
//              }
//
//              odBuf.append(dataSortByOt(i + 1)._4)
//              i = i + 1;
//            }
//            org.append(dataSortByOt(dataSortByOt.length - 1)._4)
//            val lastTrip: Array[String] = dataSortByOt(dataSortByOt.length - 1)._4.split(",")
//            for (j <- 11 until lastTrip.length) {
//              trip(j) = lastTrip(j)
//            }
//            //          if (!lastMode.equals(trip(modelIndex)))
//            //            trip(trip.length - 1) = lastMode + trip(modelIndex)
//
//            if (dataSortByOt.length == 1) {
//              trip(modelIndex) = lastMode + "_rule7"
//            }
//            else if (lastIndex == dataSortByOt.length - 1) { //切断出行链后只有一条记录
//              trip(modelIndex) = lastMode + "_rule7"
//            }
//            else {
//              trip(modelIndex) = lastMode + "_rule8"
//            }
//            //          trip(modelIndex) = lastMode + "_rule7"
//
//            trip(trainNumIndex) = lastTrainNum
//            val transfer: Int = dataSortByOt.length - 1 - lastIndex
//            BusTripChain.append(trip.mkString(",") + "," + transfer)
//            (id, (org, BusTripChain))
//          })
//          .filter(line => {
//            var flag = false
//            val BusTripChain: ArrayBuffer[String] = line._2._2
//            val inner = new Breaks
//            inner.breakable {
//
//
//              for (i <- BusTripChain) {
//
//                val strings1: Array[String] = i.split(",")
//
//                val modes: Array[String] = strings1(modelIndex).split("_")
//
//                val transferIndex = 18
//
//                //              // 如果有条件5就直接退出
//                //              if(strings1(modelIndex).contains("rule5")){
//                //                flag = true;
//                //                inner.break
//                //              }
//
//                //
//
//                if (strings1(transferIndex).toInt >= 4) {
//                  flag = true;
//                  inner.break
//                }
//
//                //              // 使用集合来判断出行链有没有两个车次是一样的.
//                //              val trainNumIndex = 9 //车次
//                //              val trainNums: Array[String] = strings1(trainNumIndex).split("_")
//                //              val set = new mutable.HashSet[String]
//                //              for (j <- trainNums.indices) {
//                //                if (!trainNums(j).equals("-")) {
//                //                  if(set.contains(trainNums(j))) {
//                //                    flag = true
//                //                    inner.break
//                //                  }
//                //                  set.add(trainNums(j))
//                //                }
//                //              }
//
//                //              if (strings1(strings1.length - 1).toInt > 1) { //最少换乘2次
//                //                flag = true
//                //                inner.break
//                //              }
//
//                //                val distanceKM: Double = getDistatce(strings1(3).toDouble, strings1(4).toDouble,
//                //                  strings1(13).toDouble, strings1(14).toDouble)
//                //                if (strings1(strings1.length - 1).toInt > 2 //最少换乘1次
//                //                  && distanceKM < 2.0//and OD之间距离小于1km;
//                //                ) {
//                //                  flag = true
//                //                  inner.break
//                //                }
//
//                //              //距离少于2KM
//                //              val distanceKM: Double = getDistatce(strings1(3).toDouble, strings1(4).toDouble,
//                //                strings1(13).toDouble, strings1(14).toDouble)
//                //              if (distanceKM < 0.7) {
//                //                flag = true
//                //                inner.break()
//                //              }
//
//                //              var SubNum = 0
//                //              // 2个Sub
//                //              for (j <- modes.indices) {
//                //                if (modes(j).equals("Sub")) {
//                //                  SubNum += 1
//                //                }
//                //                if (SubNum>=2) {
//                //                  flag = true
//                //                  inner.break
//                //                }
//                //              }
//
//                //              var BubNum = 0
//                //              // 2个Bus
//                //              for (j <- modes.indices) {
//                //                if (modes(j).equals("Bus")) {
//                //                  BubNum += 1
//                //                }
//                //                if (BubNum>=2) {
//                //                  flag = true
//                //                  inner.break
//                //                }
//                //              }
//
//
//                //              if (strings1(modelIndex).contains("Sub") && strings1(modelIndex).contains("Bus")) { //换乘包括地铁公交的
//                //                flag = true
//                //                inner.break
//                //              }
//
//
//                //                var SubFlag = false
//                //                // 先有sub,再有bus
//                //                for (j <- modes.indices) {
//                //                  if (modes(j).equals("Sub")){
//                //                    SubFlag = true
//                //                  }
//                //                  else if (SubFlag && modes(j).equals("Bus")) {
//                //                    flag = true
//                //                    inner.break
//                //                  }
//                //                }
//
//              }
//
//
//              //            val strings2: Array[String] = strings1(strings1.length - 2).split(",")
//              //            if (strings2.contains("Sub") && strings2.contains("Bus")) { //换乘包括地铁公交的
//              //              flag = true
//              //              inner.break
//              //            }
//
//              //            var SubFlag = false
//              //            // 先有sub,再有bus
//              //            for (j <- strings.indices) {
//              //              if (strings(j).equals("Sub")){
//              //                SubFlag = true
//              //              }
//              //              else if (SubFlag && strings(j).equals("Bus")) {
//              //                flag = true
//              //                inner.break
//              //              }
//              //            }
//
//              //            for (i <- BusTripChain) {
//              //              val strings: Array[String] = i.split(",")
//              //              var SubFlag = false
//              //              // 先有sub,再有Sus
//              //              for (j <- strings.indices) {
//              //                if (strings(j).equals("Sub")) {
//              //                  SubFlag = true
//              //                }
//              //                else if (SubFlag && strings(j).equals("Sub")) {
//              //                  flag = true
//              //                  inner.break
//              //                }
//              //              }
//              //            }
//
//              //            var BusFlag = false
//              //            // 先有bus,再有sub
//              //            for (j <- strings.indices) {
//              //              if (strings(j).equals("Bus")){
//              //                BusFlag = true
//              //              }
//              //              else if (BusFlag && strings(j).equals("Sub")) {
//              //                flag = true
//              //                inner.break
//              //              }
//              //            }
//
//            }
//            flag
//          })
//          .map(_._1) //取出ID
//          .collect().toArray
//        //      for (j <- targetIds.indices){
//        //        println(targetIds(j))
//        //      }
//        //      println(targetIds.contains("665364083"))

        val unionRdd = Rdd1
//          .filter(line => {
//            targetIds.contains(line._1)
//          })
          .union(HomeRDData).union(WorkRDDData)
          .groupByKey()

          .map(line => {
            val id: String = line._1
            val data: Array[(Long, Long, Long, String)] = line._2.toArray

            val tmp2: Array[(Long, Long, Long, String)] = data.sortBy(item => item._2) //原始数组不变,排序后多一个新的
            var sliceStartIndex = 0


            val triIdIndex = 0
            val triOtIndex = 1
            val triOStationIndex = 2
            val triOlatIndex = 3
            val triOlonIndex = 4
            val triDtIndex = 11
            val triDStationIndex = 12
            val triDlatIndex = 13
            val triDlonIndex = 14
            val latIndex = 3
            val lonIndex = 4
            var homeLat = 0.0
            var homeLon = 0.0
            var workLat = 0.0
            var workLon = 0.0
            if (tmp2(0)._2==0l){ //家一定是0
              val homeDatas = tmp2(0)._4.split(",")
              homeLat = homeDatas(latIndex).toDouble
              homeLon = homeDatas(lonIndex).toDouble
              sliceStartIndex = sliceStartIndex + 1
            }

            if (tmp2(0)._2==1l){//假设没有家,只有工作地,
              val workDatas = tmp2(0)._4.split(",")
              workLat = workDatas(latIndex).toDouble
              workLon = workDatas(lonIndex).toDouble
              sliceStartIndex = sliceStartIndex + 1
            }
            else if (tmp2.length > 1 && tmp2(1)._2==1l){
              val workDatas = tmp2(1)._4.split(",")
              workLat = workDatas(latIndex).toDouble
              workLon = workDatas(lonIndex).toDouble
              sliceStartIndex = sliceStartIndex + 1
            }



            var BusTripChain = new ArrayBuffer[String]

            // 存在有的ID只有家和工作地,没有记录的问题
            if (sliceStartIndex < tmp2.length) {


              val dataSortByOt: Array[(Long, Long, Long, String)] = tmp2.slice(sliceStartIndex, tmp2.length)

              //          val maxTripTime = 30;
              var i = 0;

              var BusTripChainBuf = new ArrayBuffer[String]

              var org = new ArrayBuffer[String] //排序后原始的od记录
              var trip: Array[String] = dataSortByOt(0)._4.split(",")
              var lastIndex = 0; // 用于计算换乘
              var lastMode: String = trip(modelIndex)

              //          val trainNumIndex = 9 //车次
              var lastTrainNum: String = trip(trainNumIndex)

              var odBuf = new ArrayBuffer[Array[String]]
              //          odBuf.append(dataSortByOt(0)._4) //i从0开始就不增加这句话
              odBuf.append(dataSortByOt(0)._4.split(",")) // 下面的保存都是从1开始的.不加会丢失数据
              BusTripChainBuf.append(dataSortByOt(0)._4)



              while (i < dataSortByOt.length - 1) {
                org.append(dataSortByOt(i)._4)
                val origin: Array[String] = dataSortByOt(i + 1)._4.split(',')
                var destination: Array[String] = dataSortByOt(i)._4.split(',') //得到终点的字串
                val distanceKM: Double = getDistatce(origin(3).toDouble, origin(4).toDouble,
                  destination(13).toDouble, destination(14).toDouble)


                val ot: Long = dataSortByOt(i + 1)._2
                val dt: Long = dataSortByOt(i)._3

                var loopIndex: Int = -1
                if (odBuf.size > 1)
                  loopIndex = containLoop(odBuf)

                var clearFlag = false

                //            目前的并行规则：
                //            （1）判断现有乘车记录中是否有环,存在则在环内非入环点钟最大逗留时间处切断,剩下部分继续进行下列判断
                //
                //            （1）两次乘车之间的时间间隔大于20分钟则切断
                //            （2）两次乘车前一次D与下一次O的距离大于0.7km则切断
                //            （3）不存在一次出行中包括两次地铁记录。
                //            （4）不存在一次出行中包括同一趟公交线路的数据。
                if (
                // 将这个条件弄上来可以避免(a,b),(b,a), (a,c)第二条记录满足规则1-4之一,切分为(a,a),(a,c)
                //
                //判断出行链中有没有环,
                // 判断环的条件是,假设两点之间距离很近,但是中间的距离却是很远,
                // 如果两点之间距离很近但是总监距离也很近怎么办?
                // 有环的话在两个环之间切断,切断的条件是在终点停留时间最长就切断(这里存在问题),不需要再计算,直接在环切断
                //切断就未必构成两个OD,但是有环一定有一个OD,后面的保持状态,等待下一次判断
                // (a,b), (b,a),(a,c),(c,d) - >实际是(a,b),(b,d),如果保持则会变成(a,b),(b,a),(a,d)
                  odBuf.size > 1 && loopIndex >= 0
                ) {

                  val firstOD: Array[String] = odBuf(0)

                  var maxDurationIndex: Int = loopIndex
                  var maxDuration: Long = transTimeV2ToTimestamp(odBuf(loopIndex + 1)(otIndex)) -
                    transTimeV2ToTimestamp(odBuf(loopIndex)(otIndex))
                  for (k <- loopIndex + 1 until odBuf.size - 1) {
                    val Duration: Long = transTimeV2ToTimestamp(odBuf(k + 1)(otIndex)) -
                      transTimeV2ToTimestamp(odBuf(k)(dtIndex))
                    if (maxDuration < Duration) {
                      maxDuration = Duration
                      maxDurationIndex = k
                    }
                  }

                  for (j <- 0 until 11) { //第一个OD的起点
                    trip(j) = firstOD(j)
                  }
                  val FirstDestination: Array[String] = odBuf(maxDurationIndex)
                  // 终点
                  for (j <- 11 until FirstDestination.length) {
                    trip(j) = FirstDestination(j)
                  }
                  //                lastMode = firstOD(modelIndex) + "_rule51"
                  lastMode = firstOD(modelIndex)
                  for (k <- 1 until maxDurationIndex + 1)
                    lastMode = lastMode + "_" + odBuf(k)(modelIndex)
                  trip(modelIndex) = lastMode //保存数据

                  var transfer: Int = maxDurationIndex - 0

                  var newTrip = new ArrayBuffer[String]()
                  newTrip.append(trip(triIdIndex))
                  newTrip.append(trip(triOStationIndex))
                  newTrip.append(trip(triOtIndex))
                  newTrip.append(trip(triOlatIndex))
                  newTrip.append(trip(triOlonIndex))
                  newTrip.append(trip(triDtIndex))
                  newTrip.append(trip(triDStationIndex))
                  newTrip.append(trip(triDlatIndex))
                  newTrip.append(trip(triDlonIndex))
                  newTrip.append(transfer.toString)
                  newTrip.append(homeLat.toString)
                  newTrip.append(homeLon.toString)
                  newTrip.append(workLat.toString)
                  newTrip.append(workLon.toString)
                  BusTripChainBuf.clear()
                  var TransferStationSequence = new ArrayBuffer[String]()
                  for (i <- 0 to maxDurationIndex) {
                    BusTripChainBuf.append(odBuf(i).mkString(";"))
                    val TransferStationOneData = new ArrayBuffer[String]()
                    TransferStationOneData.append(odBuf(i)(triOStationIndex))
                    TransferStationOneData.append(odBuf(i)(triOlatIndex))
                    TransferStationOneData.append(odBuf(i)(triOlonIndex))
                    TransferStationOneData.append(odBuf(i)(triDStationIndex))
                    TransferStationOneData.append(odBuf(i)(triDlatIndex))
                    TransferStationOneData.append(odBuf(i)(triDlonIndex))
                    TransferStationSequence.append(TransferStationOneData.mkString(";"))
                  }

                  newTrip.append(TransferStationSequence.mkString("\t"))
                  newTrip.append(BusTripChainBuf.mkString("\t"))
                  BusTripChain.append(newTrip.mkString(","))

                  // 还原
                  //起点
                  for (j <- 0 until 11) { //新的起点
                    trip(j) = odBuf(maxDurationIndex + 1)(j)
                  }

                  //                lastMode = odBuf(maxDurationIndex + 1)(modelIndex) + "_rule52"
                  lastMode = odBuf(maxDurationIndex + 1)(modelIndex)
                  lastTrainNum = odBuf(maxDurationIndex + 1)(trainNumIndex)
                  lastIndex = i - (odBuf.length - maxDurationIndex) + 2

                  var tmpBuf = new ArrayBuffer[Array[String]];
                  for (j <- maxDurationIndex + 2 until (odBuf.length - 1)) {
                    //                  lastMode = lastMode + "_" + odBuf(j)(modelIndex) + "_rule52"
                    lastMode = lastMode + "_" + odBuf(j)(modelIndex)
                    lastTrainNum = lastTrainNum + "_" + odBuf(j)(trainNumIndex)
                    tmpBuf.append(odBuf(j))
                    BusTripChainBuf.append(odBuf(j).mkString(","))
                    //                lastIndex = lastIndex - 1
                  }
                  tmpBuf.append(odBuf(odBuf.length - 1))

                  odBuf.clear() //新的数据开始保存,需要先清空
                  tmpBuf.copyToBuffer(odBuf)
                  tmpBuf.clear()

                }
                if (
                  ot - dt > 60 * 20 //这次下车和上次上车时间超过20分钟
                ) {
                  clearFlag = true
                  // 终点
                  for (j <- 11 until destination.length) {
                    trip(j) = destination(j)
                  }

                }
                else if (
                  distanceKM > 0.7 //或者这次下车和上次上车距离超过0.7KM,则认为这次出行链已经结束
                ) {
                  clearFlag = true
                  // 终点
                  for (j <- 11 until destination.length) {
                    trip(j) = destination(j)
                  }
                }
                else if (
                  (origin(modelIndex).equals("Sub") && lastMode.contains("Sub")) //不存在两次地铁记录
                ) {
                  clearFlag = true
                  // 终点
                  for (j <- 11 until destination.length) {
                    trip(j) = destination(j)
                  }

                }
                else if (
                  (origin(modelIndex).equals("Bus") && lastTrainNum.contains(origin(trainNumIndex))) //一个出行链不可能有两次车次是一样的
                ) {
                  clearFlag = true
                  // 终点
                  for (j <- 11 until destination.length) {
                    trip(j) = destination(j)
                  }
                }


                else {
                  clearFlag = false
                  val value: Array[String] = dataSortByOt(i + 1)._4.split(",") //得到终点的字
                  //                lastMode = lastMode + "_" + value(modelIndex) + "_rule0"
                  lastMode = lastMode + "_" + value(modelIndex)
                  lastTrainNum = lastTrainNum + "_" + value(trainNumIndex)
                }

                if (clearFlag) {
                  trip(modelIndex) = lastMode
                  trip(trainNumIndex) = lastTrainNum //保存数据
                  val transfer: Int = i - lastIndex //换乘次数


                  var newTrip = new ArrayBuffer[String]()
                  newTrip.append(trip(triIdIndex))
                  newTrip.append(trip(triOStationIndex))
                  newTrip.append(trip(triOtIndex))
                  newTrip.append(trip(triOlatIndex))
                  newTrip.append(trip(triOlonIndex))
                  newTrip.append(trip(triDtIndex))
                  newTrip.append(trip(triDStationIndex))
                  newTrip.append(trip(triDlatIndex))
                  newTrip.append(trip(triDlonIndex))
                  newTrip.append(transfer.toString)
                  newTrip.append(homeLat.toString)
                  newTrip.append(homeLon.toString)
                  newTrip.append(workLat.toString)
                  newTrip.append(workLon.toString)

                  var TransferStationSequence = new ArrayBuffer[String]()
                  BusTripChainBuf.clear()
                  for (i <- 0 until odBuf.length) {
                    BusTripChainBuf.append(odBuf(i).mkString(";"))
                    val TransferStationOneData = new ArrayBuffer[String]()
                    TransferStationOneData.append(odBuf(i)(triOStationIndex))
                    TransferStationOneData.append(odBuf(i)(triOlatIndex))
                    TransferStationOneData.append(odBuf(i)(triOlonIndex))
                    TransferStationOneData.append(odBuf(i)(triDStationIndex))
                    TransferStationOneData.append(odBuf(i)(triDlatIndex))
                    TransferStationOneData.append(odBuf(i)(triDlonIndex))
                    TransferStationSequence.append(TransferStationOneData.mkString(";"))

                  }
                  newTrip.append(TransferStationSequence.mkString("\t"))
                  newTrip.append(BusTripChainBuf.mkString("\t"))
                  BusTripChain.append(newTrip.mkString(","))


                  //起点
                  for (j <- 0 until 11) { //新的起点
                    trip(j) = origin(j)
                  }

                  lastMode = origin(modelIndex)
                  lastTrainNum = origin(trainNumIndex)
                  lastIndex = i + 1
                  odBuf.clear() //新的数据开始保存,需要先清空

                }

                odBuf.append(origin)
                BusTripChainBuf.append(dataSortByOt(i + 1)._4)
                i = i + 1
              }
              org.append(dataSortByOt(dataSortByOt.length - 1)._4)
              val lastTrip: Array[String] = dataSortByOt(dataSortByOt.length - 1)._4.split(",")
              for (j <- 11 until lastTrip.length) {
                trip(j) = lastTrip(j)
              }
              //          if (!lastMode.equals(trip(modelIndex)))
              //            trip(trip.length - 1) = lastMode + trip(modelIndex)

              if (dataSortByOt.length == 1) {
                //              trip(modelIndex) = lastMode + "_rule7"
                trip(modelIndex) = lastMode
              }
              else if (lastIndex == dataSortByOt.length - 1) { //切断出行链后只有一条记录
                //              trip(modelIndex) = lastMode + "_rule7"
                trip(modelIndex) = lastMode
              }
              else {
                //              trip(modelIndex) = lastMode + "_rule8"
                trip(modelIndex) = lastMode
              }
              //          trip(modelIndex) = lastMode + "_rule7"

              trip(trainNumIndex) = lastTrainNum
              val transfer: Int = dataSortByOt.length - 1 - lastIndex

              var newTrip = new ArrayBuffer[String]()
              newTrip.append(trip(triIdIndex))
              newTrip.append(trip(triOStationIndex))
              newTrip.append(trip(triOtIndex))
              newTrip.append(trip(triOlatIndex))
              newTrip.append(trip(triOlonIndex))
              newTrip.append(trip(triDtIndex))
              newTrip.append(trip(triDStationIndex))
              newTrip.append(trip(triDlatIndex))
              newTrip.append(trip(triDlonIndex))
              newTrip.append(transfer.toString)
              newTrip.append(homeLat.toString)
              newTrip.append(homeLon.toString)
              newTrip.append(workLat.toString)
              newTrip.append(workLon.toString)

              var TransferStationSequence = new ArrayBuffer[String]()
              BusTripChainBuf.clear()
              for (i <- 0 until odBuf.length) {
                BusTripChainBuf.append(odBuf(i).mkString(";"))
                val TransferStationOneData = new ArrayBuffer[String]()
                TransferStationOneData.append(odBuf(i)(triOStationIndex))
                TransferStationOneData.append(odBuf(i)(triOlatIndex))
                TransferStationOneData.append(odBuf(i)(triOlonIndex))
                TransferStationOneData.append(odBuf(i)(triDStationIndex))
                TransferStationOneData.append(odBuf(i)(triDlatIndex))
                TransferStationOneData.append(odBuf(i)(triDlonIndex))
                TransferStationSequence.append(TransferStationOneData.mkString(";"))
              }
              newTrip.append(TransferStationSequence.mkString("\t"))
              newTrip.append(BusTripChainBuf.mkString("\t"))
              BusTripChain.append(newTrip.mkString(","))
            }

            BusTripChain
          })
          .filter(line => {
            line.nonEmpty
          })
          .flatMap(line =>{
              val resArrayBuffer: mutable.Seq[String] = line
              resArrayBuffer
          })


        //        .filter(line=>{
        //          var flag = false
        //          val BusTripChain: ArrayBuffer[String] = line._2._2
        //          val inner = new Breaks;
        //          inner.breakable {
        //
        //
        //            for (i <- BusTripChain) {
        //
        //              val strings1: Array[String] = i.split(",")
        //              val modelIndex = 16
        //              val modes: Array[String] = strings1(modelIndex).split("_")
        //
        //              val transferIndex = 19
        //
        //              //              // 如果有条件5就直接退出
        //              //              if(strings1(modelIndex).contains("rule5")){
        //              //                flag = true;
        //              //                inner.break
        //              //              }
        //
        //              //
        //
        ////              if(strings1(transferIndex).toInt >= 4){
        ////                flag = true;
        ////                inner.break
        ////              }
        //
        //              //              // 使用集合来判断出行链有没有两个车次是一样的.
        //              //              val trainNumIndex = 9 //车次
        //              //              val trainNums: Array[String] = strings1(trainNumIndex).split("_")
        //              //              val set = new mutable.HashSet[String]
        //              //              for (j <- trainNums.indices) {
        //              //                if (!trainNums(j).equals("-")) {
        //              //                  if(set.contains(trainNums(j))) {
        //              //                    flag = true
        //              //                    inner.break
        //              //                  }
        //              //                  set.add(trainNums(j))
        //              //                }
        //              //              }
        //
        //              //              if (strings1(strings1.length - 1).toInt > 1) { //最少换乘2次
        //              //                flag = true
        //              //                inner.break
        //              //              }
        //
        //              //                val distanceKM: Double = getDistatce(strings1(3).toDouble, strings1(4).toDouble,
        //              //                  strings1(13).toDouble, strings1(14).toDouble)
        //              //                if (strings1(strings1.length - 1).toInt > 2 //最少换乘1次
        //              //                  && distanceKM < 2.0//and OD之间距离小于1km;
        //              //                ) {
        //              //                  flag = true
        //              //                  inner.break
        //              //                }
        //
        //              //              //距离少于2KM
        //              //              val distanceKM: Double = getDistatce(strings1(3).toDouble, strings1(4).toDouble,
        //              //                strings1(13).toDouble, strings1(14).toDouble)
        //              //              if (distanceKM < 0.7) {
        //              //                flag = true
        //              //                inner.break()
        //              //              }
        //
        //              //              var SubNum = 0
        //              //              // 2个Sub
        //              //              for (j <- modes.indices) {
        //              //                if (modes(j).equals("Sub")) {
        //              //                  SubNum += 1
        //              //                }
        //              //                if (SubNum>=2) {
        //              //                  flag = true
        //              //                  inner.break
        //              //                }
        //              //              }
        //
        //              //              var BubNum = 0
        //              //              // 2个Bus
        //              //              for (j <- modes.indices) {
        //              //                if (modes(j).equals("Bus")) {
        //              //                  BubNum += 1
        //              //                }
        //              //                if (BubNum>=2) {
        //              //                  flag = true
        //              //                  inner.break
        //              //                }
        //              //              }
        //
        //
        //              //              if (strings1(modelIndex).contains("Sub") && strings1(modelIndex).contains("Bus")) { //换乘包括地铁公交的
        //              //                flag = true
        //              //                inner.break
        //              //              }
        //
        //
        //              //                var SubFlag = false
        //              //                // 先有sub,再有bus
        //              //                for (j <- modes.indices) {
        //              //                  if (modes(j).equals("Sub")){
        //              //                    SubFlag = true
        //              //                  }
        //              //                  else if (SubFlag && modes(j).equals("Bus")) {
        //              //                    flag = true
        //              //                    inner.break
        //              //                  }
        //              //                }
        //
        //            }
        //
        //
        //            //            val strings2: Array[String] = strings1(strings1.length - 2).split(",")
        //            //            if (strings2.contains("Sub") && strings2.contains("Bus")) { //换乘包括地铁公交的
        //            //              flag = true
        //            //              inner.break
        //            //            }
        //
        //            //            var SubFlag = false
        //            //            // 先有sub,再有bus
        //            //            for (j <- strings.indices) {
        //            //              if (strings(j).equals("Sub")){
        //            //                SubFlag = true
        //            //              }
        //            //              else if (SubFlag && strings(j).equals("Bus")) {
        //            //                flag = true
        //            //                inner.break
        //            //              }
        //            //            }
        //
        //            //            for (i <- BusTripChain) {
        //            //              val strings: Array[String] = i.split(",")
        //            //              var SubFlag = false
        //            //              // 先有sub,再有Sus
        //            //              for (j <- strings.indices) {
        //            //                if (strings(j).equals("Sub")) {
        //            //                  SubFlag = true
        //            //                }
        //            //                else if (SubFlag && strings(j).equals("Sub")) {
        //            //                  flag = true
        //            //                  inner.break
        //            //                }
        //            //              }
        //            //            }
        //
        //            //            var BusFlag = false
        //            //            // 先有bus,再有sub
        //            //            for (j <- strings.indices) {
        //            //              if (strings(j).equals("Bus")){
        //            //                BusFlag = true
        //            //              }
        //            //              else if (BusFlag && strings(j).equals("Sub")) {
        //            //                flag = true
        //            //                inner.break
        //            //              }
        //            //            }
        //
        //          }
        //          flag
        //        })

        //        .flatMap(line =>{
        //          val resArrayBuffer: mutable.Seq[String] = line._2._2
        //          resArrayBuffer
        //        })//将数组变成多条记录
        //        .map(line =>{
        //          val fields: Array[String] = line.split(',')
        //          val ot: Long = transTimeV2ToTimestamp(fields(1)) //(单位S)
        //          (ot,line)
        //      })
        //        .sortByKey()//按照ot排序
        //        .map(_._2)//只取line,不取ot


        val savePath: String = savePathPre + "\\" + dataName
        val bool: Boolean = deletePath(savePath)
        if (bool) {
          println("文件删除成功")
        } else {
          println("文件不存在")
        }

        println(savePath)
        unionRdd.repartition(10).saveAsTextFile(savePath)
//        unionRdd.saveAsTextFile(savePath)
        val end_time =new Date().getTime
        println("costtime = " + (end_time-start_time)/1000.0) //单位毫秒

      }
    }

    sc.stop()
  }
}



import GeneralFunctionSets.{deleteFilePath, deletePath, getDistatce, transTimeV2ToTimestamp}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ArrayBuffer
import org.gavaghan.geodesy.Ellipsoid
import org.gavaghan.geodesy.GeodeticCalculator
import org.gavaghan.geodesy.GeodeticCurve
import org.gavaghan.geodesy.GlobalCoordinates
import org.jets3t.service.utils.ServiceUtils

import scala.collection.mutable

object SubwayAndBusTrip2ToOD {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("SubwayAndBusTrip2ToOD")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    val BusDataPath : String ="D:\\subwayData\\mutiOD\\tripChaining"
    val SubDataPath : String ="D:\\subwayData\\mutiOD\\SubwayODOut"
    val savePathPre : String ="D:\\subwayData\\mutiOD\\SubAndBusDataOut"

//    val BusDataPath : String ="D:\\subwayData\\mutiOD\\tripChainingTest"
//    val SubDataPath : String ="D:\\subwayData\\mutiOD\\SubwayODOutTest"
//    val savePathPre : String ="D:\\subwayData\\mutiOD\\SubAndBusDataOutTest"

    val file = new File(BusDataPath)
    val iterator: Iterator[File] = file.listFiles().iterator


    while (iterator.hasNext) {
      val dataPath: String = iterator.next().toString
      val strings: Array[String] = dataPath.split('\\')
      val dataName: String = strings(strings.length - 1)

      val resFile = sc.textFile(dataPath + "//part-00000"  + "," +
        SubDataPath + "//" + dataName + "//part-00000").map(line => {

        //   0           1                      2   3                  4                  5                     6    7                 8                   9
        // 326929564,2018-04-01T06:10:39.000Z,后瑞,113.835462650496,22.628844951543417,2018-04-01T07:06:45.000Z,西丽,113.95383461056423,22.580671029095516,Sub
        val fields: Array[String] = line.split(',')
        val id: String = fields(0)
        val ot: Long = transTimeV2ToTimestamp(fields(1)) //(单位S)

        val dt: Long = transTimeV2ToTimestamp(fields(5)) //(单位S)

        val trvaltime: Long = dt - ot


        //        (id, (trvaltime,ot, dt))
        //(id, (trvaltime,ot, dt,(id,ot,oName,oLat,oLong)))
        (id, (trvaltime, ot, dt, line))
      })
        .filter(line => {
          val trvaltime: Long = line._2._1
          trvaltime > 0 && trvaltime < 60 * 200 //200分钟
        }) // 去掉明显出问题的数据
        .groupByKey()

        //      .filter(line => {//将有bus和sub的过滤出来看看
        //        val data: Array[(Long, Long, Long, String)] = line._2.toArray
        //        val dataSortByOt: Array[(Long, Long, Long, String)] = data.sortBy(item => item._2) //原始数组不变,排序后多一个新的
        //        var i = 0
        //        var busFlag = false
        //        var subFlag = false
        //        while (i < dataSortByOt.length) {
        //          val datas: Array[String] = dataSortByOt(i)._4.split(",")
        //          if (datas(9).equals("Sub"))
        //            subFlag=true
        //          else  if (datas(9).equals("Bus"))
        //            busFlag=true
        //          i = i+1;
        //        }
        //        busFlag && subFlag
        //      }
        //      )
        .map(line => {
          val id: String = line._1
          val data: Array[(Long, Long, Long, String)] = line._2.toArray
          val dataSortByOt: Array[(Long, Long, Long, String)] = data.sortBy(item => item._2) //原始数组不变,排序后多一个新的

          var org = new ArrayBuffer[String] //排序后原始的od记录
          var ODs = new ArrayBuffer[String]
          var i = 0;

          var trip: Array[String] = dataSortByOt(0)._4.split(",")
          var lastMode: String = trip(trip.length - 1)

          var transfer = 0; // 用于计算换乘

          while (i < dataSortByOt.length - 1) {
            org.append(dataSortByOt(i)._4)
            val origin: Array[String] = dataSortByOt(i + 1)._4.split(',')
            val destination: Array[String] = dataSortByOt(i)._4.split(',')
            val distanceKM: Double = getDistatce(origin(3).toDouble, origin(4).toDouble, destination(7).toDouble, destination(8).toDouble)

            val ot: Long = dataSortByOt(i + 1)._2
            val dt: Long = dataSortByOt(i)._3

            transfer += destination(10).toInt

            if (ot - dt > 60 * 20 || //这次下车和上次上车时间超过20分钟,
              distanceKM > 0.7) { //或者这次下车和上次上车距离超过2KM,则认为这次出行链已经结束
              var value: String = dataSortByOt(i)._4 //得到终点的字串
              var strings1: Array[String] = value.split(",")
              for (j <- 5 until strings1.length) {
                trip(j) = strings1(j)
              }

              if (!lastMode.equals(trip(trip.length - 1))) // 将第一次上次出行模式放在前面,结束的出行模式放在后面
                trip(trip.length - 1) = lastMode + trip(trip.length - 1)

              value = dataSortByOt(i + 1)._4 //得到起点的字串
              strings1 = value.split(",")
              ODs.append(trip.mkString(",") + "," + transfer)
              for (j <- 0 until 5) { //新的终点为
                trip(j) = strings1(j)
              }
              lastMode = strings1(strings1.length - 1)
              transfer = 0;
            }
            i = i + 1
          }
          org.append(dataSortByOt(dataSortByOt.length - 1)._4)
          val strings1: Array[String] = dataSortByOt(dataSortByOt.length - 1)._4.split(",")
          for (j <- 5 until strings1.length) {
            trip(j) = strings1(j)
          }
          if (!lastMode.equals(trip(trip.length - 1)))
            trip(trip.length - 1) = lastMode + trip(trip.length - 1)
          transfer += strings1(10).toInt
          ODs.append(trip.mkString(",") + "," + transfer)
          (id, (org, ODs))
        })
            .flatMap(line =>{
              val resArrayBuffer: mutable.Seq[String] = line._2._2
              resArrayBuffer
            })//将数组变成多条记录
            .map(line =>{
              val fields: Array[String] = line.split(',')
              val ot: Long = transTimeV2ToTimestamp(fields(1)) //(单位S)
              (ot,line)
            })
            .sortByKey()//按照ot排序
            .map(line =>{
              line._2
            })


      val savePath: String = savePathPre + "\\" + dataName
      val bool: Boolean = deletePath(savePath)
      if (bool) {
        println("文件删除成功")
      } else {
        println("文件不存在")
      }

      resFile.repartition(1).saveAsTextFile(savePath)
    }
    sc.stop()

  }
}

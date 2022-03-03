import GeneralFunctionSets.{dayOfMonth_long, transTimeToTimestamp}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * AP和AFC数据统计对比
 * 包括出行天数、出行次数、花费时间三部分对比
 * AFC:(292870821,2019-06-15 21:12:46,科学馆,2019-06-15 21:27:04,市民中心)
 * -> (出行天数, 数量), (出行次数, 数量), (花费时间, 数量)
 *
 * AP:(1C151FD38BD0,2019-06-19 22:56:37,老街,378,2019-06-19 23:31:22,下沙,121)
 */
object AfcAndApContrast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AfcAndApContrast").setMaster("local")
    val sc = new SparkContext(conf)

    //var ApTextFile = "D:\\subwayData\\spark\\data\\zlt-hdfs\\UI\\SampledAPData\\part*"
    var ApTextFile = "D:\\subwayData\\spark\\data\\Destination\\subway-pair\\part-00000"
    var saveFilePath = "D:\\subwayData\\spark\\data\\zlt-hdfs\\UI\\zfr-AfcAndApContrast_n"
    var AfcTextFile = "D:\\subwayData\\spark\\data\\zlt-hdfs\\UI\\zfr-SampledAPData_n"

    val file=new File(saveFilePath)
    val file2=new File(AfcTextFile)

    // 读取深圳通卡数据
    // textFile : (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
    // subwayFile: (689414061,List((3947,6), (2746,8), (3859,14), (3425,14), (5364,16), (5269,22), (5330,23), (4078,25), (3823,27)))
    // subwayFile: (id,list((时长,日期), (时长,日期), ..., (时长,日期)))
    val subwayFile = sc.textFile(ApTextFile).map(line => {
      val fields = line.split(',')
      //println(fields(4))
      val id = fields(0).drop(1) // drop(1)去掉括号
      val ot = transTimeToTimestamp(fields(1)) //(单位MS)
      val dt = transTimeToTimestamp(fields(4)) //(单位MS)
      val dur = dt - ot // 一次出行的时间(单位MS)
      val day = dayOfMonth_long(ot)
      (id, (dur, day)) // 生成key value
    }).groupByKey().mapValues(_.toList)

    println("subwayFile",subwayFile.first())

    // (id, ((dur, day), (dur, day), ..., (dur, day)))
    // (689414061,List((3947,6), (2746,8), (3859,14), (3425,14), (5364,16), (5269,22), (5330,23), (4078,25), (3823,27)))
    // (id, (durs list), 出行次数, 出行天数)
    // => (689414061,List(3947, 2746, 3859, 3425, 5364, 5269, 5330, 4078, 3823),9,8)
    val processingAFC = subwayFile.map(line => {
      val daySets: mutable.Set[Int] = mutable.Set() // 可变集合, imutable.set() 不可变集合
      val durs = new ListBuffer[Long]
      line._2.foreach(x => { // x表示一行中的 (dur,day)列表
        daySets.add(x._2) // 取subwayFile的 (dur,day)列表的day
        durs.append(x._1) // 将坐车时间追加进
      })
      // (id，出行花费时间序列，出行次数，出行天数)
      (line._1, durs.toList, durs.length, daySets.size)
    }).cache()

    println("processingAFC",processingAFC.first())

    // 统计出行片段的时间长度分布,120s为一个单位
    // for循环中的 yield 会把当前的元素记下来，保存在集合(不去重)中，循环结束后将返回该集合。如果被循环的是Map，返回的就是Map，以此类推。
    //  val a = Array(1, 2, 3, 4, 5, 4 ,5); for (e <- a if e > 2) yield e, 结果为 Array(3, 4, 5, 5, 4)
    // 每个id 的 出行花费时间, 次数
    // (id，出行花费时间序列，出行次数，出行天数) -> (出行花费时间/120,1) -> '出行花费时间/120,数量'
    val travelTimeLengthAFC = processingAFC.flatMap(line => for (v <- line._2) yield (v / 120, 1))
      // reduceByKey会寻找相同key的数据，当找到这样的两条记录时会对其value(分别记为x,y)做(x,y) => x+y的处理，
      // 即只保留求和之后的数据作为value。反复执行这个操作直至每个key只留下一条记录。
      .reduceByKey(_ + _) // reduceByKey((x,y) => x+y) == reduceByKey(_ + _),
      .repartition(1) // 用coalesce代替
      .sortByKey()
      .map(x => x._1.toString + "," + x._2.toString)
//     travelTimeLengthAFC.saveAsTextFile(saveFilePath + "/AFC-TimeLength")
    println("travelTimeLengthAFC",travelTimeLengthAFC.first())

    // 统计出行次数分布
    val travelNumAFC = processingAFC.map(x => (x._3, 1)) // x 是出行次数
      .reduceByKey(_ + _)
      .repartition(1)
      .sortByKey()
//    travelNumAFC.saveAsTextFile(saveFilePath + "/AFC-Num")

    // 统计出行天数分布
    val travelDaysAFC = processingAFC.map(x => (x._4, 1)) // x是出行天数
      .reduceByKey(_ + _)
      .repartition(1)
      .sortByKey()
//    travelDaysAFC.saveAsTextFile(saveFilePath + "/AFC-Days")

    // AP数据格式:(000000000000,2019-06-01 10:38:05,布吉,0,2019-06-01 10:43:50,上水径,15)
    val macFile = sc.textFile(ApTextFile).map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val dt = transTimeToTimestamp(fields(4))
      val dur = dt - ot
      val day = dayOfMonth_long(ot)
      (id, (dur, day))
    }) // reuse subwayFile

    // 过滤掉出行片段时间超过3小时和小于0的数据
    // filteringData ==> (id, list(dur, day))
    val filteringData = macFile.filter(x => x._2._1 > 0 && x._2._1 < 10800) // x是(dur, day)列表 3*60*60=10800s
      .groupByKey() // id
      .mapValues(_.toList)

    println("filteringData",filteringData.first())

    val processingAP = filteringData.map(line => {
      val daySets: mutable.Set[Int] = mutable.Set()
      val durList = new ListBuffer[Long]
      line._2.foreach(x => {
        daySets.add(x._2)
        durList.append(x._1)
      })
      // (id，出行花费时间序列，出行次数，出行天数)
      (line._1, durList.toList, durList.length, daySets.size)
    }).cache()


    // 统计出行片段的时间长度分布
    val travelTimeLengthAP = processingAP.flatMap(line => for (v <- line._2) yield (v / 120, 1))
      .reduceByKey(_ + _)
      .repartition(1)
      .sortByKey()
      .map(x => x._1.toString + "," + x._2.toString)
//    travelTimeLengthAP.saveAsTextFile(saveFilePath + "/AP-TimeLength")

    // 统计出行次数分布
    val travelNumAP = processingAP.map(x => (x._3, 1))
      .reduceByKey(_ + _)
      .repartition(1)
      .sortByKey()
//    travelNumAP.saveAsTextFile(saveFilePath + "/AP-Num")

    // 统计出行天数分布
    val travelDaysAP = processingAP.map(x => (x._4, 1))
      .reduceByKey(_ + _)
      .repartition(1)
      .sortByKey()
//    travelDaysAP.saveAsTextFile(saveFilePath+ "/AP-Days")

    //    val sumAFC = countDaysOfAFC.map(_._2).sum()
    //    val sumAP = countDaysOfAP.map(_._2).sum()
    //
    //    println("sumAFC:" + sumAFC.toString)
    //    println("sumAP:" + sumAP.toString)

    sc.stop()
  }
}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{abs, max}

// 从AFC中选取部分数据和全部AP数据匹配
object ComparisonOfWeekAndMonth {
    def main(args: Array[String]): Unit = {
        //val conf = new SparkConf().setAppName("ComparisonOfWeekAndMonth")
        val conf = new SparkConf().setAppName("ComparisonOfWeekAndMonth").setMaster("local")
        val sc = new SparkContext(conf)

        // 读取地铁站点名和编号映射关系
        //val stationFile = sc.textFile(args(0) + "/liutao/AllInfo/stationInfo-UTF-8.txt")
        val stationFile = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\AllInfo\\stationInfo-UTF-8.txt")
        val stationNoToNameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNoToName = sc.broadcast(stationNoToNameRDD.collect().toMap)



        // 读取所有有效路径的数据
        //val validPathFile = sc.textFile(args(0) + "/liutao/AllInfo/allpath.txt").map(line => {
        // 1 2 3 4 5 # 0 V 0.0000 12.6500 -> 1 2 3 4 5, 其中1为起始站点, 5为目的地
        // 根据站点记录的得到((起始地, 目的地), 路径列表)
        val validPathFile = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\AllInfo\\allpath.txt").map(line => {
            // 仅保留站点编号信息
            val fields = line.split(' ').dropRight(5)
            val sou = stationNoToName.value(fields(0).toInt)
            val des = stationNoToName.value(fields(fields.length - 1).toInt)
            val pathStations = new ListBuffer[String]
            // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
            fields.foreach(x => pathStations.append(stationNoToName.value(x.toInt)))
            ((sou, des), pathStations.toList)
        }).groupByKey().mapValues(_.toList).cache()


        // 共享为广播变量
        val perODMap = sc.broadcast(validPathFile.collect().toMap)

        // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
        val validPathStationSetRDD = validPathFile.map(v => {
            val temp_set: mutable.Set[String] = mutable.Set()
            v._2.foreach(path => temp_set.++=(path.toSet))
            (v._1, temp_set)
        })
        println("validPathStationSetRDD", validPathStationSetRDD.first())
        val validPathStationSet = sc.broadcast(validPathStationSetRDD.collect().toMap)

        //    // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        //    val perODMap = validPathFile.groupByKey().mapValues(_.toList).collect().toMap
        //    // 将OD之间的有效路径涵盖的站点处理为Set集合，OD-pair作为键
        //    val validPathStationSetRDD  = validPathFile.groupByKey().mapValues(v => {
        //      val temp_set: mutable.Set[String] = mutable.Set()
        //      v.toList.foreach(path => temp_set.++=(path.toSet))
        //      temp_set
        //    })
        //    val validPathStationSet = sc.broadcast(validPathStationSetRDD.collect().toMap)


        // 读取最短路径的时间信息
        //val shortestPath = sc.textFile(args(0) + "/liutao/AllInfo/shortpath.txt").map(line => {
        // 起始地 目的地 时间(分钟)
        val shortestPath = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\AllInfo\\shortpath.txt").map(line => {
            val fields = line.split(' ')
            val sou = stationNoToName.value(fields(0).toInt)
            val des = stationNoToName.value(fields(1).toInt)
            // 换算成秒
            val time = (fields(2).toFloat * 60).toLong
            ((sou, des), time)
        })

        // 转换成map便于查询
        val shortestPathTime = sc.broadcast(shortestPath.collect().toMap)

        // 读取乘客的OD记录
        //val personalOD = sc.textFile(args(1)).map(line => {
        // (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
        val personalOD = sc.textFile("D:\\subwayData\\spark\\data\\Destination\\subway-pair\\part-00000").map(line => {

            val fields = line.split(',')
            val pid = fields(0).drop(1)
            val time = transTimeFormat(fields(1))
            val station = fields(2)
            //val tag = fields(3).dropRight(1)
            val tag = fields(3) // 上下车
            val weeks = getWeek(time) //获得weekOfMonth
            ((pid, weeks), (time, station, tag))
        }).cache()

        // 挑选OD记录最多的部分smartcardID
        // pid -> (pid, 1) -> (pid, n) -> 根据n排序
        val countRDD = personalOD.map(x => (x._1._1, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)

        //    println("----------")
        //    println("----------------------------------" + countRDD.count())
        //    println("---------------------")

        val countRDDSet = sc.broadcast(countRDD.take(200).map(x => x._1).toSet)

        // 过滤出这部分smartcardID的OD数据
        val selectedRDD = personalOD.filter(x => countRDDSet.value.contains(x._1._1))


        // 按照（smartcardID,week）分组，然后再按周分组
        val groupByWeeks = selectedRDD.groupByKey().mapValues(_.toList.sortBy(_._1)).map(x => (x._1._2, (x._1._1, x._2)))
            .groupByKey().mapValues(_.toList)

        // 共享为广播变量
        val broadcastODInfo = sc.broadcast(groupByWeeks.collect().toMap)

        /*----------------------------------------------------------------------------------------------------------------*/

        // 读取mac数据
        //val macFile = sc.textFile(args(2)).map(line => {
        // (000000000000,2019-06-01 01:32:44,大运,165)
        val macFile = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\UI\\NormalMacData\\part-00000").map(line => {

            val fields = line.split(',')
            val macId = fields(0).drop(1)
            val time = transTimeToTimestamp(fields(1))
            val station = fields(2)
            val weeks = getWeek(time)
            ((macId, weeks), (time, station))
        })

        // 按周分组
        val groupedMacInfo = macFile.groupByKey().mapValues(_.toList.sortBy(_._1)).map(x => (x._1._2, (x._1._1, x._2)))

        // 通过广播变量和flatMap结合替代shuffle过程，避免OOM
        // 生成每一个AFC和AP的每周数据的配对
        val flatODAndMac = groupedMacInfo.flatMap(line => {
            val ODInfo = broadcastODInfo.value
            val ODWeekInfo = ODInfo(line._1)
            for (l <- ODWeekInfo) yield {
                (line._1, (l, line._2))
            }
        })


        //    // 过滤掉与OD冲突的数据
        //    val joinedMacAndOD = flatODAndMac.filter(line => {
        //      val macArray = line._2._2._2
        //      val ODArray = line._2._1._2
        //      var flag = true
        //      for (a <- ODArray if flag){
        //        val l = macArray.indexWhere(_._1 >= a._1 - 60)
        //        val r = macArray.lastIndexWhere(_._1 <= a._1 + 60)
        //        if (l != -1 && r != -1){
        //          for (i <- l.to(r) if flag){
        //            if (macArray(i)._2 == a._2)
        //              flag = true
        //            else
        //              flag = false
        //          }
        //        }
        //      }
        //      flag
        //    })


        val rankedScoreOfWeek = flatODAndMac.map(line => {
            var score = 0f
            val weeks = line._1
            val macId = line._2._2._1
            val ODId = line._2._1._1
            val macArray = line._2._2._2
            val ODArray = line._2._1._2
            var index = 0
            while (index + 1 < ODArray.length) {
                if (ODArray(index)._3 == "21" && ODArray(index + 1)._3 == "22" && ODArray(index + 1)._1 - ODArray(index)._1 < 10800) {
                    val so = ODArray(index)._2
                    val sd = ODArray(index + 1)._2
                    val to = ODArray(index)._1
                    val td = ODArray(index + 1)._1
                    val paths = perODMap.value((so, sd))
                    val pathStationSet = validPathStationSet.value((so, sd))
                    val l = macArray.indexWhere(_._1 > to - 120)
                    val r = macArray.lastIndexWhere(_._1 < td + 120)
                    val macStationSet: mutable.Set[String] = mutable.Set()
                    if (l >= 0 && r >= l) {
                        for (i <- l.to(r))
                            macStationSet.add(macArray(i)._2)
                        if (pathStationSet.union(macStationSet).size == pathStationSet.size) {
                            var temp_score = 0f
                            var index_mac = l
                            for (path <- paths) {
                                var path_score = 0f
                                val coincideList = new ListBuffer[Int]
                                // 当所截取的mac出行片段所含站点包含在当前有效路径站点集合中时进行相似度计算
                                if (path.toSet.union(macStationSet).size == path.length) {
                                    for (station <- path if index_mac <= r) {
                                        if (macArray(index_mac)._2.equals(station)) {
                                            index_mac += 1
                                            coincideList.append(path.indexOf(station))
                                        }
                                    }
                                    // 判断所截取Mac片段是否有多余未匹配的点,允许一个点的误差
                                    if (coincideList.nonEmpty && r - index_mac <= 1) {
                                        // 判断所截取并匹配的片段的起始和结束时间是否合理
                                        val realTime1 = abs(to - macArray(l)._1)
                                        val theoreticalTime1 = shortestPathTime.value((so, path(coincideList.head)))
                                        val realTime2 = abs(td - macArray(index_mac - 1)._1)
                                        val theoreticalTime2 = shortestPathTime.value((path(coincideList.last), sd))
                                        // 通过程序统计平均误差时间为450秒，这里放宽为600秒
                                        if (abs(realTime1 - theoreticalTime1) < 600 && abs(realTime2 - theoreticalTime2) < 600) {
                                            // 计算最大跨度
                                            if (coincideList.length == 1)
                                                path_score = 1f / path.length
                                            else if (coincideList.length >= 2)
                                                path_score = (coincideList.last - coincideList.head + 1).toFloat / path.length
                                        }
                                    }
                                }
                                temp_score = max(temp_score, path_score)
                            }
                            score += temp_score
                        }
                    }
                    index += 1
                }
                index += 1
            }
            // 生成每对smartcardID和MacID的每周相似度
            ((ODId, weeks), (macId, score.formatted("%.3f").toFloat))
        }).filter(_._2._2 > 0).cache()

        // 将每周的分数相加得到整月的分数，7表示月标签
        val monthRDD = rankedScoreOfWeek.groupBy(x => (x._1._1, x._2._1)).mapValues(_.toList).map(line => {
            var total = 0f
            line._2.foreach(x => total += x._2._2)
            ((line._1._1, 7), (line._1._2, total.formatted("%.3f").toFloat))
        })

        // 将周数据和月数据合并，对每个ID只取每周和每月的最高分数据
        val unionRDD = monthRDD.union(rankedScoreOfWeek).groupByKey().mapValues(_.toList.sortBy(_._2).reverse.head)

        // 使用累加器统计可能出现overlap的smartcardId的数量并保存这些ID
        val mayOverlap = new LongAccumulator()
        sc.register(mayOverlap)
        val highSimilarity = new LongAccumulator()
        sc.register(highSimilarity)

        // 按照smartcardID合并数据，并统计overlap情况
        val resultRDD = unionRDD.groupBy(_._1._1).mapValues(_.toList.sortBy(_._1._2)).map(line => {
            val smartCardId = line._1
            val total = line._2.last._2
            val avg = total._2 / (line._2.length - 1)
            var detail = ""
            var flag = false

            line._2.foreach(x => {
                detail += x._1._2.toString + '/' + x._2._1 + '/' + x._2._2.toString + ','
                if (total._2 >= 20 && x._2._2 > avg && x._2._1 != total._1) {
                    flag = true
                }
            })

            // 添加标签，A-相似度<20,B-相似度>=20,C-相似度>=20且可能存在overlap
            if (total._2 >= 20) {
                highSimilarity.add(1)
                if (flag) {
                    mayOverlap.add(1)
                    detail += "C"
                }
                else
                    detail += "B"
            }
            else
                detail += "A"
            (smartCardId, detail)
        }).cache()

//        resultRDD.repartition(1).saveAsTextFile(args(3))

        val overlapRDD = resultRDD.filter(_._2.split(',').last == "C").map(_._1).collect()
        overlapRDD.foreach(println)
        //    overlapRDD.repartition(1).saveAsTextFile(args(6))

        println("mayOverlap:" + mayOverlap.value)
        println("similarity >= 20:" + highSimilarity.value)

        sc.stop()
    }

    def transTimeFormat(timeString: String): Long = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val time = dateFormat.parse(timeString).getTime / 1000
        time
    }

    def getWeek(t: Long): Int = {
        val pattern = "yyyy-MM-dd HH:mm:ss"
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        val timeString = dateFormat.format(t * 1000)
        val time = dateFormat.parse(timeString)
        val calendar = Calendar.getInstance()
        calendar.setTime(time)
        calendar.get(Calendar.WEEK_OF_MONTH)
    }
}

import GeneralFunctionSets.{dayOfMonth_long, hourOfDay_long, secondsOfDay, transTimeToTimestamp}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math._

object AMPI {

    case class distAndKinds(var d: Long, var k: Int)

    def main(args: Array[String]): Unit = {
//        val spark = SparkSession
//            .builder()
//            .appName("AMPI")
//            .getOrCreate()
//        val sc = spark.sparkContext

        val conf = new SparkConf().setAppName("AfcAndApContrast").setMaster("local")
        val sc = new SparkContext(conf)

        // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
//        val stationFile = sc.textFile(args(0) + "/zlt/AllInfo/stationInfo-UTF-8.txt")
        val stationFile = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\AllInfo\\stationInfo-UTF-8.txt")
        val stationNo2NameRDD = stationFile.map(line => {
            val stationNo = line.split(',')(0)
            val stationName = line.split(',')(1)
            (stationNo.toInt, stationName)
        })
        val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

        // 读取站间时间间隔，单位：秒 "(龙华,清湖,133)"

//        val readODTimeInterval = sc.textFile(args(0) + "/zlt/UI/AllODTimeInterval/ShortPathTime/part-00000").map(line => {
        //  line ==> (龙华,清湖,133)
        val readODTimeInterval = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\UI\\AllODTimeInterval\\ShortPathTime\\part-00000").map(line => {
            val p = line.split(',')
            val sou = p(0).drop(1)
            val des = p(1)
            val interval = p(2).dropRight(1).toLong
            ((sou, des), interval)
        })
        val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

        // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"
        // 将OD之间的有效路径的站点编号转换为名称，OD-pair作为键
        //val validPathFile = sc.textFile(args(0) + "/zlt/AllInfo/allpath.txt").map(line => {
        val validPathFile = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\AllInfo\\allpath.txt").map(line => {
            val fields = line.split(' ').dropRight(5)
            val sou = stationNo2Name.value(fields(0).toInt)
            val des = stationNo2Name.value(fields(fields.length - 1).toInt)
            val path = fields.map(x => stationNo2Name.value(x.toInt)) // 暂时看不懂
            ((sou, des), path)
        }).groupByKey().mapValues(x => (x.toArray, x.minBy(_.length).length))
        println("validPathFile:",validPathFile)
        val validPathMap = sc.broadcast(validPathFile.collect().toMap)

        // 读取groundTruth计算Accuracy (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
        // val groundTruthData = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/IdMap/part-*").map(line => {
        val groundTruthData = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\UI-2021\\GroundTruth\\IdMap\\part-00000").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        // 读取flow distribution "蛇口港,黄贝岭,0,0,0,259,193,173,223,350,821,903,338,114"
        //val flowDistribution = sc.textFile(args(0) + "/zlt/UI-2021/SegmentsFlowDistribution/part-00000").map(line => {
        val flowDistribution = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\UI-2021\\SegmentsFlowDistribution\\part-00000").map(line => {
            val fields = line.split(",")
            val os = fields(0)
            val ds = fields(1)
            val flow = fields.takeRight(12).map(_.toInt)
            ((os, ds), flow)
        })
        val flowMap = sc.broadcast(flowDistribution.collect().toMap)


        /**
         * Pre-process AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
         */
        // val AFCFile = sc.textFile(args(0) + "/Destination/subway-pair/part-000[0-6]*").map(line => {
        val AFCFile = sc.textFile("D:\\subwayData\\spark\\data\\Destination\\subway-pair\\part-00000").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            (id, (ot, os, dt, ds, day))
        }).filter(_._2._5 > 0) // 去掉一次出行时间超过1天的数据
        //  id            ot        os    dt       ds     day
        // (667979926,(1559608942,坪洲,1559609723,宝安中心,4))
        println("AFCFile: ",AFCFile.first())

        // 根据id聚合,仅保留出行天数大于5天的乘客数据
        val AFCPartitions = AFCFile.groupByKey().map(line => {
            val dataArray = line._2.toList.sortBy(_._1) // (ot, os, dt, ds, day),根据ot排序
            val daySets = dataArray.map(_._5).toSet // day做成的集合
            (line._1, dataArray, daySets) // id,data列表,dayOfMonth集合
        }).filter(_._3.size > 5)
        println("AFCPartitions",AFCPartitions.first())

        // AFC模式提取-基于核密度估计的聚类
        val AFCPatterns = AFCPartitions.map(line => {
            // 将每次出行的索引信息记录
            val pairs = new ArrayBuffer[(Long, String, Long, String, Int)]()
            for (i <- line._2.indices) { //  data列表下标 ==> i
                val trip = line._2(i) // (ot, os, dt, ds, day)
                pairs.append((trip._1, trip._2, trip._3, trip._4, i)) //(ot, os, dt, ds, 下标)
            }
            val daySets = line._3

            // 提取时间戳对应当天的秒数用于聚类
            val stampBuffer = new ArrayBuffer[Long]() //本行的时间戳
            pairs.foreach(v => {
                stampBuffer.append(secondsOfDay(v._1))// ot
                stampBuffer.append(secondsOfDay(v._3)) // dt
            })
            val timestamps = stampBuffer.toArray.sorted
            // 设置带宽h，单位为秒
            val h = 1800
            // 计算局部密度
            val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
            for (t <- timestamps) { // 看不懂
                var temp = 0D
                for (v <- timestamps) {
                    temp += RBF(v, t, h)
                }
                density_stamp_Buffer.append((temp / (timestamps.length * h), t))
            }
            val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

            // 判断是否存在聚类中心，若返回为空则不存在，否则分类
            val cluster_center = z_score(density_stamp)

            // 设置类边界距离并按照聚类中心分配数据
            val dc = 5400
            // 初始化类簇,结构为[所属类，出行片段]
            val clusters = new ArrayBuffer[(Int, (Long, String, Long, String, Int))]
            for (v <- pairs) { // pairs == (ot, os, dt, ds, 下标)
                if (cluster_center.nonEmpty) {
                    val o_stamp = secondsOfDay(v._1) //ot
                    val d_stamp = secondsOfDay(v._3) // dt
                    val o_to_c = distAndKinds(Long.MaxValue, 0) //看不懂
                    val d_to_c = distAndKinds(Long.MaxValue, 0)
                    for (c <- cluster_center) {
                        if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d) {
                            o_to_c.k = c._1
                            o_to_c.d = abs(o_stamp - c._2)
                        }
                        if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d) {
                            d_to_c.k = c._1
                            d_to_c.d = abs(d_stamp - c._2)
                        }
                    }
                    if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
                        clusters.append((o_to_c.k, v))
                    else
                        clusters.append((0, v))
                }
                else
                    clusters.append((0, v))
            }

            // 存储所有pattern的出行索引信息
            val afc_patterns = new ListBuffer[List[Int]]()

            // 按照所属类别分组
            val grouped = clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
            if (grouped.nonEmpty) {
                grouped.foreach(g => {
                    // 同一类中数据按照进出站分组
                    val temp_data = g._2.toArray.groupBy(x => (x._2._2, x._2._4))
                    temp_data.foreach(v => {
                        // 超过总出行天数的1/2则视为出行模式
                        if (v._2.length >= 5 || v._2.length > daySets.size / 2) {
                            // 存储当前pattern中所有出行的索引信息
                            val temp_patterns = new ListBuffer[Int]()
                            v._2.foreach(x => temp_patterns.append(x._2._5))
                            afc_patterns.append(temp_patterns.toList)
                        }
                    })
                })
            }

            // id、出行片段集合、出行模式数组(包含出行索引信息)、出行日期集合
            (line._1, pairs.toArray, afc_patterns.toList, daySets)
        })

        /**
         * 读取AP数据:(00AEFAF1826C,2019-06-01 18:47:34,皇岗口岸,243,2019-06-01 18:53:20,福民,77)
         */
        // val APFile = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/SampledAPData-30%/part-*").map(line => {
        val APFile = sc.textFile("D:\\subwayData\\spark\\data\\zlt-hdfs\\UI-2021\\GroundTruth\\SampledAPData-30%\\part-00000").map(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            (id, (ot, os, dt, ds, day))
        }).filter(_._2._5 > 0)

        // 划分AP
        val APPartitions = APFile.groupByKey().map(line => {
            val dataArray = line._2.toArray.sortBy(_._1)// (ot, os, dt, ds, day)
            val daySets = dataArray.map(_._5).toSet
            (daySets.size, (line._1, dataArray, daySets))
        })

        val APData = sc.broadcast(APPartitions.groupByKey().mapValues(_.toArray).collect().toMap)

        // 将AP和AFC数据按照天数结合
        val mergeData = AFCPatterns.flatMap(afc => {
            // 允许ap天数比afc天数多的天数限制
            val extra = 5
            val limit = afc._4.size + extra
            val candidateDays = APData.value.keys.toSet.filter(x => x <= limit)
            for (i <- candidateDays; ap <- APData.value(i)) yield {
                (ap, afc)
            }
        })

//        val gama_1 = args(1).toDouble
//        val gama_2 = args(2).toDouble
//        val gama_3 = args(3).toDouble
        val gama_1 = 10.0
        val gama_2 = 4.0
        val gama_3 = 10.0
        val matchData = mergeData.map(line => {
            //  Array[(Long, String, Long, String, Int)]
            val AP = line._1._2
            //  Array[(Long, String, Long, String, Int)]
            val AFC = line._2._2
            val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
            val tr_ap = new ArrayBuffer[Int]()
            val tr_afc = new ArrayBuffer[Int]()
            var index_ap = 0
            var index_afc = 0
            var conflict = 0

            while (index_ap < AP.length && index_afc < AFC.length) {
                val cur_ap = AP(index_ap)
                val cur_afc = AFC(index_afc)
                if (cur_ap._3 < cur_afc._1) {
                    tr_ap.append(index_ap)
                    index_ap += 1
                }
                else if (cur_ap._1 > cur_afc._3) {
                    tr_afc.append(index_afc)
                    index_afc += 1
                }
                else if (cur_ap._1 > cur_afc._1 - 300 && cur_ap._3 < cur_afc._3 + 300) {
                    val paths = validPathMap.value((cur_afc._2, cur_afc._4))._1
                    var flag = true
                    for (p <- paths if flag) {
                        if (p.indexOf(cur_ap._2) >= 0 && p.indexOf(cur_ap._4) > p.indexOf(cur_ap._2)) {
                            val interval1 = ODIntervalMap.value(p.head, cur_ap._2)
                            val headGap = cur_ap._1 - cur_afc._1
                            val interval2 = ODIntervalMap.value(cur_ap._4, p.last)
                            val endGap = cur_afc._3 - cur_ap._3
                            if (headGap < 600 + interval1) {
                                if (0.5 * interval2 < endGap  & endGap < 600 + interval2) {
                                    flag = false
                                    tr_ap_afc.append((index_ap, index_afc))
                                }
                            }
                        }
                    }
                    if (flag) {
                        conflict += 1
                    }
                    index_afc += 1
                    index_ap += 1
                }
                else {
                    conflict += 1
                    index_afc += 1
                    index_ap += 1
                }
            }
            val conflictRatio = conflict.toDouble / (AP.length + AFC.length)

            // key:afc_index, value:(ap_index, score)
            var OL: Map[Int, (Int, Double)] = Map()
            val debug = new ListBuffer[(Float, Float)]()
            val afc_pattern = line._2._3
            var Q = 0L
            var P = 0L
            var R = 0L
            val score = new ListBuffer[Double]()
//            val records = new ListBuffer[List[(Double, Double)]]()
            var Similarity = 0d
            if (conflictRatio <= 0.1) {
                if (tr_ap_afc.nonEmpty) {
                    for (pair <- tr_ap_afc) {
                        Q += 1
                        val t_ap = AP(pair._1)
                        val t_afc = AFC(pair._2)
                        val ol_1 = (t_ap._3 - t_ap._1).toFloat / (t_afc._3 - t_afc._1)
                        val ot_ap = hourOfDay_long(t_ap._1) / 2
                        val flow_ap = flowMap.value((t_ap._2, t_ap._4))(ot_ap)
                        val ot_afc = hourOfDay_long(t_afc._1) / 2
                        val flow_afc = flowMap.value((t_afc._2, t_afc._4))(ot_afc)
                        val ol_2 = flow_afc.toFloat / flow_ap
//                        val ol_3 = if (ol_2 >= 1) 0 else ol_2 *
                        OL += (pair._2 -> (pair._1, ol_1 * gama_1 + ol_2 * (1 - gama_1)))
                        debug.append((ol_1, ol_2))
                    }
                    // 首先处理存在pattern的tr_ap_afc；根据afc_pattern聚合
                    var index = Set[Int]() // 记录有对应pattern的tr_ap_afc中afc的index
                    for (pattern <- afc_pattern) {
                        val ap_seg = new ArrayBuffer[Int]()
                        val group_scores = new ArrayBuffer[Double]()
                        for (i <- pattern) {
                            if (OL.contains(i)) {
                                index += i
                                ap_seg.append(OL(i)._1)
                                group_scores.append(OL(i)._2)
                            }
                        }
                        // 计算每个group的得分
                        if (ap_seg.nonEmpty) {
                            val agg_trip = ap_seg.maxBy(x => AP(x)._3 - AP(x)._1)
                            val agg_ap = AP(agg_trip)

                            val cur_afc = AFC(pattern.head)
                            val v_1 = (agg_ap._3 - agg_ap._1).toFloat / (cur_afc._3 - cur_afc._1) * gama_1
                            val ot_afc = hourOfDay_long(cur_afc._1) / 2
                            val fl_afc = flowMap.value((cur_afc._2, cur_afc._4))(ot_afc)
                            val ot_ap = hourOfDay_long(agg_ap._1) / 2
                            val fl_ap = flowMap.value((agg_ap._2, agg_ap._4))(ot_ap)
                            val v_2 = fl_afc.toFloat / fl_ap
                            val v_3 = if (v_2 >= 1) 0 else v_2 * (1 - gama_1)
                            val v = v_1 + v_3

                            // 衰减
                            var group_score = 0d
                            val sort_a = group_scores.sorted
                            for (i  <- group_scores.indices) {
                                group_score += (gama_2 * sort_a(i) + (1 - gama_2) * v) / Math.exp(gama_3 * i)
                            }
                            score.append(group_score)
//                            records.append(group_scores.toList)
                        }
                    }
                    // 无pattern
//                    records.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).toList)
                    score.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).sum)
                }
                P = tr_afc.length
                R = tr_ap.length
                Similarity = score.sum / (Q + P + R)
            }
//            val OL_str = OL.map(x=>(x._1, (x._2._1, x._2._2.formatted("%.2f"))))
//            val score_str = score.toList.map(_.formatted("%.2f"))

            (line._1._1, (line._2._1, Similarity, debug.toList))
        }).filter(_._2._2 > 0)

//        val results = new ListBuffer[(Double, Double, Double, Double)]()
//        val gama_1_array = Range(2, 11, 4).map(_/10.0)
//        for (gama_1 <- gama_1_array) {
//            val gama_2 = 0.9d
//            val gama_3 = 0.1d
//            val test = matchData.map(line => {
//                val apID = line._1
//                val afcID = line._2._1
//                val data = line._2._2
//                val Q = line._2._3
//                val P = line._2._4
//                val R = line._2._5
//                var similarity = 0d
//                val score = data.map(line => line.map(x => x._1 * gama_1 + x._2 * (1 - gama_1)))
//                for (i <- score.indices) {
//                    val group = score(i)
//                    if (i != score.length - 1){
//                        val agg = group.last
//                        val segs = group.dropRight(1).sorted
//                        for (j <- segs.indices)
//                            similarity += (gama_2 * segs(j) + (1 - gama_2) * agg) / Math.exp(gama_3 * j)
//                    }
//                    else {
//                        similarity += group.sum
//                    }
//                }
//                (apID, (afcID, similarity / (Q + P + R)))
//            })
//
//            val result = test.groupByKey().mapValues(_.toArray.maxBy(_._2)).map(line => {
//                var flag = 0
//                if (groundTruthMap.value(line._1) == line._2._1)
//                    flag = 1
//                (flag, 1)
//            }).reduceByKey(_ + _)
//
//            val resultMap = result.collect().toMap
//            val t = resultMap(1)
//            val f = resultMap(0)
//            results.append((gama_1, gama_2, gama_3, t.toDouble/ (t + f)))
//        }
//
//        results.foreach(println(_))


//        matchData
//            .map(x => (x._2._3, (x._1, x._2._1, x._2._4)))
//            .groupByKey()
//            .mapValues(_.toList)
//            .filter(x => x._1 > 5)
//            .collect()
//            .foreach(x => {
//                println(x._1)
//                x._2.foreach(println(_))
//            })
//        matchData.repartition(1).sortBy(_._2._3, ascending = true).saveAsTextFile(args(0) + "zlt/UI-2021/WrongMatch")

//
//        val matchResult = matchData.map(line => (line._1, line._2._1)).groupByKey().mapValues(_.toSet)
//        val result = matchResult.map(line => {
//            var flag = 0
//            if (line._2.contains(groundTruthMap.value(line._1)))
//                flag = 1
//            (flag, 1)
//        }).reduceByKey(_+_)

//        val matchResult = matchData.groupByKey().map(line => {
//            val data = line._2.toArray
//            val mostMatch = data.maxBy(_._2)
//            val i = data.indexWhere(x => x._1 == groundTruthMap.value(line._1))
//            val realOne = if (i >= 0 )  data(i) else mostMatch
//            (line._1, mostMatch, "#######", realOne)
//        }).filter(x => x._2._1 != x._4._1)
//        matchResult.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/WrongMatchAnalysis")

//        val result = matchData.groupByKey().mapValues(_.toArray.maxBy(_._2)).map(line => {
//            var flag = 0
//            if (groundTruthMap.value(line._1) == line._2._1) {
//                flag = 1
//            }
//            (flag, 1)
//        }).reduceByKey(_ + _)
//
//
//        val resultMap = result.collect().toMap
//        println(gama_1, gama_2, gama_3, resultMap(1).toFloat / (resultMap(0) + resultMap(1)))
//        println(resultMap)

        val result = matchData.groupByKey().mapValues(_.toArray.maxBy(_._2))
        // result.filter(x => groundTruthMap.value(x._1) == x._2._1).repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/debug")
        val saveRdd = result.filter(x => groundTruthMap.value(x._1) == x._2._1).repartition(1)
//        println("saveRdd: ",saveRdd.first())

        sc.stop()
    }

    // 高斯核函数
    def RBF(l: Long, x: Long, h: Int): Double = {
        1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
    }

    // 计算z_score自动选取聚类中心
    def z_score(dens_pos: Array[(Double, Long)]): Array[(Int, Long)] = {
        val dist_r = compute_dist(dens_pos)
        val dist_l = compute_dist(dens_pos.reverse).reverse
        val dist_dens_pos = new ArrayBuffer[(Long, Double, Long)]()
        for (i <- dist_r.indices) {
            if (dist_r(i) == -1 && dist_l(i) == -1)
                dist_dens_pos.append((dens_pos.last._2 - dens_pos.head._2, dens_pos(i)._1, dens_pos(i)._2))
            else if (dist_r(i) != -1 && dist_l(i) != -1)
                dist_dens_pos.append((min(dist_r(i), dist_l(i)), dens_pos(i)._1, dens_pos(i)._2))
            else if (dist_l(i) != -1)
                dist_dens_pos.append((dist_l(i), dens_pos(i)._1, dens_pos(i)._2))
            else
                dist_dens_pos.append((dist_r(i), dens_pos(i)._1, dens_pos(i)._2))
        }
        var sum_dist = 0L
        var sum_dens = 0d
        dist_dens_pos.foreach(x => {
            sum_dist += x._1
            sum_dens += x._2
        })
        val avg_dist = sum_dist / dist_dens_pos.length
        val avg_dens = sum_dens / dist_dens_pos.length
        var total = 0d
        for (v <- dist_dens_pos) {
            total += pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)
        }
        val sd = sqrt(total / dist_dens_pos.length)
        val z_score = new ArrayBuffer[((Long, Double, Long), Double)]()
        var z_value = 0d
        for (v <- dist_dens_pos) {
            z_value = sqrt(pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)) / sd
            z_score.append((v, z_value))
        }
        val result = new ArrayBuffer[(Int, Long)]()
        // z-score大于3认为是类簇中心
        val clustersInfo = z_score.toArray.filter(_._2 >= 3)
        for (i <- clustersInfo.indices) {
            result.append((i + 1, clustersInfo(i)._1._3))
        }
        result.toArray
    }

    // 计算相对距离
    def compute_dist(info: Array[(Double, Long)]): Array[Long] = {
        val result = new Array[Long](info.length)
        val s = mutable.Stack[Int]()
        s.push(0)
        var i = 1
        var index = 0
        while (i < info.length) {
            if (s.nonEmpty && info(i)._1 > info(s.top)._1) {
                index = s.pop()
                result(index) = abs(info(i)._2 - info(index)._2)
            }
            else {
                s.push(i)
                i += 1
            }
        }
        while (s.nonEmpty) {
            result(s.pop()) = -1
        }
        result
    }
}

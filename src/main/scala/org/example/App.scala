package org.example
//import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * Hello world!
 *
 */
object App extends App {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)

  println( "Hello World!" )
  // user1,in,station1,2014-01-01 14:08:10
  val dataFile = "D:\\subwayData\\spark\\data\\simulate.csv"
  val csvData = sc.textFile(dataFile).map(line => {
    val fields = line.split(',')
    val id = fields(0)
//    val dt = transTimeToTimestamp(fields(3)) //(单位MS)
//    (id, (fields(1),fields(2), dt)) // 生成key value
  })
  // 切分格式

  // 如果是连续两个in 则保留后面一个
  // 如果是连续两个out, 则保留前面一个

  // 应该是进站与出站join
  // 自己跟自己join, (id, 进出站, 站点, 时间) -> (id, 进出站, 站点, 时间, 进出站, 站点, 时间)
  // join

  //根据 (id, 进站时间, 出站时间) 排序, 取第一条记录
  sc.stop()



}

import org.apache.spark.{SparkConf, SparkContext}


// 从OD记录统计每个站点的降序进站人数
object CountPassengerFlow {
    def main(args: Array[String]): Unit = {
        // val conf = new SparkConf().setAppName("CountPassengerFlow")
        val conf = new SparkConf().setAppName("CountPassengerFlow").setMaster("local")
        val sc = new SparkContext(conf)

        // (667979926,2019-06-04 08:42:22,坪洲,21,2019-06-04 08:55:23,宝安中心,22)
        //val ODFile = sc.textFile(args(0) + "/Destination/subway-seq/part-*").map(line => {
        val ODFile = sc.textFile("D:\\subwayData\\spark\\data\\Destination\\subway-pair\\part-00000").map(line => {
            val fields = line.split(",")
            val station = fields(2)
            (station, 1)
        })

        // 统计每个站点的客流量,按照客流量降序排序并保存
        ODFile.reduceByKey(_ + _)
            .repartition(1)
            .sortBy(_._2, ascending = false)
            //.saveAsTextFile(args(0) + "/zlt/UI/PassengerFlow")

        sc.stop()
    }
}
